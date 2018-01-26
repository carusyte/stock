package score

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/carusyte/stock/indc"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"github.com/montanaflynn/stats"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Medium to Long term model.
// Value stocks for:
// 1. High average DYR of up to 5 years, without interruptions
// 2. DYR with progressive increase of up to 5 years, without interruptions
// 3. High average DYR to DPR ratio
// 4. High latest DYR
// 5. Latest dividend event on appropriate registration date.
// Get warnings/penalties if:
// 1. Dividend Payout Ratio is high
type HiD struct {
	Code        string
	Name        string
	Year        string          `db:"year"`
	RegDate     string          `db:"reg_date"`
	XdxrDate    string          `db:"xdxr_date"`
	Divi        sql.NullFloat64 `db:"divi"`
	DiviGrYoy   string
	SharesAllot sql.NullFloat64 `db:"shares_allot"`
	SharesCvt   sql.NullFloat64 `db:"shares_cvt"`
	Dyr         sql.NullFloat64 `db:"dyr"`
	DyrGrYoy    string
	DyrAvg      float64
	DyrAvgYrs   int
	Dpr         sql.NullFloat64 `db:"dpr"`
	DprAvg      float64
	DprAvgYrs   int
	Dyr2Dpr     float64
	Price       float64 `db:"price"`
	PriceDate   string  `db:"price_date"`
}

const (
	AVG_GR_HIST_SIZE         = 5
	SCORE_DYR_AVG    float64 = 35
	SCORE_DYR_GR             = 20
	SCORE_LATEST_DYR         = 20
	SCORE_DYR2DPR            = 15
	SCORE_REG_DATE           = 10
	PENALTY_DPR              = 25
)

func (h *HiD) Geta() (r *Result) {
	return h.Get(nil, -1, false)
}

func (h *HiD) Get(s []string, limit int, ranked bool) (r *Result) {
	r = &Result{}
	r.PfIds = append(r.PfIds, h.Id())
	var hids []*HiD
	if s == nil || len(s) == 0 {
		sql, e := dot.Raw("HID")
		util.CheckErr(e, "failed to get HID sql")
		_, e = dbmap.Select(&hids, sql)
		util.CheckErr(e, "failed to query database, sql:\n"+sql)
	} else {
		sql, e := dot.Raw("HID_SCOPED")
		util.CheckErr(e, "failed to get HID_SCOPED sql")
		sql = fmt.Sprintf(sql, strings.Join(s, ","))
		_, e = dbmap.Select(&hids, sql)
		util.CheckErr(e, "failed to query database, sql:\n"+sql)
	}

	//mark stocks with H shares
	var hstk []string
	_, e := dbmap.Select(&hstk, "select code from basics where h_share_sum is not null and h_share_sum > 0")
	util.CheckErr(e, "failed to query H share stocks from database")
	sort.Strings(hstk)

	for _, ih := range hids {
		item := new(Item)
		r.AddItem(item)
		item.Code = ih.Code
		item.Name = ih.Name
		item.Profiles = make(map[string]*Profile)
		ip := new(Profile)
		item.Profiles[h.Id()] = ip
		ip.FieldHolder = ih
		ip.Score += scoreDyr(ih, SCORE_LATEST_DYR)

		if sort.SearchStrings(hstk, ih.Code) < len(hstk) {
			item.AddMark(HMark)
		}

		//supplement latest price
		lp := &HiD{}
		e := dbmap.SelectOne(&lp, "select close as price, date as price_date from kline_d where code = ? order by "+
			"klid desc limit 1", ih.Code)
		if e != nil {
			if "sql: no rows in result set" == e.Error() {
				logrus.Warnf("%s lack of kline_d data", item.Code)
			} else {
				log.Panicf("%s failed to query kline_d for latest price\n%+v", item.Code, e)
			}
		}
		ih.Price = lp.Price
		ih.PriceDate = lp.PriceDate

		ip.Score += scoreDyrHist(ih)

		ip.Score += scoreRegDate(ih, item, SCORE_REG_DATE)

		//warn if dpr is greater than 90%
		if ih.Dpr.Valid && ih.Dpr.Float64 > 0.9 {
			item.Cmtf("DPR is high at %.1f%%", ih.Dpr.Float64*100)
		}

		ip.Score = math.Max(0, ip.Score)
		item.Score += ip.Score
	}
	r.SetFields(h.Id(), h.Fields()...)
	if ranked {
		r.Sort()
	}
	r.Shrink(limit)
	return
}

//Score by dividend registration date of the year. There might be multiple dividend events in one year.
//The price of stock might get volatile around the registration date.
//Score is weighted by each dividend amount.
//Get max score if the registration date is more than 3 days ago or there are 10 days or more before that date.
//Otherwise, the closer to the registration date, the less we score, on that date, we get 0.
func scoreRegDate(ih *HiD, item *Item, m float64) (s float64) {
	//supplement XdxrDate, RegDate, might get multiple dates in one year
	sql, e := dot.Raw("HID_XDXR_DATES")
	util.CheckErr(e, "failed to get HID_XDXR_DATES sql")
	var xdxrs []*model.Xdxr
	dbmap.Select(&xdxrs, sql, ih.Code, ih.Year)
	for j, x := range xdxrs {
		if !x.Divi.Valid {
			continue
		}
		w := x.Divi.Float64 / ih.Divi.Float64
		var base float64
		if x.RegDate.Valid {
			ih.RegDate = ih.RegDate + x.RegDate.String[5:]
			treg, e := time.Parse("2006-01-02", x.RegDate.String)
			util.CheckErr(e, "failed to parse registration date: "+x.RegDate.String)
			days := int(math.Ceil(treg.Sub(time.Now()).Hours() / 24))
			if -3 < days && days < 10 {
				base = math.Abs(float64(days)) / 10 * m
			} else {
				base = m
			}
			if days >= 0 {
				item.Cmtf("XDXR Reg date: %s", x.RegDate.String)
			}
		} else {
			ih.RegDate = ih.RegDate + "-----"
			if x.Progress.Valid && ("董事会预案" == x.Progress.String || "股东大会预案" == x.Progress.String) {
				base = m
			} else {
				base = 0
			}
		}
		s += base * w

		if x.XdxrDate.Valid {
			ih.XdxrDate = ih.XdxrDate + x.XdxrDate.String[5:]
		} else {
			ih.XdxrDate = ih.XdxrDate + "-----"
		}

		if j < len(xdxrs)-1 {
			ih.RegDate = ih.RegDate + "/"
			ih.XdxrDate = ih.XdxrDate + "/"
		}
	}
	s = math.Min(m, s)
	return
}

func scoreDyrHist(ih *HiD) (s float64) {
	sql, e := dot.Raw("HID_HIST")
	util.CheckErr(e, "failed to get HID_HIST sql")
	var hist []*HiD
	_, e = dbmap.Select(&hist, sql, ih.Code)
	util.CheckErr(e, "failed to query hid hist for "+ih.Code)
	s += scoreDyrAvg(ih, hist, SCORE_DYR_AVG)
	s += scoreDyrGr(ih, hist, SCORE_DYR_GR)
	s += scoreDyr2Dpr(ih, SCORE_DYR2DPR)
	s -= fineDpr(ih, hist, PENALTY_DPR)
	return
}

// Score by average DYR to DPR ratio.
// Get max score if ratio >= 10
// Get 0 if ratio <= 3
func scoreDyr2Dpr(ih *HiD, m float64) float64 {
	if ih.DprAvg != 0 {
		r := ih.DyrAvg / ih.DprAvg * 100.0
		ih.Dyr2Dpr = r
		if r <= 3 {
			return 0
		} else {
			return m * math.Min(1, math.Pow((r-3)/7, 0.35))
		}
	} else {
		return m
	}
}

// Fine for max penalty if average dpr is greater than 100% and latest dpr is greater than 150%
// Baseline: Average dpr <= 90% and Latest dpr <= 95%
func fineDpr(ih *HiD, hist []*HiD, m float64) float64 {
	p1, p2 := .0, .0
	if len(hist) < 2 {
		return 0
	}
	avg := ih.DprAvg
	if ih.Dpr.Valid && ih.Dpr.Float64 <= 0.95 && avg <= 0.9 {
		return 0
	}
	if avg > 0.9 {
		p1 = 4.0 / 5.0 * math.Min(1, math.Pow((avg-0.9)/0.1, 1.23))
	}
	if ih.Dpr.Valid && ih.Dpr.Float64 > 0.95 {
		p2 = 1.0 / 5.0 * math.Min(1, math.Pow((ih.Dpr.Float64-0.95)/0.55, 1.23))
	}
	return (p1 + p2) * m
}

//Score by SMA DYR.
//Get max socre if SMA DYR >= 6% and more than 5 years dividend recorded.
func scoreDyrAvg(ih *HiD, hist []*HiD, m float64) float64 {
	if len(hist) < 1 {
		ih.DyrAvg = 0
		ih.DyrAvgYrs = 0
		ih.DprAvg = 0
		ih.DprAvgYrs = 0
		return 0
	} else {
		dyrs := make([]float64, len(hist))
		dprs := make([]float64, len(hist))
		yrs := .0
		countyrs := true
		for i, ihist := range hist {
			if ihist.Divi.Valid {
				if countyrs {
					yrs++
				}
			} else {
				countyrs = false
			}
			if ihist.Dyr.Valid {
				dyrs[i] = ihist.Dyr.Float64
			} else {
				dyrs[i] = 0
			}
			if ihist.Dpr.Valid {
				dprs[i] = ihist.Dpr.Float64
			} else {
				dprs[i] = 0
			}
		}
		s := 0.3 * m * math.Min(1, math.Pow(yrs/AVG_GR_HIST_SIZE, 1.82))
		var e error
		avgDyr := .0
		avgDpr := .0
		if len(hist) < 3 {
			avgDyr, e = stats.Mean(dyrs)
			util.CheckErr(e, "failed to calculate average dyr: "+fmt.Sprint(dyrs))
			avgDpr, e = stats.Mean(dprs)
			util.CheckErr(e, "failed to calculate average dpr: "+fmt.Sprint(dprs))
		} else {
			util.ReverseF64s(dyrs, false)
			util.ReverseF64s(dprs, false)
			avgDyr = indc.SMA(dyrs, 3, 1)[len(dyrs)-1]
			avgDpr = indc.SMA(dprs, 3, 1)[len(dprs)-1]
		}
		ih.DyrAvg = avgDyr
		ih.DyrAvgYrs = len(dyrs)
		ih.DprAvg = avgDpr
		ih.DprAvgYrs = len(dprs)
		s += 0.7 * m * math.Min(1, math.Pow(avgDyr/0.06, 2.85))
		return s
	}
}

//Score according to dyr growth rate.
//Get max score if SMA DYR GR >= 15%，
//Get 0 if SMA DYR GR <= -20% or, more than 2/5 negative growth counts and avg negative growth rate is <= -50%
func scoreDyrGr(ih *HiD, hist []*HiD, m float64) float64 {
	if len(hist) < 1 {
		ih.DyrGrYoy = "---"
		return 0
	} else {
		grs := make([]float64, len(hist))
		ngrs := make([]float64, 0)
		for j, ihist := range hist {
			if j < len(hist)-1 {
				var gr float64
				if ihist.Dyr.Valid && hist[j+1].Dyr.Valid {
					gr = (ihist.Dyr.Float64 - hist[j+1].Dyr.Float64) / hist[j+1].Dyr.Float64 * 100.0
				} else if ihist.Dyr.Valid {
					gr = 100.0
				} else {
					gr = -100.0
				}
				grs[j] = gr
				if j < AVG_GR_HIST_SIZE {
					ih.DyrGrYoy = ih.DyrGrYoy + fmt.Sprintf("%.1f", gr)
					if j < int(math.Min(AVG_GR_HIST_SIZE-1, float64(len(hist)-2))) {
						ih.DyrGrYoy = ih.DyrGrYoy + "/"
					}
				}
				if gr < 0 {
					ngrs = append(ngrs, gr)
				}
			}
		}
		var (
			avg  = .0
			navg = .0
			p    = .0
			e    error
		)
		if len(ngrs) > 0 {
			navg, e = stats.Mean(ngrs)
			util.CheckErr(e, "faild to calculate mean for :"+fmt.Sprint(grs))
			if float64(len(ngrs))/float64(len(grs)) > 2./5. && navg <= -50 {
				return 0
			}
			p = m * math.Min(1, math.Pow(-1*navg/50, 3.12))
		}
		if len(grs) < 3 {
			avg, e = stats.Mean(grs)
			util.CheckErr(e, "faild to calculate mean for :"+fmt.Sprint(grs))
		} else {
			util.ReverseF64s(grs, false)
			avg = indc.SMA(grs, 3, 1)[len(grs)-1]
		}
		if avg <= -20 {
			return 0
		} else {
			s := m*math.Min(1, math.Pow((20+avg)/35, 1.68)) - p
			return math.Max(0, s)
		}
	}
}

// Score according to DYR.
// You get 2/3 max score if DYR >= 5% (0.05), get max score if DYR >= 7%
// Otherwise you get 0 if DYR <= 4%
func scoreDyr(ih *HiD, max float64) (s float64) {
	const THRESHOLD = 0.05
	const BOTTOM = 0.04
	const FACTOR = 0.16 // the smaller the factor, the more slowly the score regresses when under BOTTOM
	if ih.Dyr.Valid {
		if ih.Dyr.Float64 >= THRESHOLD {
			return max*2.0/3.0 + max/3.0*math.Min(1, (ih.Dyr.Float64-THRESHOLD)*50)
		} else {
			return 2.0 / 3.0 * max * math.Pow(math.Max(0, (ih.Dyr.Float64-BOTTOM)*100), FACTOR)
		}
	} else {
		return 0
	}
}

func (h *HiD) Id() string {
	return "HiD"
}

func (h *HiD) Fields() []string {
	return []string{"Year", "Divi", "DYR", "DYR GR", "DYR AVG", "DPR",
		"DPR AVG", "DYR:DPR", "Shares Allot",
		"Shares Cvt", "Price", "Price Date", "REG DT", "XDXR DT"}
}

func (h *HiD) GetFieldStr(name string) string {
	switch name {
	case "Divi":
		return fmt.Sprintf("%.2f", h.Divi.Float64)
	case "DYR":
		return fmt.Sprintf("%.2f%%", h.Dyr.Float64*100)
	case "DYR GR":
		return h.DyrGrYoy
	case "DYR AVG":
		return fmt.Sprintf("%.2f%%/%dy", h.DyrAvg*100, h.DyrAvgYrs)
	case "DPR":
		return fmt.Sprintf("%.2f%%", h.Dpr.Float64*100)
	case "DPR AVG":
		return fmt.Sprintf("%.2f%%/%dy", h.DprAvg*100, h.DprAvgYrs)
	case "DYR:DPR":
		return fmt.Sprintf("%.2f", h.Dyr2Dpr)
	case "Shares Allot":
		if h.SharesAllot.Valid {
			return fmt.Sprint(h.SharesAllot.Float64)
		} else {
			return ""
		}
	case "Shares Cvt":
		if h.SharesCvt.Valid {
			return fmt.Sprint(h.SharesCvt.Float64)
		} else {
			return ""
		}
	case "Divi GR":
		return h.DiviGrYoy
	case "Price Date":
		return h.PriceDate
	case "REG DT":
		return h.RegDate
	case "XDXR DT":
		return h.XdxrDate
	default:
		r := reflect.ValueOf(h)
		f := reflect.Indirect(r).FieldByName(name)
		if !f.IsValid() {
			panic(errors.New("undefined field for HiD: " + name))
		}
		return fmt.Sprintf("%+v", f.Interface())
	}
}

func (h *HiD) Description() string {
	return fmt.Sprint("Medium to Long term model.\n" +
		"Value stocks for:\n" +
		"1. High average DYR of up to 5 years, without interruptions\n" +
		"2. DYR with progressive increase of up to 5 years, without interruptions\n" +
		"3. High average DYR to DPR ratio\n" +
		"4. High latest DYR\n" +
		"5. Latest dividend event on appropriate registration date.\n" +
		"Get warnings/penalties if:\n" +
		"1. Dividend Payout Ratio is high\n")
}
