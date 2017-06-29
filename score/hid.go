package score

import (
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"fmt"
	"reflect"
	"math"
	"time"
	"database/sql"
	"github.com/pkg/errors"
	"github.com/montanaflynn/stats"
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
	Year        string `db:"year"`
	RegDate     string `db:"reg_date"`
	XdxrDate    string `db:"xdxr_date"`
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
	Price       float64    `db:"price"`
	PriceDate   string    `db:"price_date"`
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

func (h *HiD) Get(s []*model.Stock, limit int, ranked bool) (r *Result) {
	r = &Result{}
	r.PfIds = append(r.PfIds, h.Id())
	var hids []*HiD
	if s == nil || len(s) == 0 {
		sql, e := dot.Raw("HID")
		util.CheckErr(e, "failed to get HID sql")
		_, e = dbmap.Select(&hids, sql)
		util.CheckErr(e, "failed to query database, sql:\n"+sql)
	} else {
		//TODO select by specified stock codes
	}

	for _, ih := range hids {
		item := new(Item)
		r.AddItem(item)
		item.Code = ih.Code
		item.Name = ih.Name
		item.Profiles = make(map[string]*Profile)
		ip := new(Profile)
		item.Profiles[h.Id()] = ip
		ip.FieldHolder = ih
		ip.AddField("Year")
		ip.AddField("Divi")
		ip.AddField("DYR")
		ip.AddField("DPR")
		ip.AddField("Shares Allot")
		ip.AddField("Shares Cvt")
		ip.Score += scoreDyr(ih, SCORE_LATEST_DYR)

		//supplement latest price
		lp := &HiD{}
		e := dbmap.SelectOne(&lp, "select close as price, date as price_date from kline_d where code = ? order by "+
			"klid desc limit 1", ih.Code)
		util.CheckErr(e, "failed to query kline_d for lastest price: "+ih.Code)
		ih.Price = lp.Price
		ih.PriceDate = lp.PriceDate
		ip.AddField("Price")
		ip.AddField("Price Date")

		ip.Score += scoreDyrHist(ih)
		ip.AddFieldAt(3, "DYR GR")
		ip.AddFieldAt(4, "DYR AVG")
		ip.AddFieldAt(6, "DPR AVG")
		ip.AddFieldAt(7, "DYR:DPR")

		ip.Score += scoreRegDate(ih, SCORE_REG_DATE)
		ip.AddField("REG DT")
		ip.AddField("XDXR DT")

		//warn if dpr is greater than 90%
		if ih.Dpr.Valid && ih.Dpr.Float64 > 0.9 {
			ip.Cmtf("DPR is high at %.1f%%", ih.Dpr.Float64*100)
		}

		item.Score += ip.Score
	}
	if ranked {
		r.Sort()
	}
	r.Shrink(limit)
	return
}

//Score by dividend registration date of the year. There might be multiple dividend events in p year.
//The price of stock might get volatile on and immediately after the registration date.
//Score is weighted by each dividend amount.
//Get max score if the registration date is more than 3 days ago or there are 10 days or more before that date.
//Otherwise, the closer to the registration date, the less we score, on that date, we get 0.
func scoreRegDate(ih *HiD, m float64) (s float64) {
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
	return
}

func scoreDyrHist(ih *HiD) (s float64) {
	sql, e := dot.Raw("HID_HIST")
	util.CheckErr(e, "failed to get HID_HIST sql")
	var hist []*HiD
	_, e = dbmap.Select(&hist, sql, ih.Code, AVG_GR_HIST_SIZE+1)
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
		r := ih.DyrAvg / ih.DprAvg * 100
		ih.Dyr2Dpr = r
		if r <= 3 {
			return 0
		} else {
			return m * math.Min(1, math.Pow((r-3)/7, 3.75))
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

//Score by Dyr average.
//Get max socre if average DYR of up to 5 years >= 6%
func scoreDyrAvg(ih *HiD, hist []*HiD, m float64) float64 {
	avg := .0
	l := 0
	if len(hist) < 2 {
		ih.DyrAvg = avg
		ih.DyrAvgYrs = l
		return 0
	} else {
		dyrs := make([]float64, len(hist)-1)
		dprs := make([]float64, len(hist)-1)
		l = len(dyrs)
		for i, ihist := range hist[:len(hist)-1] {
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
		var e error
		avg, e = stats.Mean(dyrs)
		util.CheckErr(e, "failed to calculate average dyr: "+fmt.Sprint(dyrs))
		ih.DyrAvg = avg
		ih.DyrAvgYrs = l
		avg, e = stats.Mean(dprs)
		util.CheckErr(e, "failed to calculate average dpr: "+fmt.Sprint(dprs))
		ih.DprAvg = avg
		ih.DprAvgYrs = len(dprs)
		return m * math.Min(1, math.Pow(ih.DyrAvg/0.06, 2.85))
	}
}

//Score according to dyr growth rate.
//Get 4/5 max score if growth rate is all positive and full max if avg >= 15%.
//Otherwise, get 4/5 max if avg growth rate >= 50% and get 0 if avg negative growth rate is <= -33%
//or sum of non-dividend year is greater than 3.
func scoreDyrGr(ih *HiD, hist []*HiD, m float64) float64 {
	if len(hist) < 2 {
		ih.DyrGrYoy = "---"
		return 0
	} else {
		positive := true
		grs := make([]float64, len(hist)-1)
		ngrs := make([]float64, 0)
		nodiv := .0
		for j, ihist := range hist {
			if j < len(hist)-1 {
				var gr float64
				if ihist.Dyr.Valid && hist[j+1].Dyr.Valid {
					gr = (ihist.Dyr.Float64 - hist[j+1].Dyr.Float64) / hist[j+1].Dyr.Float64 * 100.0
				} else if ihist.Dyr.Valid {
					gr = 100.0
				} else {
					gr = -100.0
					nodiv++
				}
				grs[j] = gr
				ih.DyrGrYoy = ih.DyrGrYoy + fmt.Sprintf("%.1f", gr)
				if j < len(hist)-2 {
					ih.DyrGrYoy = ih.DyrGrYoy + "/"
				}
				if gr < 0 {
					ngrs = append(ngrs, gr)
					positive = false
				}
			}
		}

		avg, e := stats.Mean(grs)
		util.CheckErr(e, "faild to calculate mean for :"+fmt.Sprint(grs))
		if len(grs) >= AVG_GR_HIST_SIZE && positive {
			return (4.0 + math.Min(1, math.Pow(avg/15.0, 0.21))) / 5.0 * m
		} else {
			if nodiv > 3 {
				return 0
			}
			if avg <= -33 {
				return 0
			} else {
				s := 4.0 / 5.0 * m
				s *= math.Min(1, math.Pow((33+avg)/83, 1.68))
				if len(ngrs) > 0 {
					navg, e := stats.Mean(ngrs)
					util.CheckErr(e, "faild to calculate mean for :"+fmt.Sprint(ngrs))
					s = math.Max(0, s-math.Pow(-1*navg/33, 1.35))
				}
				return s
			}
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
