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
)

// Medium to Long term model.
// Value stocks for:
// 1. Latest high yearly dividend yield ratio
// 2. Dividend with progressive increase or constantly at high level
// 3. Nearer registration date.
// Get warnings if:
// 1. Dividend Payout Ratio is greater than 90%
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
	Dpr         sql.NullFloat64 `db:"dpr"`
	Price       float64    `db:"price"`
	PriceDate   string    `db:"price_date"`
}

const LIMIT = 30

func (h *HiD) Get(s []*model.Stock) (r *Result) {
	r = &Result{}
	r.AspectIds = append(r.AspectIds, h.Id())
	var hids []*HiD
	if s == nil || len(s) == 0 {
		sql, e := dot.Raw("HID")
		util.CheckErr(e, "failed to get HID sql")
		_, e = dbmap.Select(&hids, sql, LIMIT)
		util.CheckErr(e, "failed to query database, sql:\n"+sql)
	} else {
		//TODO select by specified stock codes
	}

	for _, ih := range hids {
		item := new(Item)
		r.Items = append(r.Items, item)
		item.Code = ih.Code
		item.Name = ih.Name
		item.Aspects = make(map[string]*Aspect)
		iasp := new(Aspect)
		item.Aspects[h.Id()] = iasp
		iasp.Weight = 1
		iasp.FieldHolder = ih
		iasp.AddField("Year")
		iasp.AddField("Divi")
		iasp.AddField("DYR")
		iasp.AddField("DPR")
		iasp.AddField("Shares Allot")
		iasp.AddField("Shares Cvt")
		iasp.Score = iasp.Score + scoreDyr(ih, 30)

		//supplement latest price
		lp := &HiD{}
		e := dbmap.SelectOne(&lp, "select close as price, date as price_date from kline_d where code = ? order by "+
			"klid desc limit 1", ih.Code)
		util.CheckErr(e, "failed to query kline_d for lastest price: "+ih.Code)
		ih.Price = lp.Price
		ih.PriceDate = lp.PriceDate
		iasp.AddField("Price")
		iasp.AddField("Price Date")

		//supplement DiviGrYoy
		sql, e := dot.Raw("HID_HIST")
		util.CheckErr(e, "failed to get HID_HIST sql")
		var hist []*HiD
		_, e = dbmap.Select(&hist, sql, ih.Code, 4)
		util.CheckErr(e, "failed to query hid hist for "+ih.Code)
		positive := true
		var grs []float64
		for j, ihist := range hist {
			if j < len(hist)-1 {
				var gr float64
				if ihist.Divi.Valid && hist[j+1].Divi.Valid {
					gr = (ihist.Divi.Float64 - hist[j+1].Divi.Float64) / hist[j+1].Divi.Float64 * 100.0
				} else if ihist.Divi.Valid {
					gr = 100.0
				} else {
					gr = -100.0
				}
				grs = append(grs, gr)
				ih.DiviGrYoy = ih.DiviGrYoy + fmt.Sprintf("%.1f", gr)
				if j < len(hist)-2 {
					ih.DiviGrYoy = ih.DiviGrYoy + "/"
				}
				if gr < 0 {
					positive = false
				}
			}
		}
		iasp.AddFieldAt(2, "Divi GR", ih.DiviGrYoy)
		if (len(grs) == 3 && positive) || len(grs) == 0 {
			iasp.Score = iasp.Score + 30
		} else if len(grs) == 1 && positive {
			iasp.Score = iasp.Score + 25
		} else {
			if grs[0] > 0 {
				iasp.Score = iasp.Score + 15 + (15 * math.Min(1, 0.4+3*grs[0]))
			} else {
				factor := .0
				for j, g := range grs {
					if g < 0 {
						factor += float64(len(grs)-j) / float64(len(grs)) * g * 5
					}
				}
				factor = math.Min(1, factor)
				iasp.Score = iasp.Score - (15 * factor)
			}
		}

		//supplement XdxrDate, RegDate, might get multiple dates in one year
		sql, e = dot.Raw("HID_XDXR_DATES")
		util.CheckErr(e, "failed to get HID_XDXR_DATES sql")
		var xdxrs []*model.Xdxr
		dbmap.Select(&xdxrs, sql, ih.Code, ih.Year)
		for j, x := range xdxrs {
			if !x.Divi.Valid {
				continue
			}
			w := x.Divi.Float64 / ih.Divi.Float64
			var factor float64
			if x.RegDate.Valid {
				ih.RegDate = ih.RegDate + x.RegDate.String[5:]
				treg, e := time.Parse("2006-01-02", x.RegDate.String)
				util.CheckErr(e, "failed to parse registration date: "+x.RegDate.String)
				days := int(math.Ceil(treg.Sub(time.Now()).Hours() / 24))
				if days < -5 {
					factor = 20
				} else if days < 3 {
					factor = 20
				} else if days <= 10 {
					factor = 30
				} else {
					factor = 25
				}
				iasp.Score = iasp.Score + factor*w
			} else {
				ih.RegDate = ih.RegDate + "_"
				if x.Progress.Valid {
					if "董事会预案" == x.Progress.String {
						factor = 5
					} else if "股东大会预案" == x.Progress.String {
						factor = 10
					}
				}
				iasp.Score = iasp.Score + (15+factor)*w
			}

			if x.XdxrDate.Valid {
				ih.XdxrDate = ih.XdxrDate + x.XdxrDate.String[5:]
			} else {
				ih.XdxrDate = ih.XdxrDate + "_"
			}

			if j < len(xdxrs)-1 {
				ih.RegDate = ih.RegDate + "/"
				ih.XdxrDate = ih.XdxrDate + "/"
			}
		}
		iasp.AddField("REG DT")
		iasp.AddField("XDXR DT")

		item.Score += iasp.Score
	}
	return
}

// Score according to DYR.
// You get 2/3 max score if DYR >= 5% (0.05), get max score if DYR >= 6%
// Otherwise you get 0 if DYR <= 4%
func scoreDyr(ih *HiD, max float64) (s float64) {
	const THRESHOLD = 0.05
	const BOTTOM = 0.04
	const FACTOR = 0.18 // the smaller the factor, the more slowly the score regresses when under BOTTOM
	if ih.Dyr.Valid {
		if ih.Dyr.Float64 >= THRESHOLD {
			return max*2.0/3.0 + max/3.0*math.Min(1, (ih.Dyr.Float64-THRESHOLD)*100)
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
	case "DPR":
		return fmt.Sprintf("%.2f%%", h.Dpr.Float64*100)
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
	return fmt.Sprintf("HiD Value stocks for:\n" +
		" 1. Latest high yearly dividend yield ratio" +
		" 2. Dividend with progressive increase or constantly at high level" +
		" 3. Nearer registration date." +
		"Get warnings if:" +
		" 1. Dividend Payout Ratio is greater than 90%")
}
