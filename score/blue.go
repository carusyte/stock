package score

import (
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"fmt"
	"reflect"
	"github.com/pkg/errors"
	"math"
	"database/sql"
	"github.com/montanaflynn/stats"
	"strings"
)

// Search for stocks with excellent financial report.
// Medium to long term model, mainly focusing on yearly financial reports.
// · Low latest P/E, normally below 50
// · Growing EPS each year and quarter, up to 3 years
// · Low latest P/U
// · Growing UDPPS each year and quarter, up to 3 years
// Get warnings/penalties if:
// · High latest DAR
// · High average DAR, up to 3 years
type BlueChip struct {
	model.Finance
	Name       string
	Pe         sql.NullFloat64 `db:"pe"`
	Pu         sql.NullFloat64 `db:"pu"`
	Po         sql.NullFloat64 `db:"po"`
	EpsGrs     []float64
	EpsGrAvg   float64
	UdppsGrs   []float64
	UdppsGrAvg float64
	Dars       []float64
	DarAvg     float64
}

// The assessment metric diverts, some of them are somewhat negative correlated.
const (
	SCORE_PE            = 20.
	SCORE_GEPS          = 30.
	SCORE_PU            = 35.
	SCORE_GUDPPS        = 15.
	PENALTY_DAR         = 15.
	PE_THRESHOLD        = 50.
	BLUE_HIST_SPAN_YEAR = 3.
)

func (b *BlueChip) Geta() (r *Result) {
	return b.Get(nil, -1, false)
}

func (b *BlueChip) Get(s []string, limit int, ranked bool) (r *Result) {
	r = &Result{}
	r.PfIds = append(r.PfIds, b.Id())
	var blus []*BlueChip
	if s == nil || len(s) == 0 {
		sql, e := dot.Raw("BLUE")
		util.CheckErr(e, "failed to get BLUE sql")
		_, e = dbmap.Select(&blus, sql, PE_THRESHOLD)
		util.CheckErr(e, "failed to query database, sql:\n"+sql)
	} else {
		sql, e := dot.Raw("BLUE_SCOPED")
		util.CheckErr(e, "failed to get BLUE_SCOPED sql")
		sql = fmt.Sprintf(sql, strings.Join(s, ","))
		_, e = dbmap.Select(&blus, sql, PE_THRESHOLD)
		util.CheckErr(e, "failed to query database, sql:\n"+sql)
	}

	for _, ib := range blus {
		item := new(Item)
		r.AddItem(item)
		item.Code = ib.Code
		item.Name = ib.Name
		item.Profiles = make(map[string]*Profile)
		ip := new(Profile)
		item.Profiles[b.Id()] = ip
		ip.FieldHolder = ib

		hist := getFinHist(ib.Code, BLUE_HIST_SPAN_YEAR*4)

		ip.Score += sEps(ib, hist)
		ip.Score += sUdpps(ib, hist)
		ip.Score -= pDar(ib, hist)

		if ib.Dar.Valid && ib.Dar.Float64 >= 90 {
			item.Cmtf("DAR is high at %.0f", ib.Dar.Float64)
		}
		if ib.DarAvg >= 90 {
			item.Cmtf("AVG DAR is high at %.0f", ib.DarAvg)
		}

		item.Score += ip.Score
	}
	r.SetFields(b.Id(), b.Fields()...)
	if ranked {
		r.Sort()
	}
	r.Shrink(limit)
	return
}

// Fine for max penalty if
// · Latest DAR >= 100
// · Average DAR >= 95
// Baseline: Latest DAR <= 80% and avg DAR <= 70%
func pDar(b *BlueChip, hist []*model.Finance) (s float64) {
	MAX_DAR := 100.
	ZERO_DAR := 80.
	// fine latest DAR
	if !b.Dar.Valid || b.Dar.Float64 < 0 || b.Dar.Float64 <= ZERO_DAR {
		s = 0
	} else {
		s = 1. / 2. * PENALTY_DAR * math.Min(1, math.Pow((b.Dar.Float64-ZERO_DAR)/(MAX_DAR-ZERO_DAR), 4.37))
	}
	// fine average DAR
	dars := make([]float64, BLUE_HIST_SPAN_YEAR*4)
	for i, h := range hist {
		if h.Dar.Valid {
			dars[i] = h.Dar.Float64
		} else {
			dars[i] = 0
		}
	}
	avg, e := stats.Mean(dars)
	util.CheckErr(e, "failed to calculate mean for "+fmt.Sprintf("%+v", dars))
	b.DarAvg = avg
	if avg > 70 {
		s += 1. / 2. * PENALTY_DAR * math.Min(1, math.Pow((avg-70.)/(95.-70.), 2.1))
	}
	b.Dars = dars
	return
}

// Score by assessing UDPPS or P/U.
// P/U: Get max score if latest P/U <= 1, get 0 if P/U >= 10
// UDPPS: Get max score if UDPPS_YOY is all positive and complete, and avg UDPPS_YOY >= 10%;
//        Get 0 if avg negative growth rate is <= -70%
func sUdpps(b *BlueChip, fins []*model.Finance) (s float64) {
	ZERO_PU := 10.
	MAX_PU := 1.
	// score latest P/U
	if b.Pu.Float64 < 0 || b.Pu.Float64 >= ZERO_PU {
		s = 0
	} else {
		s = SCORE_PU * math.Min(1, math.Pow((ZERO_PU-b.Pu.Float64)/(ZERO_PU-MAX_PU), 0.5))
	}
	// score UDPPS growth rate
	grs := make([]float64, BLUE_HIST_SPAN_YEAR*4)
	ngrs := make([]float64, 0)
	for i, f := range fins {
		if f.UdppsYoy.Valid {
			grs[i] = f.UdppsYoy.Float64
			if grs[i] < 0 {
				ngrs = append(ngrs, grs[i])
			}
		} else {
			grs[i] = 0
		}
	}
	avg, e := stats.Mean(grs)
	util.CheckErr(e, "failed to calculate mean for "+fmt.Sprintf("%+v", grs))
	b.UdppsGrAvg = avg
	s += 2. / 5. * SCORE_GUDPPS * math.Min(1, math.Pow(float64(len(fins))/BLUE_HIST_SPAN_YEAR/4., 1.74))
	if avg >= -20. {
		s += 3. / 5. * SCORE_GUDPPS * math.Min(1, math.Pow((20.+avg)/30., 0.55))
	}
	if len(ngrs) > 0 {
		navg, e := stats.Mean(ngrs)
		util.CheckErr(e, "failed to calculate mean for "+fmt.Sprintf("%+v", ngrs))
		s -= SCORE_GUDPPS * math.Min(1, math.Pow(navg / -70., 3.12))
	}
	b.UdppsGrs = grs
	return
}

func getFinHist(code string, size int) (fins []*model.Finance) {
	sql, e := dot.Raw("BLUE_HIST")
	util.CheckErr(e, "failed to get BLUE_HIST sql")
	_, e = dbmap.Select(&fins, sql, code, size)
	util.CheckErr(e, "failed to query BLUE_HIST for "+code)
	return
}

// Score by assessing EPS or P/E
// P/E: Get max score if 0 < P/E <= 5, get 0 if P/E >= 40
// EPS GR: Get max score if EPS_YOY is all positive and complete, and avg EPS_YOY >= 15;
//         Get 0 if avg negative growth rate is <= -80%
func sEps(b *BlueChip, hist []*model.Finance) (s float64) {
	ZERO_PE := 40.
	MAX_PE := 5.
	// score latest P/E
	if b.Pe.Float64 < 0 || b.Pe.Float64 >= ZERO_PE {
		s = 0
	} else {
		s = SCORE_PE * math.Min(1, math.Pow((ZERO_PE-b.Pe.Float64)/(ZERO_PE-MAX_PE), 0.5))
	}
	// score EPS growth rate
	grs := make([]float64, BLUE_HIST_SPAN_YEAR*4)
	ngrs := make([]float64, 0)
	for i, f := range hist {
		if f.EpsYoy.Valid {
			grs[i] = f.EpsYoy.Float64
			if grs[i] < 0 {
				ngrs = append(ngrs, grs[i])
			}
		} else {
			grs[i] = 0
		}
	}
	avg, e := stats.Mean(grs)
	util.CheckErr(e, "failed to calculate mean for "+fmt.Sprintf("%+v", grs))
	b.EpsGrAvg = avg
	s += 2. / 5. * SCORE_GEPS * math.Min(1, math.Pow(float64(len(hist))/BLUE_HIST_SPAN_YEAR/4., 1.74))
	if avg >= -15. {
		s += 3. / 5. * SCORE_GEPS * math.Min(1, math.Pow((15.+avg)/30., 0.55))
	}
	if len(ngrs) > 0 {
		navg, e := stats.Mean(ngrs)
		util.CheckErr(e, "failed to calculate mean for "+fmt.Sprintf("%+v", ngrs))
		s -= SCORE_GEPS * math.Min(1, math.Pow(navg / -80., 3.12))
		s = math.Max(0, s)
	}
	b.EpsGrs = grs
	return
}

func (*BlueChip) Id() string {
	return "BLUE"
}

func (b *BlueChip) Fields() []string {
	return []string{"Latest Report", "PE", "EPS GR%",
					"EPS GR AVG%", "PU", "UDPPS GR%", "UDPPS GR AVG%",
					"DARS%", "DAR AVG%"}
}

func (b *BlueChip) GetFieldStr(name string) string {
	switch name {
	case "Latest Report":
		return b.Year
	case "PE":
		if b.Pe.Valid {
			return fmt.Sprintf("%.2f", b.Pe.Float64)
		} else {
			return "NaN"
		}
	case "EPS GR%":
		return util.SprintFa(b.EpsGrs, "%.2f", "/", 4)
	case "EPS GR AVG%":
		return fmt.Sprintf("%.2f", b.EpsGrAvg)
	case "PU":
		if b.Pu.Valid {
			return fmt.Sprintf("%.2f", b.Pu.Float64)
		} else {
			return "NaN"
		}
	case "UDPPS GR%":
		return util.SprintFa(b.UdppsGrs, "%.2f", "/", 4)
	case "UDPPS GR AVG%":
		return fmt.Sprintf("%.2f", b.UdppsGrAvg)
	case "DARS%":
		return util.SprintFa(b.Dars, "%.2f", "/", 4)
	case "DAR AVG%":
		return fmt.Sprintf("%.2f", b.DarAvg)
	default:
		r := reflect.ValueOf(b)
		f := reflect.Indirect(r).FieldByName(name)
		if !f.IsValid() {
			panic(errors.New("undefined field for BLUE: " + name))
		}
		return fmt.Sprintf("%+v", f.Interface())
	}
}

func (b *BlueChip) Description() string {
	return "Search for stocks with excellent financial report." +
		"Medium to long term model, mainly focusing on yearly financial reports." +
		"· Low latest P/E, normally below 50" +
		"· Growing EPS each year and quarter, up to 3 years" +
		"· Low latest P/U" +
		"· Growing UDPPS each year and quarter, up to 3 years" +
		"Get warnings/penalties if:" +
		"· High latest DAR" +
		"· High average DAR, up to 3 years"
}
