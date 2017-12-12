package score

import (
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/carusyte/stock/indc"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"github.com/montanaflynn/stats"
	"github.com/pkg/errors"
)

// BlueChip Search for stocks with excellent financial report.
// Medium to long term model, mainly focusing on annualy financial reports and predictions.
// · Low latest P/E
// · Growing EPS each year and quarter
// · Growing ROE each year and quarter
// · Low latest P/U
// · Growing UDPPS each year and quarter
// · Promissing financial prediction
// Get warnings/penalties if:
// · High latest DAR
// · High average DAR
type BlueChip struct {
	model.Finance
	Name       string
	Pe         sql.NullFloat64 `db:"pe"`
	Pu         sql.NullFloat64 `db:"pu"`
	Po         sql.NullFloat64 `db:"po"`
	RoeGrs     []float64
	EpsGrs     []float64
	EpsGrAvg   float64
	UdppsGrs   []float64
	UdppsGrAvg float64
	EpsPredict []float64
	EpsIndAvg  []float64
	NpPredict  []float64
	NpIndAvg   []float64
	Dars       []float64
	DarAvg     float64
}

//TODO add fin predict evalutation
// The assessment metric diverts, some of them are somewhat negatively correlated.
const (
	WeightPE     = 25.
	WeightGEPS   = 30.
	WeightROE    = 35.
	WeightPU     = 5.
	WeightGUDPPS = 5.
	PenaltyDAR   = 15.
	//PE_THRESHOLD        = 50.
	//BLUE_HIST_SPAN_YEAR = 3.
	ExtWeightFinPredict = 50.
)

var (
	roeLambda = math.NaN()
)

//Geta evaluate scores of all stock
func (b *BlueChip) Geta() (r *Result) {
	return b.Get(nil, -1, false)
}

//Get evaluate scores of the given stock codes,
//limit: limit the result numbers
//ranked: sort the result by score in descending order
func (b *BlueChip) Get(s []string, limit int, ranked bool) (r *Result) {
	r = &Result{}
	r.PfIds = append(r.PfIds, b.Id())
	var blus []*BlueChip
	if s == nil || len(s) == 0 {
		sql, e := dot.Raw("BLUE")
		util.CheckErr(e, "failed to get BLUE sql")
		_, e = dbmap.Select(&blus, sql)
		util.CheckErr(e, "failed to query database, sql:\n"+sql)
	} else {
		sql, e := dot.Raw("BLUE_SCOPED")
		util.CheckErr(e, "failed to get BLUE_SCOPED sql")
		sql = fmt.Sprintf(sql, strings.Join(s, ","))
		_, e = dbmap.Select(&blus, sql)
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

		wts := make(WtScore)
		hist := getFinHist(ib.Code)
		sFinPredict(ib, hist, wts)
		sEps(ib, hist, wts)
		sROE(ib, hist, wts)
		sUdpps(ib, hist, wts)
		pDar(ib, hist, wts)
		ip.Score = wts.Sum()

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

// Score based on self ROE growth rate.
// get max score if historical data is complete for 5 years
// and MA growth rate >= the best ranked 25% roe annual growth rate.
func sROE(b *BlueChip, finHist []*model.Finance, wts WtScore) {
	var (
		ygrs []float64
		qgrs []float64
		qcnt = 0
		s    = 0.
	)
	for _, f := range finHist {
		if "12" == f.Year[5:7] {
			if f.RoeYoy.Valid {
				ygrs = append(ygrs, f.RoeYoy.Float64)
			} else {
				ygrs = append(ygrs, math.NaN())
			}
		} else if len(ygrs) == 0 {
			qcnt++
			if f.RoeYoy.Valid {
				qgrs = append(qgrs, f.RoeYoy.Float64)
			}
		}
		if len(ygrs) >= 5 {
			break
		}
	}
	wtHead := 0.
	if qcnt > 0 {
		wtHead = 0.05 * float64(qcnt)
		if len(qgrs) > 0 {
			roeGr, e := stats.Mean(qgrs)
			util.CheckErr(e, fmt.Sprintf("%s failed to calculate mean for quarterly roe: %+v",
				b.Code, qgrs))
			s = wtHead * scoreRoeGr(roeGr)
		}
	}
	cf := 0.
	if len(ygrs) >= 5 {
		cf = 1.
	} else if len(ygrs) > 0 {
		cf = 0.05*float64(len(ygrs)) + 0.75
	}
	maRoeGr := 0.
	for i := len(ygrs) - 1; i >= 0; i-- {
		if math.IsNaN(ygrs[i]) {
			maRoeGr = math.NaN()
			break
		}
		if i < len(ygrs)-1 {
			maRoeGr = (maRoeGr + ygrs[i]) / 2.
		} else {
			maRoeGr = ygrs[i]
		}
	}
	if !math.IsNaN(maRoeGr) {
		s += (1 - wtHead) * cf * scoreRoeGr(maRoeGr)
	}
	wts.Add("ROE_GR", s, WeightROE)
	b.RoeGrs = ygrs
}

func scoreRoeGr(roe float64) (s float64) {
	lambda := getRoeLambda(0.25)
	if roe >= lambda {
		return 100.
	} else if roe <= 0. {
		return 0.
	}
	return 100. * math.Log((math.E-1.)/lambda*roe+1.)
}

func getRoeLambda(pos float64) float64 {
	if math.IsNaN(roeLambda) {
		c, e := dbmap.SelectInt("select count(*) from finance where year like '%-12-%'")
		util.CheckErr(e, "failed to count annual report in finance table")
		rank := math.Ceil(pos * float64(c))
		roeLambda, e = dbmap.SelectFloat("select roe_yoy from finance "+
			"where year like '%-12-%' order by roe_yoy desc limit 1 offset ?", int(rank))
		util.CheckErr(e, "failed to query roe lambda from finance table")
	}
	return roeLambda
}

// Score based on the financial performance prediction.
// If no prediction is available, its weight is not added to the total score.
// Evaluation composition:
// Number of institution providing the prediction; (as confidence factor to adjust weight)
// Immediate comparison of nearest prediction and latest annual report; (0.35)
// Comparison between EPS average and industrial average; (0.25)
// Trend of growing performance, including growing rate. (0.4)
func sFinPredict(b *BlueChip, finHist []*model.Finance, wts WtScore) {
	fps := getFinPredicts(b.Code)
	if len(fps) == 0 {
		return
	}
	var (
		larp *model.Finance //latest annual report
	)
	epsCofa, npCofa := fiprCofa(fps)
	// find larp
	for _, f := range finHist {
		if "12" == f.Year[5:7] {
			larp = f
			break
		}
	}
	if larp != nil {
		// remove overlap year
		if strings.HasPrefix(larp.Year, fps[0].Year) {
			if len(fps) > 1 {
				fps = fps[1:]
			} else {
				return
			}
		}
		nextPerf(larp, fps, wts, epsCofa, npCofa, 0.35)
	}
	compIndustrial(fps, wts, epsCofa, npCofa, 0.25)
	perfTrend(larp, fps, wts, epsCofa, npCofa, 0.4)
	for _, fp := range fps {
		if fp.EpsAvg.Valid {
			b.EpsPredict = append(b.EpsPredict, fp.EpsAvg.Float64)
		} else {
			b.EpsPredict = append(b.EpsPredict, math.NaN())
		}
		if fp.EpsIndAvg.Valid {
			b.EpsIndAvg = append(b.EpsIndAvg, fp.EpsIndAvg.Float64)
		} else {
			b.EpsIndAvg = append(b.EpsIndAvg, math.NaN())
		}
		if fp.NpAvg.Valid {
			b.NpPredict = append(b.NpPredict, fp.NpAvg.Float64)
		} else {
			b.NpPredict = append(b.NpPredict, math.NaN())
		}
		if fp.NpIndAvg.Valid {
			b.NpIndAvg = append(b.NpIndAvg, fp.NpIndAvg.Float64)
		} else {
			b.NpIndAvg = append(b.NpIndAvg, math.NaN())
		}
	}
}

// evaluate trend of prediction.
// focuses on the growing rate of eps and np prediction.
// max: pi*e/(10*sqrt(pi+e)) (around 0.35)
func perfTrend(larp *model.Finance, fps []*model.FinPredict, wts WtScore, epsCofa, npCofa []float64, wtPortion float64) {
	var (
		maEps    = 0.
		maNp     = 0.
		sidx     = 0
		avgWtEps = 0.
		avgWtNp  = 0.
		cntEps   = 0.
		cntNp    = 0.
		s        = 0.
		max      = (math.Pi * math.E) / (10. * math.Sqrt(math.Pi+math.E))
	)
	for i, fpt := range fps {
		fpy, e := strconv.Atoi(fpt.Year)
		util.CheckErr(e, "invalid year value "+fpt.Year)
		larpy, e := strconv.Atoi(larp.Year[0:4])
		util.CheckErr(e, "invalid year value "+larp.Year)
		if fpy-1 == larpy {
			sidx = i
			break
		}
	}
	//FIXME zero denominator issue
	if larp != nil {
		cntEps++
		cntNp++
		if larp.Eps.Valid && fps[sidx].EpsAvg.Valid {
			if larp.Eps.Float64 == 0. {
				if fps[sidx].EpsAvg.Float64 > 0. {
					maEps = 1.
				} else if fps[sidx].EpsAvg.Float64 < 0. {
					maEps = -1.
				}
			} else {
				maEps = (fps[sidx].EpsAvg.Float64 - larp.Eps.Float64) / math.Abs(larp.Eps.Float64)
			}
			avgWtEps += epsCofa[sidx]
		}
		if larp.Np.Valid && fps[sidx].NpAvg.Valid {
			if larp.Np.Float64 == 0. {
				if fps[sidx].NpAvg.Float64 > 0. {
					maNp = 1.
				} else if fps[sidx].NpAvg.Float64 < 0. {
					maNp = -1.
				}
			} else {
				maNp = (fps[sidx].NpAvg.Float64 - larp.Np.Float64) / math.Abs(larp.Np.Float64)
			}
			avgWtNp += npCofa[sidx]
		}
	}
	for i := sidx + 1; i < len(fps); i++ {
		cntEps++
		cntNp++
		if fps[i].EpsAvg.Valid && fps[i-1].EpsAvg.Valid {
			if fps[i-1].EpsAvg.Float64 == 0. {
				if fps[i].EpsAvg.Float64 > 0. {
					maEps += 1.
				} else if fps[i].EpsAvg.Float64 < 0. {
					maEps -= 1.
				}
			} else {
				maEps += (fps[i].EpsAvg.Float64 - fps[i-1].EpsAvg.Float64) / math.Abs(fps[i-1].EpsAvg.Float64)
			}
			maEps /= 2.
			avgWtEps += epsCofa[i]
		}
		if fps[i].NpAvg.Valid && fps[i-1].NpAvg.Valid {
			if fps[i-1].NpAvg.Float64 == 0. {
				if fps[i].NpAvg.Float64 > 0. {
					maNp += 1.
				} else if fps[i].NpAvg.Float64 < 0. {
					maNp -= 1.
				}
			} else {
				maNp += (fps[i].NpAvg.Float64 - fps[i-1].NpAvg.Float64) / math.Abs(fps[i-1].NpAvg.Float64)
			}
			maNp /= 2.
			avgWtNp += npCofa[i]
		}
	}
	avgWtEps /= float64(cntEps)
	avgWtNp /= float64(cntNp)
	if maEps > max {
		s = 100.
	} else if maEps < 0. {
		s = 0.
	} else {
		s = 100. * math.Log(10.*(math.E-1.)*math.Sqrt(math.Pi+math.E)/(math.Pi*math.E)*maEps+1.)
	}
	wts.Add("FP_GR_EPS", s, ExtWeightFinPredict*wtPortion*avgWtEps*0.7)
	//TODO calculates NP score
	if maNp > max {
		s = 100.
	} else if maNp < 0. {
		s = 0.
	} else {
		s = 100. * math.Log(10.*(math.E-1.)*math.Sqrt(math.Pi+math.E)/(math.Pi*math.E)*maNp+1.)
	}
	wts.Add("FP_GR_NP", s, ExtWeightFinPredict*wtPortion*avgWtNp*0.3)
}

// compare eps and np prediction with industrial performance
// max: indication of surpassing industrial performance
func compIndustrial(fps []*model.FinPredict, wts WtScore, epsCofa, npCofa []float64, wtPortion float64) {
	var (
		difEps   []float64
		difNp    []float64
		cpgEps   = 0
		cpgNp    = 0
		avgWtEps = 0.
		avgWtNp  = 0.
		s        = 0.
	)
	for i, f := range fps {
		if f.EpsAvg.Valid && f.EpsIndAvg.Valid {
			difEps = append(difEps, f.EpsAvg.Float64-f.EpsIndAvg.Float64)
			avgWtEps += epsCofa[i]
		} else {
			difEps = append(difEps, math.NaN())
		}
		if f.NpAvg.Valid && f.NpIndAvg.Valid {
			difNp = append(difNp, f.NpAvg.Float64-f.NpIndAvg.Float64)
			avgWtNp += npCofa[i]
		} else {
			difNp = append(difNp, math.NaN())
		}
	}
	avgWtEps /= float64(len(difEps))
	avgWtNp /= float64(len(difNp))
	if len(difEps) == 1 {
		if difEps[0] > 0 {
			s = 100.
		} else if difEps[0] < 0. {
			s = 0.
		} else {
			s = 60.
		}
		wts.Add("FP_COMP_EPS", s, ExtWeightFinPredict*wtPortion*avgWtEps*0.7)
	} else {
		for i, d := range difEps {
			if i > 0 && d-difEps[i-1] > 0 {
				cpgEps++
			}
		}
		s = 100. * float64(cpgEps) / float64(len(difEps)-1)
		wts.Add("FP_COMP_EPS", s, ExtWeightFinPredict*wtPortion*avgWtEps*0.7)
	}
	if len(difNp) == 1 {
		if difNp[0] > 0 {
			s = 100.
		} else if difNp[0] < 0. {
			s = 0.
		} else {
			s = 60.
		}
		wts.Add("FP_COMP_NP", s, ExtWeightFinPredict*wtPortion*avgWtNp*0.3)
	} else {
		for i, d := range difNp {
			if i > 0 && d-difNp[i-1] > 0 {
				cpgNp++
			}
		}
		s = 100. * float64(cpgNp) / float64(len(difNp)-1)
		wts.Add("FP_COMP_NP", s, ExtWeightFinPredict*wtPortion*avgWtNp*0.3)
	}
}

// evaluation of next year performance prediction
// max: >= pi/10 better than last year's eps growth rate & >= 0.5 better than last year's np growth rate
func nextPerf(larp *model.Finance, fps []*model.FinPredict, wts WtScore, epsCofa, npCofa []float64, wtPortion float64) {
	var (
		fp          *model.FinPredict
		epscf, npcf float64
	)
	for i, fpt := range fps {
		fpy, e := strconv.Atoi(fpt.Year)
		util.CheckErr(e, "invalid year value "+fpt.Year)
		larpy, e := strconv.Atoi(larp.Year[0:4])
		util.CheckErr(e, "invalid year value "+larp.Year)
		if fpy-1 == larpy {
			fp = fpt
			epscf = epsCofa[i]
			npcf = npCofa[i]
			break
		}
	}
	if fp != nil {
		s := 0.
		if fp.EpsAvg.Valid && larp.Eps.Valid && larp.EpsYoy.Valid && fp.EpsAvg.Float64 > larp.Eps.Float64 {
			ngr := 0.
			if larp.Eps.Float64 == 0. {
				ngr = 100.
			} else {
				ngr = (fp.EpsAvg.Float64 - larp.Eps.Float64) / math.Abs(larp.Eps.Float64) * 100.
			}
			if ngr > larp.EpsYoy.Float64 {
				dgr := 0.
				if larp.EpsYoy.Float64 == 0. {
					dgr = 1.
				} else {
					dgr = (ngr - larp.EpsYoy.Float64) / math.Abs(larp.EpsYoy.Float64)
				}
				if dgr > math.Pi/10. {
					s = 100.
				} else {
					s = 100. * math.Log(10./math.Pi*(math.E-1.)*dgr+1.)
				}
				wts.Add("FP_NEXT_EPS", s, ExtWeightFinPredict*wtPortion*epscf*0.7)
			}
		}
		if fp.NpAvg.Valid && larp.Np.Valid && larp.NpYoy.Valid && fp.NpAvg.Float64 > larp.Np.Float64 {
			ngr := 0.
			if larp.Np.Float64 == 0. {
				ngr = 100.
			} else {
				ngr = (fp.NpAvg.Float64 - larp.Np.Float64) / math.Abs(larp.Np.Float64) * 100.
			}
			if ngr > larp.NpYoy.Float64 {
				dgr := 0.
				if larp.NpYoy.Float64 == 0. {
					dgr = 1.
				} else {
					dgr = (ngr - larp.NpYoy.Float64) / math.Abs(larp.NpYoy.Float64)
				}
				if dgr > 0.5 {
					s = 100.
				} else {
					s = 100. * math.Log(2*(math.E-1.)*dgr+1.)
				}
				wts.Add("FP_NEXT_NP", s, ExtWeightFinPredict*wtPortion*npcf*0.3)
			}
		}
	}
}

// calculates EPS and NP confidence factor
func fiprCofa(fps []*model.FinPredict) (epsCofa, npCofa []float64) {
	epsCofa = make([]float64, len(fps))
	npCofa = make([]float64, len(fps))
	for i, fp := range fps {
		if fp.EpsNum.Valid {
			epsCofa[i] = cofa(float64(fp.EpsNum.Int64))
		} else {
			epsCofa[i] = 0
		}
		if fp.NpNum.Valid {
			npCofa[i] = cofa(float64(fp.NpNum.Int64))
		} else {
			npCofa[i] = 0
		}
	}
	return
}

// calculates confidence factor.
// num(1, 10) -> cofa(0.6, 1)
func cofa(num float64) float64 {
	if num < 1 {
		return 0
	}
	if num >= 10 {
		return 1
	}
	return 0.5835 + 0.0187*num - 0.00097726*math.Pow(num, 2) + 0.00033333*math.Pow(num, 3)
}

// Fine for max penalty if
// · Latest DAR >= 100
// · SMA(DAR,3) >= 95
// Baseline: Latest DAR <= 80% and SMA DAR <= 70%
func pDar(b *BlueChip, hist []*model.Finance, wts WtScore) {
	MAX_DAR := 100.
	ZERO_DAR := 80.
	s := .0
	// fine latest DAR
	if b.Dar.Valid && b.Dar.Float64 > ZERO_DAR {
		s = 50. * math.Min(1, math.Pow((b.Dar.Float64-ZERO_DAR)/(MAX_DAR-ZERO_DAR), 4.37))
	}
	// fine average DAR
	dars := make([]float64, 0, 16)
	for _, h := range hist {
		if h.Dar.Valid {
			dars = append(dars, h.Dar.Float64)
		} else {
			// default penalty for no DAR data available
			dars = append(dars, 80)
		}
	}
	if len(dars) > 4 {
		b.Dars = dars[:4]
	} else {
		b.Dars = dars
	}
	var (
		avg float64
		e   error
	)
	if len(dars) > 2 {
		rdars := util.ReverseF64s(dars, true)
		madars := indc.SMA(rdars, 3, 1)
		avg = madars[len(madars)-1]
	} else {
		avg, e = stats.Mean(dars)
		util.CheckErr(e, "failed to calculate mean for "+fmt.Sprintf("%+v", dars))
	}
	b.DarAvg = avg
	if avg > 70 {
		s += 50. * math.Min(1, math.Pow((avg-70.)/(95.-70.), 2.1))
	}
	wts.Add("PDAR", s, PenaltyDAR)
	return
}

// Score by assessing UDPPS or P/U.
// P/U: Get max score if latest P/U <= 1, get 0 if P/U >= 10
// UDPPS: Get max score if UDPPS_YOY is all positive and complete for 3 years, and SMA UDPPS_YOY >= 10%;
//        Get 0 if avg negative growth rate is <= -70%
func sUdpps(b *BlueChip, fins []*model.Finance, wts WtScore) {
	ZERO_PU := 10.
	MAX_PU := 1.
	s := .0
	// score latest P/U
	if b.Pu.Valid && b.Pu.Float64 >= 0 && b.Pu.Float64 < ZERO_PU {
		s = 100. * math.Min(1, math.Pow((ZERO_PU-b.Pu.Float64)/(ZERO_PU-MAX_PU), 0.5))
	}
	wts.Add("PU", s, WeightPU)
	// score UDPPS growth rate
	grs := make([]float64, 0, 16)
	ngrs := make([]float64, 0, 16)
	pnum := .0
	countyr := true
	for i, f := range fins {
		if f.UdppsYoy.Valid {
			grs = append(grs, f.UdppsYoy.Float64)
			if grs[i] < 0 {
				if i < 4 {
					ngrs = append(ngrs, grs[i])
				}
				countyr = false
			}
			if countyr {
				pnum++
			}
		} else {
			grs = append(grs, 0)
			countyr = false
		}
	}
	if len(grs) > 4 {
		b.UdppsGrs = grs[:4]
	} else {
		b.UdppsGrs = grs
	}
	var avg = .0
	var e error
	if len(grs) > 2 {
		rgrs := util.ReverseF64s(grs, true)
		magrs := indc.SMA(rgrs, 3, 1)
		avg = magrs[len(magrs)-1]
	} else {
		avg, e = stats.Mean(grs)
		util.CheckErr(e, "failed to calculate mean for "+fmt.Sprintf("%+v", grs))
	}
	b.UdppsGrAvg = avg
	s = 40. * math.Min(1, math.Log((math.E-1)*pnum/4.+1))
	if avg >= -20. {
		s += 60. * math.Min(1, math.Pow((20.+avg)/30., 0.55))
	}
	if len(ngrs) > 0 {
		navg, e := stats.Mean(ngrs)
		util.CheckErr(e, "failed to calculate mean for "+fmt.Sprintf("%+v", ngrs))
		s -= 100. * math.Min(1, math.Pow(navg/-70., 3.12))
		s = math.Max(0, s)
	}
	wts.Add("GUDPPS", s, WeightGUDPPS)
	return
}

func getFinHist(code string) (fins []*model.Finance) {
	sql, e := dot.Raw("BLUE_HIST")
	util.CheckErr(e, "failed to get BLUE_HIST sql")
	_, e = dbmap.Select(&fins, sql, code)
	util.CheckErr(e, "failed to query BLUE_HIST for "+code)
	return
}

func getFinPredicts(code string) (fps []*model.FinPredict) {
	sql, e := dot.Raw("FIN_PREDICT")
	util.CheckErr(e, "failed to get FIN_PREDICT sql")
	_, e = dbmap.Select(&fps, sql, code)
	util.CheckErr(e, "failed to query FIN_PREDICT for "+code)
	return
}

// Score by assessing EPS or P/E
// P/E: Get max score if 0 < P/E <= 5, get 0 if P/E >= 40
// EPS GR: Get max score if EPS_YOY is all positive and complete for 3 years, and SMA EPS_YOY >= 15;
//         Get 0 if recent 5 avg negative growth rate is <= -80%
func sEps(b *BlueChip, hist []*model.Finance, wts WtScore) {
	ZERO_PE := 80.
	MAX_PE := 5.
	// score latest P/E
	s := .0
	if b.Pe.Float64 > 0 && b.Pe.Float64 < ZERO_PE {
		s = 100. * math.Min(1, math.Pow((ZERO_PE-b.Pe.Float64)/(ZERO_PE-MAX_PE), 0.5))
	}
	wts.Add("PE", s, WeightPE)
	// score EPS growth rate
	grs := make([]float64, 0, 16)
	ngrs := make([]float64, 0, 16)
	pnum := .0
	count := true
	for i, f := range hist {
		if f.EpsYoy.Valid {
			grs = append(grs, f.EpsYoy.Float64)
			if grs[i] < 0 {
				if i < 4 {
					ngrs = append(ngrs, grs[i])
				}
				count = false
			} else if count {
				pnum++
			}
		} else {
			grs = append(grs, 0)
			count = false
		}
	}
	if len(grs) > 4 {
		b.EpsGrs = grs[:4]
	} else {
		b.EpsGrs = grs
	}
	var avg = .0
	var e error
	if len(grs) > 2 {
		rgrs := util.ReverseF64s(grs, true)
		magrs := indc.SMA(rgrs, 3, 1)
		avg = magrs[len(magrs)-1]
	} else {
		avg, e = stats.Mean(grs)
		util.CheckErr(e, "failed to calculate mean for "+fmt.Sprintf("%+v", grs))
	}
	b.EpsGrAvg = avg
	s = 40. * math.Min(1, math.Log((math.E-1)*pnum/4.+1))
	if avg >= -15. {
		s += 60. * math.Min(1, math.Pow((15.+avg)/30., 1.75))
	}
	if len(ngrs) > 0 {
		navg, e := stats.Mean(ngrs)
		util.CheckErr(e, "failed to calculate mean for "+fmt.Sprintf("%+v", ngrs))
		s -= 70. * math.Min(1, math.Pow(navg/-80., 3.12))
		s = math.Max(0, s)
	}
	wts.Add("GEPS", s, WeightGEPS)
	return
}

//Id the identifier of this scorer
func (*BlueChip) Id() string {
	return "BLUE"
}

//Fields returns the related fields assessed by this scorer
func (b *BlueChip) Fields() []string {
	return []string{"Latest Report", "PE", "ROE GR%", "EPS GR%",
		"EPS GR AVG%", "PU", "UDPPS GR%", "UDPPS GR AVG%",
		"EPS Predict", "EPS Industry", "NP Predict", "NP Industry",
		"DARS%", "DAR AVG%"}
}

//GetFieldStr returns the string representation of the specified field.
func (b *BlueChip) GetFieldStr(name string) string {
	switch name {
	case "Latest Report":
		return b.Year
	case "PE":
		if b.Pe.Valid {
			return fmt.Sprintf("%.2f", b.Pe.Float64)
		}
		return "NaN"
	case "ROE GR%":
		return util.SprintFa(b.RoeGrs, "%.2f", "/", 5)
	case "EPS GR%":
		return util.SprintFa(b.EpsGrs, "%.2f", "/", 4)
	case "EPS GR AVG%":
		return fmt.Sprintf("%.2f", b.EpsGrAvg)
	case "PU":
		if b.Pu.Valid {
			return fmt.Sprintf("%.2f", b.Pu.Float64)
		}
		return "NaN"
	case "UDPPS GR%":
		return util.SprintFa(b.UdppsGrs, "%.2f", "/", 4)
	case "UDPPS GR AVG%":
		return fmt.Sprintf("%.2f", b.UdppsGrAvg)
	case "EPS Predict":
		return util.SprintFa(b.EpsPredict, "%.2f", "/", 4)
	case "EPS Industry":
		return util.SprintFa(b.EpsIndAvg, "%.2f", "/", 4)
	case "NP Predict":
		return util.SprintFa(b.NpPredict, "%.2f", "/", 4)
	case "NP Industry":
		return util.SprintFa(b.NpIndAvg, "%.2f", "/", 4)
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

// Description shows how this scorer evaluates the object
func (b *BlueChip) Description() string {
	return "Search for stocks with excellent financial report." +
		"Medium to long term model, mainly focusing on annualy financial reports." +
		"· Low latest P/E, normally below 50" +
		"· Growing EPS each year and quarter, up to 3 years" +
		"· Growing ROE each year and quarter" +
		"· Low latest P/U" +
		"· Growing UDPPS each year and quarter, up to 3 years" +
		"· Promissing financial prediction" +
		"Get warnings/penalties if:" +
		"· High latest DAR" +
		"· High average DAR, up to 3 years"
}
