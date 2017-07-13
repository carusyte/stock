package score

import (
	"github.com/carusyte/stock/model"
	"math"
	"github.com/carusyte/stock/getd"
	"fmt"
	"github.com/carusyte/stock/util"
	"log"
	"github.com/montanaflynn/stats"
	"sort"
)

// Medium to Long term model.
// Search for stocks whose J value in KDJ indicator is near valley, considering all periods
// Golden cross death cross theory?
// Correlation to price fluctuation
type KdjV struct {
	model.Indicator
	Name  string
	CCMO  string
	CCWK  string
	CCDAY string
}

const (
	SCORE_KDJV_MONTH     float64 = 40.0
	SCORE_KDJV_WEEK              = 30.0
	SCORE_KDJV_DAY               = 30.0
	KDJ_PEAK_THRESHOLD           = 80.0
	KDJ_VALLEY_THRESHOLD         = 20.0
	KDJ_RETROSPECT               = 600
)

func (k *KdjV) GetFieldStr(name string) string {
	panic("implement me")
}

//TODO make it
func (k *KdjV) Get(stock []string, limit int, ranked bool) (r *Result) {
	r = &Result{}
	r.PfIds = append(r.PfIds, k.Id())
	var stks []*model.Stock
	if stock == nil || len(stock) == 0 {
		stks = getd.StocksDb()
	} else {
		stks = getd.StocksDbByCode(stock...)
	}

	for _, s := range stks {
		kdjv := new(KdjV)
		item := new(Item)
		r.AddItem(item)
		item.Code = s.Code
		item.Name = s.Name
		item.Profiles = make(map[string]*Profile)
		ip := new(Profile)
		item.Profiles[k.Id()] = ip
		ip.FieldHolder = kdjv

		ip.Score += scoreKdjMon(kdjv)

		//warn if...

		ip.Score = math.Max(0, ip.Score)
		item.Score += ip.Score
	}
	r.SetFields(k.Id(), k.Fields()...)
	if ranked {
		r.Sort()
	}
	r.Shrink(limit)
	return
}

func getKdjHist(code string, tab model.DBTab) (indcs []*model.Indicator) {
	sql, e := dot.Raw("KDJ_HIST")
	util.CheckErr(e, "failed to get KDJ_HIST sql")
	sql = fmt.Sprintf(sql, tab)
	_, e = dbmap.Select(&indcs, sql, code, KDJ_RETROSPECT)
	util.CheckErr(e, "failed to query kdj hist, sql:\n"+sql)
	return
}

//Get max score if monthly kdj forms a Golden Cross at the valley.
//Get 0 score if monthly kdj forms a Death Cross at the peak.
//Final score is modified by correlation coefficient.
func scoreKdjMon(v *KdjV) float64 {
	//TODO make it
	hist := getKdjHist(v.Code, model.INDICATOR_MONTH)
	klhist := getd.GetKlineDb(v.Code, model.KLINE_MONTH, KDJ_RETROSPECT, false)
	if len(hist) != len(klhist) {
		log.Panicf("%s %s and %s does not match", v.Code, model.INDICATOR_MONTH, model.KLINE_MONTH)
	}
	kcc, dcc, jcc := getKdjCC(hist, klhist)
	v.CCMO = fmt.Sprintf("%.2f/%.2f/%.2f", kcc, dcc, jcc)
	//TODO how to make use of cc
	//evaluate latest 5 kdj values
	s := len(hist) - 5
	if s < 0 {
		s = 0
	}
	for ; s < len(hist); s++ {

	}
	panic("implement me")
}

func getKdjCC(indicators []*model.Indicator, quotes []*model.Quote) (kcc, dcc, jcc float64) {
	k := make([]float64, len(indicators))
	d := make([]float64, len(indicators))
	j := make([]float64, len(indicators))
	q := make([]float64, len(quotes))
	for i, _ := range indicators {
		k[i] = indicators[i].KDJ_K
		d[i] = indicators[i].KDJ_D
		j[i] = indicators[i].KDJ_J
		q[i] = quotes[i].Close
	}
	var e error
	kcc, e = stats.Correlation(k, q)
	util.CheckErr(e, "failed to calculate kcc")
	dcc, e = stats.Correlation(d, q)
	util.CheckErr(e, "failed to calculate dcc")
	jcc, e = stats.Correlation(j, q)
	util.CheckErr(e, "failed to calculate jcc")
	return
}

func analyzeKdjCC(code string, expVr, mxrt float64, mxwait int) {
	hist := getKdjHist(code, model.INDICATOR_MONTH)
	klhist := getd.GetKlineDb(code, model.KLINE_MONTH, KDJ_RETROSPECT, false)
	if len(hist) != len(klhist) {
		log.Panicf("%s %s and %s does not match: %d:%d", code, model.INDICATOR_MONTH, model.KLINE_MONTH, len(hist),
			len(klhist))
	} else if len(hist) < 3 {
		log.Printf("%s historical data is insufficient for analyzing KDJ CC.", code)
		return
	}
	vr2Kdj := make(map[float64][]*model.Indicator)
	vrs := make([]float64, 0, 16)
	nl, nm, nh := 0, 0, 0
	vrl, vrm, vrh := 0.0, 0.0, 0.0
	k1, d1, j1, k2, d2, j2 := make([]float64, 0, 16), make([]float64, 0, 16), make([]float64, 0, 16), make([]float64, 0, 16),
		make([]float64, 0, 16), make([]float64, 0, 16)
	for i := 2; i < len(hist); i++ {
		kl := klhist[i]
		sc := kl.Close
		hc := math.Inf(-1)
		pc := klhist[i-1].Close
		for w, j := 0, 0; i+j < len(hist); j++ {
			nc := klhist[i+j].Close
			if nc > hc {
				hc = nc
			}
			if pc >= nc {
				rt := (hc - nc) / math.Abs(hc) * 100
				if rt >= mxrt || w > mxwait {
					break
				}
				if j > 0 {
					w++
				}
			} else {
				w = 0
			}
			pc = nc
		}
		actVr := (hc - sc) / math.Abs(sc) * 100
		if actVr > expVr {
			if k, e := vr2Kdj[actVr]; e {
				k = append(k, hist[i-2], hist[i-1])
			} else {
				vr2Kdj[actVr] = []*model.Indicator{hist[i-2], hist[i-1]}
				vrs = append(vrs, actVr)
			}
			k1 = append(k1, hist[i-1].KDJ_K)
			d1 = append(d1, hist[i-1].KDJ_D)
			j1 = append(j1, hist[i-1].KDJ_J)
			k2 = append(k2, hist[i-2].KDJ_K)
			d2 = append(d2, hist[i-2].KDJ_D)
			j2 = append(j2, hist[i-2].KDJ_J)
			if hist[i-1].KDJ_J >= KDJ_PEAK_THRESHOLD {
				nh++
				vrh += actVr
			} else if hist[i-1].KDJ_J >= KDJ_VALLEY_THRESHOLD {
				nm++
				vrm += actVr
			} else {
				nl++
				vrl += actVr
			}
		}
	}
	sort.SliceStable(vrs, func(i, j int) bool {
		return vrs[i] > vrs[j]
	})
	for _, v := range vrs {
		fmt.Printf("%.3f: ", v)
		for _, k := range vr2Kdj[v] {
			fmt.Printf("[%s|%.2f/%.2f/%.2f] ", k.Date, k.KDJ_K, k.KDJ_D, k.KDJ_J)
		}
		fmt.Println()
	}
	mk1, _ := stats.Median(k1)
	md1, _ := stats.Median(d1)
	mj1, _ := stats.Median(j1)
	mk2, _ := stats.Median(k2)
	md2, _ := stats.Median(d2)
	mj2, _ := stats.Median(j2)

	mek1, _ := stats.Mean(k1)
	med1, _ := stats.Mean(d1)
	mej1, _ := stats.Mean(j1)
	mek2, _ := stats.Mean(k2)
	med2, _ := stats.Mean(d2)
	mej2, _ := stats.Mean(j2)
	fmt.Printf("\nMedian: %.2f/%.2f/%.2f, %.2f/%.2f/%.2f     Mean: %.2f/%.2f/%.2f, %.2f/%.2f/%.2f\n", mk1, md1, mj1,
		mk2, md2, mj2, mek1, med1, mej1, mek2, med2, mej2)
	fmt.Printf("High: %.2f/%d/%.2f    Mid: %.2f/%d/%.2f    Low: %.2f/%d/%.2f\n", vrh, nh, vrh/float64(nh), vrm, nm,
		vrm/float64(nm), vrl, nl, vrl/float64(nl))
}

func (k *KdjV) Id() string {
	panic("implement me")
}

func (k *KdjV) Fields() []string {
	panic("implement me")
}

func (k *KdjV) Description() string {
	panic("implement me")
}

func (k *KdjV) Geta() (r *Result) {
	return k.Get(nil, -1, false)
}
