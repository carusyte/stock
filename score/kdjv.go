package score

import (
	"github.com/carusyte/stock/model"
	"math"
	"github.com/carusyte/stock/getd"
	"fmt"
	"github.com/carusyte/stock/util"
	"github.com/montanaflynn/stats"
	"time"
	"reflect"
	"errors"
)

// Medium to Long term model.
// Search for stocks with best KDJ form which closely matches the historical ones indicating the buy opportunity.
type KdjV struct {
	Code string
	Name string
	CCMO string
	CCWK string
	CCDY string
}

const (
	SCORE_KDJV_MONTH float64 = 40.0
	SCORE_KDJV_WEEK          = 30.0
	SCORE_KDJV_DAY           = 30.0
)

func (k *KdjV) GetFieldStr(name string) string {
	switch name {
	case "KDJ_DY":
		return k.CCDY
	case "KDJ_WK":
		return k.CCWK
	case "KDJ_MO":
		return k.CCMO
	default:
		r := reflect.ValueOf(k)
		f := reflect.Indirect(r).FieldByName(name)
		if !f.IsValid() {
			panic(errors.New("undefined field for KDJV: " + name))
		}
		return fmt.Sprintf("%+v", f.Interface())
	}
}

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
		kdjv.Code = s.Code
		kdjv.Name = s.Name
		item := new(Item)
		r.AddItem(item)
		item.Code = s.Code
		item.Name = s.Name
		item.Profiles = make(map[string]*Profile)
		ip := new(Profile)
		item.Profiles[k.Id()] = ip
		ip.FieldHolder = kdjv

		ip.Score += scoreKdjMon(kdjv) * SCORE_KDJV_MONTH
		ip.Score += scoreKdjWk(kdjv) * SCORE_KDJV_WEEK
		ip.Score += scoreKdjDy(kdjv) * SCORE_KDJV_DAY

		//warn if...

		ip.Score = math.Min(100, math.Max(0, ip.Score))
		item.Score += ip.Score
	}
	r.SetFields(k.Id(), k.Fields()...)
	if ranked {
		r.Sort()
	}
	r.Shrink(limit)
	return
}

//Score by assessing the coefficient of recent daily KDJ and historical data.
func scoreKdjDy(v *KdjV) (s float64) {
	rkdj := getd.GetKdjHist(v.Code, model.INDICATOR_DAY, 100)
	rkdj = getd.ToLstJDCross(rkdj)
	byhist := getd.GetKdjFeatDat(model.DAY, true)
	fcc, bcc, num, bm := getKdjCC(rkdj, byhist)
	v.CCDY = fmt.Sprintf("%.2f/%.2f/%.0f/%s\n", fcc, bcc, num, bm)
	s = 100 * math.Pow(fcc, 0.12)
	slhist := getd.GetKdjFeatDat(model.DAY, false)
	fcc, bcc, num, bm = getKdjCC(rkdj, slhist)
	v.CCDY += fmt.Sprintf("%.2f/%.2f/%.0f/%s\n", fcc, bcc, num, bm)
	s -= s * math.Pow(fcc, 0.02)
	return
}

//Score by assessing the coefficient of recent weekly KDJ and historical data.
func scoreKdjWk(v *KdjV) (s float64) {
	rkdj := getd.GetKdjHist(v.Code, model.INDICATOR_WEEK, 100)
	rkdj = getd.ToLstJDCross(rkdj)
	byhist := getd.GetKdjFeatDat(model.WEEK, true)
	fcc, bcc, num, bm := getKdjCC(rkdj, byhist)
	v.CCWK = fmt.Sprintf("%.2f/%.2f/%.0f/%s\n", fcc, bcc, num, bm)
	s = 100 * math.Pow(fcc, 0.12)
	slhist := getd.GetKdjFeatDat(model.WEEK, false)
	fcc, bcc, num, bm = getKdjCC(rkdj, slhist)
	v.CCWK += fmt.Sprintf("%.2f/%.2f/%.0f/%s\n", fcc, bcc, num, bm)
	s -= s * math.Pow(fcc, 0.02)
	return
}

//Score by assessing the coefficient of recent monthly KDJ and historical data.
func scoreKdjMon(v *KdjV) (s float64) {
	rkdj := getd.GetKdjHist(v.Code, model.INDICATOR_MONTH, 100)
	rkdj = getd.ToLstJDCross(rkdj)
	byhist := getd.GetKdjFeatDat(model.MONTH, true)
	fcc, bcc, num, bm := getKdjCC(rkdj, byhist)
	v.CCMO = fmt.Sprintf("%.2f/%.2f/%.0f/%s\n", fcc, bcc, num, bm)
	s = 100 * math.Pow(fcc, 0.12)
	slhist := getd.GetKdjFeatDat(model.MONTH, false)
	fcc, bcc, num, bm = getKdjCC(rkdj, slhist)
	v.CCMO += fmt.Sprintf("%.2f/%.2f/%.0f/%s\n", fcc, bcc, num, bm)
	s -= s * math.Pow(fcc, 0.02)
	return
}

// Evaluate CC against all historical kdj feature data of one direction.
// CC is assessed according to number of matches, matched feature freshness, and each matched CC.
// Returning the resulted final CC, best matched CC, number of match with CC greater than 0 and the best matched feature
// ID.
func getKdjCC(hist []*model.Indicator, fdsMap map[string][]*model.KDJfd) (fcc, bcc, num float64, bm string) {
	hk := make([]float64, len(hist))
	hd := make([]float64, len(hist))
	hj := make([]float64, len(hist))
	code := hist[0].Code
	for i, h := range hist {
		hk[i] = h.KDJ_K
		hd[i] = h.KDJ_D
		hj[i] = h.KDJ_J
	}
	bcc = math.Inf(-1)
	ccs := make([]float64, 0, 16)
	for fid, fd := range fdsMap {
		//skip the identical
		if code == fd[0].Code && hist[0].Klid == fd[0].Klid {
			continue
		}
		mod := 1.0
		tsmp, e := time.Parse("2006-01-02", fd[0].Feat.SmpDate)
		util.CheckErr(e, "failed to parse sample date: "+fd[0].Feat.SmpDate)
		days := time.Now().Sub(tsmp).Hours() / 24.0
		if days > 800 {
			mod = 0.8
		}
		k, d, j := extractKdjFd(fd)
		cc := bestKdjCC(hk, hd, hj, k, d, j)
		if cc >= 0 {
			cc *= mod
			ccs = append(ccs, cc)
			if cc > bcc {
				bcc = cc
				bm = fid
			}
		}
	}
	if bcc > math.Inf(-1) {
		mcc, e := stats.Mean(ccs)
		util.CheckErr(e, "failed to calculate mean for cc.")
		num = float64(len(ccs))
		fcc = bcc*0.8 + mcc*0.2
		fcc += (1 - fcc) * math.Min(1.0, math.Pow(num, 0.3)/5)
	}
	return
}

func bestKdjCC(sk, sd, sj, tk, td, tj []float64) float64 {
	if len(sk) > len(tk) {
		cc := math.Inf(-1)
		dif := len(sk) - len(tk)
		for i := 0; i <= dif; i++ {
			e := len(sk) - dif + i - 1
			tcc := calcKdjCC(sk[i:e], sd[i:e], sj[i:e], tk, td, tj)
			if tcc > cc {
				cc = tcc
			}
		}
		return cc
	} else if len(sk) < len(tk) {
		cc := math.Inf(-1)
		dif := len(tk) - len(sk)
		for i := 0; i <= dif; i++ {
			e := len(tk) - dif + i - 1
			tcc := calcKdjCC(sk, sd, sj, tk[i:e], td[i:e], tj[i:e])
			if tcc > cc {
				cc = tcc
			}
		}
		return cc
	} else {
		return calcKdjCC(sk, sd, sj, tk, td, tj)
	}
}

func calcKdjCC(sk, sd, sj, tk, td, tj []float64) float64 {
	kcc, e := stats.Correlation(sk, tk)
	util.CheckErr(e, "failed to calculate kcc")
	dcc, e := stats.Correlation(sd, td)
	util.CheckErr(e, "failed to calculate dcc")
	jcc, e := stats.Correlation(sj, tj)
	util.CheckErr(e, "failed to calculate jcc")
	return (kcc*1.0 + dcc*4.0 + jcc*5.0) / 10.0
}

func extractKdjFd(fds []*model.KDJfd) (k, d, j []float64) {
	for _, f := range fds {
		k = append(k, f.K)
		d = append(d, f.D)
		j = append(j, f.J)
	}
	return
}

func (k *KdjV) Id() string {
	return "KDJV"
}

func (k *KdjV) Fields() []string {
	return []string{"KDJ_DY", "KDJ_WK", "KDJ_MO"}
}

func (k *KdjV) Description() string {
	panic("implement me")
}

func (k *KdjV) Geta() (r *Result) {
	return k.Get(nil, -1, false)
}
