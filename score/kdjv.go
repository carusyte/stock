package score

import (
	"github.com/carusyte/stock/model"
	"math"
	"github.com/carusyte/stock/getd"
	"fmt"
	"github.com/carusyte/stock/util"
	"time"
	"reflect"
	"errors"
	logr "github.com/sirupsen/logrus"
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

	//TODO need to speed up the evaluation process, now cost nearly 2 min each stock
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
		ip.Score /= SCORE_KDJV_MONTH + SCORE_KDJV_WEEK + SCORE_KDJV_DAY

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
	start := time.Now()
	byhist := getd.GetKdjFeatDat(model.DAY, true)
	logr.Debugf("DAILY kdj_feat_dat: %.2f", time.Since(start).Seconds())
	start = time.Now()
	rbuy := getKdjCCRatio(rkdj, byhist)
	s = 100 * math.Pow(rbuy, 0.33)
	slhist := getd.GetKdjFeatDat(model.DAY, false)
	rsell := getKdjCCRatio(rkdj, slhist)
	s -= s * math.Pow(rsell, 0.25)
	logr.Debugf("DAILY score calculation: %.2f", time.Since(start).Seconds())
	v.CCDY = fmt.Sprintf("%.2f : %.2f", rbuy, rsell)
	return
}

//Score by assessing the coefficient of recent weekly KDJ and historical data.
func scoreKdjWk(v *KdjV) (s float64) {
	rkdj := getd.GetKdjHist(v.Code, model.INDICATOR_WEEK, 100)
	rkdj = getd.ToLstJDCross(rkdj)
	start := time.Now()
	byhist := getd.GetKdjFeatDat(model.WEEK, true)
	logr.Debugf("WEEKLY kdj_feat_dat: %.2f", time.Since(start).Seconds())
	start = time.Now()
	rbuy := getKdjCCRatio(rkdj, byhist)
	s = 100 * math.Pow(rbuy, 0.33)
	slhist := getd.GetKdjFeatDat(model.WEEK, false)
	rsell := getKdjCCRatio(rkdj, slhist)
	s -= s * math.Pow(rsell, 0.25)
	logr.Debugf("WEEKLY score calculation: %.2f", time.Since(start).Seconds())
	v.CCWK = fmt.Sprintf("%.2f : %.2f", rbuy, rsell)
	return
}

//Score by assessing the coefficient of recent monthly KDJ and historical data.
func scoreKdjMon(v *KdjV) (s float64) {
	rkdj := getd.GetKdjHist(v.Code, model.INDICATOR_MONTH, 100)
	rkdj = getd.ToLstJDCross(rkdj)
	start := time.Now()
	byhist := getd.GetKdjFeatDat(model.MONTH, true)
	logr.Debugf("MONTHLY kdj_feat_dat: %.2f", time.Since(start).Seconds())
	start = time.Now()
	rbuy := getKdjCCRatio(rkdj, byhist)
	s = 100 * math.Pow(rbuy, 0.33)
	slhist := getd.GetKdjFeatDat(model.MONTH, false)
	rsell := getKdjCCRatio(rkdj, slhist)
	s -= s * math.Pow(rsell, 0.25)
	logr.Debugf("MONTHLY score calculation: %.2f", time.Since(start).Seconds())
	v.CCMO = fmt.Sprintf("%.2f : %.2f", rbuy, rsell)
	return
}

// Calculates ratio of historical data likely matching the given feature data.
func getKdjCCRatio(hist []*model.Indicator, fdsMap map[string][]*model.KDJfd) (r float64) {
	//TODO refine the meaning of return value to be more useful
	hk := make([]float64, len(hist))
	hd := make([]float64, len(hist))
	hj := make([]float64, len(hist))
	code := hist[0].Code
	for i, h := range hist {
		hk[i] = h.KDJ_K
		hd[i] = h.KDJ_D
		hj[i] = h.KDJ_J
	}
	c := .0
	for _, fd := range fdsMap {
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
		cc := bestKdjCC(hk, hd, hj, k, d, j) * mod
		if cc >= 0.5 {
			c++
		}
	}
	r = c / float64(len(fdsMap))
	return
}

func bestKdjCC(sk, sd, sj, tk, td, tj []float64) float64 {
	dif := len(sk) - len(tk)
	if dif > 0 {
		cc := -100.0
		for i := 0; i <= dif; i++ {
			e := len(sk) - dif + i
			tcc := calcKdjCC(sk[i:e], sd[i:e], sj[i:e], tk, td, tj)
			if tcc > cc {
				cc = tcc
			}
		}
		return cc
	} else if dif < 0 {
		cc := -100.0
		dif *= -1
		for i := 0; i <= dif; i++ {
			e := len(tk) - dif + i
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
	kcc, e := util.Devi(sk, tk)
	util.CheckErr(e, "failed to calculate kcc")
	dcc, e := util.Devi(sd, td)
	util.CheckErr(e, "failed to calculate dcc")
	jcc, e := util.Devi(sj, tj)
	util.CheckErr(e, "failed to calculate jcc")
	scc := (kcc*1.0 + dcc*4.0 + jcc*5.0) / 10.0
	return -0.001*math.Pow(scc, math.E) + 1
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
