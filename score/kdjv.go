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
	"github.com/montanaflynn/stats"
	"log"
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
	start := time.Now()
	bymo := getd.GetKdjFeatDat(model.MONTH, true)
	slmo := getd.GetKdjFeatDat(model.MONTH, false)
	bywk := getd.GetKdjFeatDat(model.WEEK, true)
	slwk := getd.GetKdjFeatDat(model.WEEK, false)
	bydy := getd.GetKdjFeatDat(model.DAY, true)
	sldy := getd.GetKdjFeatDat(model.DAY, false)
	logr.Debugf("query kdj_feat_dat: %.2f", time.Since(start).Seconds())
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

		kdjhist := getd.ToLstJDCross(getd.GetKdjHist(s.Code, model.INDICATOR_MONTH, 100))
		ip.Score += scoreKdj(kdjv, model.MONTH, kdjhist, bymo, slmo) * SCORE_KDJV_MONTH

		kdjhist = getd.ToLstJDCross(getd.GetKdjHist(s.Code, model.INDICATOR_WEEK, 100))
		ip.Score += scoreKdj(kdjv, model.WEEK, kdjhist, bywk, slwk) * SCORE_KDJV_WEEK

		kdjhist = getd.ToLstJDCross(getd.GetKdjHist(s.Code, model.INDICATOR_DAY, 100))
		ip.Score += scoreKdj(kdjv, model.DAY, kdjhist, bydy, sldy) * SCORE_KDJV_DAY

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

//Score by assessing the historical data against feature data.
func scoreKdj(v *KdjV, cytp model.CYTP, kdjhist []*model.Indicator, byhist,
slhist map[string][]*model.KDJfd) (s float64) {
	start := time.Now()
	defer logr.Debugf("cycle %s score calculation: %.2f", cytp, time.Since(start).Seconds())
	var val string
	hdr, pdr, mpd, bdi := calcKdjDI(kdjhist, byhist)
	val = fmt.Sprintf("%.2f/%.2f/%.2f/%.2f\n", hdr, pdr, mpd, bdi)
	hdr, pdr, mpd, sdi := calcKdjDI(kdjhist, slhist)
	val += fmt.Sprintf("%.2f/%.2f/%.2f/%.2f\n", hdr, pdr, mpd, sdi)
	dirat := .0
	s = .0
	if sdi == 0 {
		dirat = bdi
	} else {
		dirat = (bdi - sdi) / math.Abs(sdi)
	}
	if dirat > 0 && dirat < 0.995 {
		s = 30 * (0.0015 + 3.3609*dirat - 4.3302*math.Pow(dirat, 2.) + 2.5115*math.Pow(dirat, 3.) -
			0.5449*math.Pow(dirat, 4.))
	} else if dirat >= 0.995 {
		s = 30
	}
	if bdi > 0.201 && bdi < 0.81 {
		s += 70 * (0.0283 - 1.8257*bdi + 10.4231*math.Pow(bdi, 2.) - 10.8682*math.Pow(bdi, 3.) + 3.2234*math.Pow(bdi, 4.))
	} else if bdi >= 0.81 {
		s += 70
	}
	switch cytp {
	case model.DAY:
		v.CCDY = val
	case model.WEEK:
		v.CCWK = val
	case model.MONTH:
		v.CCMO = val
	default:
		log.Panicf("unsupported cytp: %s", cytp)
	}
	return
}

// Evaluates KDJ DEVIA indicator, returns the following result:
// Ratio of high DEVIA, ratio of positive DEVIA, mean of positive DEVIA, and DEVIA indicator, ranging from 0 to 1
func calcKdjDI(hist []*model.Indicator, fdsMap map[string][]*model.KDJfd) (hdr, pdr, mpd, di float64) {
	//TODO refine algorithm
	hk := make([]float64, len(hist))
	hd := make([]float64, len(hist))
	hj := make([]float64, len(hist))
	code := hist[0].Code
	for i, h := range hist {
		hk[i] = h.KDJ_K
		hd[i] = h.KDJ_D
		hj[i] = h.KDJ_J
	}
	pds := make([]float64, 0, 16)
	hdc := .0
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
			mod = math.Max(0.8, -0.0003*math.Pow(days-800, 1.0002)+1)
		}
		k, d, j := extractKdjFd(fd)
		bkd := bestKdjDevi(hk, hd, hj, k, d, j) * mod
		if bkd >= 0 {
			pds = append(pds, bkd)
			if bkd >= 0.8 {
				hdc++
			}
		}
	}
	total := float64(len(fdsMap))
	pdr = float64(len(pds)) / total
	hdr = hdc / total
	var e error
	mpd, e = stats.Mean(pds)
	util.CheckErr(e, code+" failed to calculate mean of devia")
	di = 0.5 * math.Min(1, math.Pow(hdr+0.92, 50))
	di += 0.3 * math.Min(1, math.Pow(math.Log(pdr+1), 0.37)+0.4*math.Pow(pdr, math.Pi)+math.Pow(pdr, 0.476145))
	di += 0.2 * math.Min(1, math.Pow(math.Log(math.Pow(mpd, math.E*math.Pi/1.1)+1), 0.06)+
		math.E/1.25/math.Pi*math.Pow(mpd, math.E*math.Pi))
	return
}

// Calculates the best match KDJ DEVIA, len(sk)==len(sd)==len(sj),
// and len(sk) and len(tk) can vary.
// DEVIA ranges from negative infinite to 1, with 1 indicating the most relevant KDJ data sets.
func bestKdjDevi(sk, sd, sj, tk, td, tj []float64) float64 {
	//should we also consider the len(x) to weigh the final result?
	dif := len(sk) - len(tk)
	if dif > 0 {
		cc := -100.0
		for i := 0; i <= dif; i++ {
			e := len(sk) - dif + i
			tcc := calcKdjDevi(sk[i:e], sd[i:e], sj[i:e], tk, td, tj)
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
			tcc := calcKdjDevi(sk, sd, sj, tk[i:e], td[i:e], tj[i:e])
			if tcc > cc {
				cc = tcc
			}
		}
		return cc
	} else {
		return calcKdjDevi(sk, sd, sj, tk, td, tj)
	}
}

func calcKdjDevi(sk, sd, sj, tk, td, tj []float64) float64 {
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
