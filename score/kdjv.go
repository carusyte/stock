package score

import (
	"github.com/carusyte/stock/model"
	"math"
	"github.com/carusyte/stock/getd"
	"fmt"
	"github.com/carusyte/stock/util"
	"log"
	"github.com/montanaflynn/stats"
	"strings"
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

//Score by assessing the coefficient of recent monthly KDJ and historical data.
func scoreKdjMon(v *KdjV) float64 {
	//TODO make it
	hist := getd.GetKdjHist(v.Code, model.INDICATOR_MONTH, KDJ_RETROSPECT)
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
