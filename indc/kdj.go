package indc

import (
	"github.com/carusyte/stock/model"
	"math"
)

func KDJ(src []*model.Quote, n, m1, m2 int) []*model.Indicator {
	r := make([]*model.Indicator, len(src))
	rsv := make([]float64, len(src))
	for i, s := range src {
		r[i] = &model.Indicator{}
		r[i].Code = s.Code
		r[i].Date = s.Date[:10]
		r[i].Klid = s.Klid
		bg := int(math.Max(float64(i-n+1), 0))
		llv := LLV(src[bg:i+1], "Low")
		hhv := HHV(src[bg:i+1], "High")
		if llv != hhv {
			rsv[i] = (s.Close - llv) / (hhv - llv) * 100
		}else{
			rsv[i] = 1
		}
	}
	a := SMA(rsv, m1, 1)
	b := SMA(a, m2, 1)
	for i := 0; i < len(src); i++ {
		r[i].KDJ_K = a[i]
		r[i].KDJ_D = b[i]
		r[i].KDJ_J = 3*a[i] - 2*b[i]
	}
	return r
}

func DeftKDJ(src []*model.Quote) []*model.Indicator {
	return KDJ(src, 9, 3, 3)
}

func DeftKDJ_W(src []*model.Quote) []*model.IndicatorW {
	kdj := DeftKDJ(src)
	r := make([]*model.IndicatorW, len(kdj))
	for i := range r {
		r[i] = &model.IndicatorW{*kdj[i]}
	}
	return r
}

func DeftKDJ_M(src []*model.Quote) []*model.IndicatorM {
	kdj := DeftKDJ(src)
	r := make([]*model.IndicatorM, len(kdj))
	for i := range r {
		r[i] = &model.IndicatorM{*kdj[i]}
	}
	return r
}
