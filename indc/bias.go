package indc

import (
	"math"

	"github.com/carusyte/stock/model"
)

//BIAS calculates BIAS Indicator for the given parameters
func BIAS(src []*model.TradeDataBasic, n1, n2, n3 int) []*model.Indicator {
	r := make([]*model.Indicator, len(src))
	ma1 := make([]float64, len(src))
	ma2 := make([]float64, len(src))
	ma3 := make([]float64, len(src))
	for i := range src {
		ma1[i] = calcMA(src, i, n1)
		ma2[i] = calcMA(src, i, n2)
		ma3[i] = calcMA(src, i, n3)
	}
	for i, s := range src {
		idc := &model.Indicator{}
		r[i] = idc
		d1 := ma1[i]
		d2 := ma2[i]
		d3 := ma3[i]
		if d1 == 0 {
			d1 = 0.01
		}
		if d2 == 0 {
			d2 = 0.01
		}
		if d3 == 0 {
			d3 = 0.01
		}
		idc.BIAS1 = (s.Close - ma1[i]) / d1 * 100.
		idc.BIAS2 = (s.Close - ma2[i]) / d2 * 100.
		idc.BIAS3 = (s.Close - ma3[i]) / d3 * 100.
	}
	return r
}

func calcMA(src []*model.TradeDataBasic, curIdx, n int) float64 {
	if curIdx >= len(src) {
		log.Panicf("invalid curIdx:%d, maximum:%d", curIdx, len(src)-1)
	}
	nu := 0.
	for i := int(math.Max(0, float64(curIdx-n+1))); i <= curIdx; i++ {
		nu += src[i].Close
	}
	return nu / float64(n)
}

//DeftBIAS calculates BIAS indicator using default parameters (6,12,24)
func DeftBIAS(src []*model.TradeDataBasic) []*model.Indicator {
	return BIAS(src, 6, 12, 24)
}
