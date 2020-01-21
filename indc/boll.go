package indc

import (
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
)

//BOLL calculates Bollinger Bandwidth indicator for the given parameters
func BOLL(src []*model.TradeDataBasic, n, p int) []*model.Indicator {
	r := make([]*model.Indicator, len(src))
	close := make([]float64, len(src))
	for i, s := range src {
		close[i] = s.Close
	}
	for i, s := range src {
		idc := &model.Indicator{}
		r[i] = idc
		mid := MA(close, i, n)
		idc.BOLL_mid = mid
		std := STD(close, i, n)
		pf := float64(p)
		upper := mid + pf*std
		lower := mid - pf*std
		idc.BOLL_upper = upper
		idc.BOLL_lower = lower

		//calculates LR for OHLC
		bias := 0.01
		idc.BOLL_lower_o = util.LogReturn(lower, s.Open, bias)
		idc.BOLL_lower_h = util.LogReturn(lower, s.High, bias)
		idc.BOLL_lower_l = util.LogReturn(lower, s.Low, bias)
		idc.BOLL_lower_c = util.LogReturn(lower, s.Close, bias)
		idc.BOLL_mid_o = util.LogReturn(mid, s.Open, bias)
		idc.BOLL_mid_h = util.LogReturn(mid, s.High, bias)
		idc.BOLL_mid_l = util.LogReturn(mid, s.Low, bias)
		idc.BOLL_mid_c = util.LogReturn(mid, s.Close, bias)
		idc.BOLL_upper_o = util.LogReturn(upper, s.Open, bias)
		idc.BOLL_upper_h = util.LogReturn(upper, s.High, bias)
		idc.BOLL_upper_l = util.LogReturn(upper, s.Low, bias)
		idc.BOLL_upper_c = util.LogReturn(upper, s.Close, bias)
	}
	return r
}

//DeftBOLL calculates BOLL indicator using default parameters (20,2)
func DeftBOLL(src []*model.TradeDataBasic) []*model.Indicator {
	return BOLL(src, 20, 2)
}
