package indc

import (
	"github.com/carusyte/stock/model"
)

//MACD calculates MACD indicator for the given parameters
func MACD(src []*model.TradeDataBase, nshort, nlong, m float64) []*model.Indicator {
	r := make([]*model.Indicator, len(src))
	close, diff := .0, .0
	for i, s := range src {
		idc := &model.Indicator{}
		r[i] = idc
		idc.Code = s.Code
		idc.Date = s.Date[:10]
		idc.Klid = s.Klid

		idc.MACD_diff = EMA(s.Close, close, nshort) - EMA(s.Close, close, nlong)
		idc.MACD_dea = EMA(idc.MACD_diff, diff, m)
		idc.MACD = 2. * (idc.MACD_diff - idc.MACD_dea)

		close = s.Close
		diff = idc.MACD_diff
	}
	return r
}

//DeftMACD calculates MACD indicator using default parameters (12,26,9)
func DeftMACD(src []*model.TradeDataBase) []*model.Indicator {
	return MACD(src, 12, 26, 9)
}
