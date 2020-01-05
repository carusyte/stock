package indc

import (
	"math"

	"github.com/carusyte/stock/model"
)

//RSI calculates Relative Strength Indicator for the given parameters
func RSI(src []*model.TradeDataBase, n1, n2, n3 int) []*model.Indicator {
	r := make([]*model.Indicator, len(src))
	nums := make([]float64, len(src))
	dens := make([]float64, len(src))
	pc := .0
	for i, s := range src {
		d := s.Close - pc
		nums[i] = math.Max(0, d)
		dens[i] = math.Abs(d)
		pc = s.Close
	}
	nums1 := SMA(nums, n1, 1)
	nums2 := SMA(nums, n2, 1)
	nums3 := SMA(nums, n3, 1)
	dens1 := SMA(dens, n1, 1)
	dens2 := SMA(dens, n2, 1)
	dens3 := SMA(dens, n3, 1)
	for i := range r {
		idc := &model.Indicator{}
		r[i] = idc
		d1 := dens1[i]
		d2 := dens2[i]
		d3 := dens3[i]
		if d1 == 0 {
			d1 = 0.01
		}
		if d2 == 0 {
			d2 = 0.01
		}
		if d3 == 0 {
			d3 = 0.01
		}
		idc.RSI1 = math.Min(100., nums1[i]/d1*100.)
		idc.RSI2 = math.Min(100., nums2[i]/d2*100.)
		idc.RSI3 = math.Min(100., nums3[i]/d3*100.)
	}
	return r
}

//DeftRSI calculates RSI indicator using default parameters (6,12,24)
func DeftRSI(src []*model.TradeDataBase) []*model.Indicator {
	return RSI(src, 6, 12, 24)
}
