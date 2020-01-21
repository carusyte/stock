package indc

import (
	"math"
	"reflect"

	"github.com/montanaflynn/stats"

	"github.com/carusyte/stock/model"
)

//MA calculates moving average for given values.
func MA(vals []float64, curIdx, n int) float64 {
	if curIdx >= len(vals) {
		log.Panicf("invalid curIdx:%d, maximum:%d", curIdx, len(vals)-1)
	}
	nu := 0.
	for i := int(math.Max(0, float64(curIdx-n+1))); i <= curIdx; i++ {
		nu += vals[i]
	}
	return nu / float64(n)
}

//STD calculates standard deviation for given values.
func STD(vals []float64, curIdx, n int) float64 {
	if curIdx >= len(vals) {
		log.Panicf("invalid curIdx:%d, maximum:%d", curIdx, len(vals)-1)
	}
	bg := int(math.Max(0, float64(curIdx-n+1)))
	std, e := stats.StandardDeviation(vals[bg : curIdx+1])
	if e != nil {
		log.Panicf("failed to calculate standard deviation: %+v", e)
	}
	return std
}

//SMA calculates Simple Moving Average for given values.
//formula: Y = [M * X + (N-M) * Y'] / N
func SMA(src []float64, n, m int) []float64 {
	r := make([]float64, len(src))
	for x, i := range src {
		fm := float64(m)
		fn := float64(n)
		if x == 0 {
			r[x] = fm * i / fn
		} else {
			r[x] = (fm*i + (fn-fm)*r[x-1]) / fn
		}
		if math.IsNaN(r[x]) {
			log.Printf("NaN detected in SMA, x[%d], i[%f], m[%d], n[%d], %+v", x, i, m, n, src)
			panic(src)
		}
	}
	return r
}

//EMA calculates exponential moving average for given values.
//formula: Y = [2*X + (N-1)*Y']/(N+1)
func EMA(x, pre, n float64) float64 {
	return (2.*x + (n-1.)*pre) / (n + 1.)
}

//LLV returns lowest value of given field
func LLV(src []*model.TradeDataBasic, field string) float64 {
	var t reflect.Value
	for i, s := range src {
		r := reflect.ValueOf(s)
		f := reflect.Indirect(r).FieldByName(field)
		if i == 0 {
			t = f
		} else {
			switch f.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				if f.Int() < t.Int() {
					t = f
				}
			case reflect.Float32, reflect.Float64:
				if f.Float() < t.Float() {
					t = f
				}
			}
		}
	}
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(t.Int())
	case reflect.Float32, reflect.Float64:
		return t.Float()
	default:
		return t.Float()
	}
}

//HHV returns highest value of given field
func HHV(src []*model.TradeDataBasic, field string) float64 {
	var t reflect.Value
	for i, s := range src {
		r := reflect.ValueOf(s)
		f := reflect.Indirect(r).FieldByName(field)
		if i == 0 {
			t = f
		} else {
			switch f.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				if f.Int() > t.Int() {
					t = f
				}
			case reflect.Float32, reflect.Float64:
				if f.Float() > t.Float() {
					t = f
				}
			}
		}
	}
	return t.Float()
}
