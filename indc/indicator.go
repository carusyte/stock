package indc

import (
	"github.com/carusyte/stock/model"
	"log"
	"math"
	"reflect"
)

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

func LLV(src []*model.Quote, field string) float64 {
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

func HHV(src []*model.Quote, field string) float64 {
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
