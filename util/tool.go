package util

import (
	"strconv"
	"database/sql"
)

func Reverse(s []*interface{}){
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func Str2f64(s string) (f float64) {
	f64, e := strconv.ParseFloat(s, 64)
	if e == nil {
		f = f64
	}
	return
}

func Str2f32(s string) (f float32) {
	f32, e := strconv.ParseFloat(s, 32)
	if e == nil {
		f = float32(f32)
	}
	return
}

func Str2fnull(s string) (f sql.NullFloat64) {
	f64, e := strconv.ParseFloat(s, 64)
	if e == nil {
		f.Float64 = f64
		f.Valid = true
	} else {
		f.Valid = false
	}
	return
}