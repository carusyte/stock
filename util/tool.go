package util

import (
	"strconv"
	"database/sql"
	"strings"
)

func Reverse(s []*interface{}){
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func Str2F64(s string) (f float64) {
	f64, e := strconv.ParseFloat(s, 64)
	if e == nil {
		f = f64
	}
	return
}

func Str2F32(s string) (f float32) {
	f32, e := strconv.ParseFloat(s, 32)
	if e == nil {
		f = float32(f32)
	}
	return
}

func Str2Fnull(s string) (f sql.NullFloat64) {
	f64, e := strconv.ParseFloat(s, 64)
	if e == nil {
		f.Float64 = f64
		f.Valid = true
	} else {
		f.Valid = false
	}
	return
}

func Str2Snull(s string)(snull sql.NullString){
	v := strings.TrimSpace(s)
	if  v == ""{
		snull.Valid = false
	}else{
		snull.String = v
		snull.Valid = true
	}
	return
}