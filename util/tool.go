package util

import (
	"database/sql"
	"strconv"
	"strings"
	"time"
	"fmt"
	"math"
)

func Reverse(s []*interface{}) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func Str2F64(s string) (f float64) {
	f64, e := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if e == nil {
		f = f64
	}
	return
}

func Pct2Fnull(s string) (f sql.NullFloat64) {
	f = Str2Fnull(strings.TrimSuffix(strings.TrimSpace(s), "%"))
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
	f64, e := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if e == nil {
		f.Float64 = f64
		f.Valid = true
	} else {
		f.Valid = false
	}
	return
}

func Str2Inull(s string) (i sql.NullInt64) {
	i64, e := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if e == nil {
		i.Int64 = i64
		i.Valid = true
	} else {
		i.Valid = false
	}
	return
}

func Str2Snull(s string) (snull sql.NullString) {
	v := strings.TrimSpace(s)
	if v == "" {
		snull.Valid = false
	} else {
		snull.String = v
		snull.Valid = true
	}
	return
}

func Str2FBil(s string) (f sql.NullFloat64) {
	mod := 1.0
	s = strings.TrimSpace(s)
	if strings.HasSuffix(s, `万`) {
		s = strings.TrimSuffix(s, `万`)
		mod = 0.0001
	} else if strings.HasSuffix(s, `亿`) {
		s = strings.TrimSuffix(s, `亿`)
	} else {
		mod = 0.00000001
	}
	f64, e := strconv.ParseFloat(s, 64)
	if e == nil {
		f.Float64 = f64 * mod
		f.Valid = true
	} else {
		f.Valid = false
	}
	return
}

func Str2FBilMod(s string, mod float64) (f sql.NullFloat64) {
	s = strings.TrimSpace(s)
	if strings.HasSuffix(s, `万`) {
		s = strings.TrimSuffix(s, `万`)
		mod = 0.0001
	} else if strings.HasSuffix(s, `亿`) {
		s = strings.TrimSuffix(s, `亿`)
		mod = 1
	}
	f64, e := strconv.ParseFloat(s, 64)
	if e == nil {
		f.Float64 = f64 * mod
		f.Valid = true
	} else {
		f.Valid = false
	}
	return
}

func TimeStr() (d, t string) {
	now := time.Now()
	d = now.Format("2006-01-02")
	t = now.Format("15:04:05")
	return
}

func SprintFa(fa []float64, format, sep string, ls int) string {
	if len(fa) == 0 {
		return ""
	}
	if ls > 0 && ls < len(fa) {
		lns := math.Ceil(float64(len(fa) / ls))
		sas := make([][]string, int(lns))
		for i, f := range fa {
			x := i / ls
			y := i % ls
			if sas[x] == nil {
				sas[x] = make([]string, ls)
			}
			sas[x][y] = fmt.Sprintf(format, f)
		}
		var ret string
		for i, s := range sas {
			ret += strings.Join(s, sep)
			if i < len(sas)-1 {
				ret += "\n"
			}
		}
		return ret
	} else {
		sa := make([]string, len(fa))
		for i, f := range fa {
			sa[i] = fmt.Sprintf(format, f)
		}
		return strings.Join(sa, sep)
	}
}
