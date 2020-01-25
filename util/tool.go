package util

import (
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

func Reverse(s []interface{}) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func ReverseF64s(s []float64, returnNew bool) (rs []float64) {
	if returnNew {
		rs = make([]float64, len(s))
		for i, f := range s {
			rs[len(s)-i-1] = f
		}
	} else {
		for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
			s[i], s[j] = s[j], s[i]
		}
		rs = s
	}
	return
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

//Str2Fnull converts string value to sql.NullFloat64
func Str2Fnull(s string) (f sql.NullFloat64) {
	s = strings.TrimSpace(s)
	mod := 1.
	if strings.HasSuffix(s, `万`) {
		s = strings.TrimSuffix(s, `万`)
		mod = 1e4
	} else if strings.HasSuffix(s, `亿`) {
		s = strings.TrimSuffix(s, `亿`)
		mod = 1e8
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

func Join(ss []string, sep string, quote bool) string {
	if quote {
		rs := ""
		for i, s := range ss {
			rs += fmt.Sprintf("'%s'", s)
			if i < len(ss)-1 {
				rs += sep
			}
		}
		return rs
	} else {
		return strings.Join(ss, sep)
	}
}

func Devi(a, b []float64) (float64, error) {
	if len(a) != len(b) || len(a) == 0 {
		return 0, errors.New("invalid input")
	}
	s := .0
	for i := 0; i < len(a); i++ {
		s += math.Pow(a[i]-b[i], 2)
	}
	return math.Pow(s/float64(len(a)), 0.5), nil
}

func DiffStrings(str1 []string, str2 []string) (equal bool, dif1, dif2 []string) {
	m1 := make(map[string]bool)
	m2 := make(map[string]bool)
	equal = true
	for _, s := range str1 {
		m1[s] = true
	}
	for _, s := range str2 {
		m2[s] = true
		if _, exists := m1[s]; !exists {
			equal = false
			dif2 = append(dif2, s)
		}
	}
	for _, s := range str1 {
		if _, exists := m2[s]; !exists {
			equal = false
			dif1 = append(dif1, s)
		}
	}
	return
}

//Str2IntfSlice converts string slice to slice of interface{}
func Str2IntfSlice(ss []string) (intfs []interface{}) {
	for _, s := range ss {
		intfs = append(intfs, s)
	}
	return
}
