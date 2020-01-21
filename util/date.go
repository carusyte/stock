package util

import (
	"fmt"
	"time"

	"github.com/carusyte/stock/global"
)

//SameWeek checks if two dates denoted by respective strings are in the same week.
// if arg format is not supplied, default format "2006-01-02" will be used.
func SameWeek(d1, d2, format string) (yes bool, e error) {
	dfmt := global.DateFormat
	if format != "" {
		dfmt = format
	}
	t1, e := time.Parse(dfmt, d1)
	if e != nil {
		return false, fmt.Errorf("unable to parse date from string %s using format %s: %+v", d1, dfmt, e)
	}
	t2, e := time.Parse(dfmt, d2)
	if e != nil {
		return false, fmt.Errorf("unable to parse date from string %s using format %s: %+v", d2, dfmt, e)
	}
	yt1, wt1 := t1.ISOWeek()
	yt2, wt2 := t2.ISOWeek()
	return yt1 == yt2 && wt1 == wt2, nil
}
