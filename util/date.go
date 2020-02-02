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

//TimeStr returns date and time in project standard formats respectively.
func TimeStr() (d, t string) {
	now := time.Now()
	d = now.Format(global.DateFormat)
	t = now.Format(global.TimeFormat)
	return
}

//DaysSince returns how many days since the provided date.
func DaysSince(then string) (float64, error) {
	t, err := time.Parse(global.DateFormat, then)
	if err != nil {
		return 0, err
	}
	return time.Since(t).Hours() / 24.0, nil
}

//UnixMilliseconds returns milliseconds since Unix Ephoch.
func UnixMilliseconds(t time.Time) int64 {
	ms := float64(t.UnixNano()) * float64(time.Nanosecond) / float64(time.Millisecond)
	return int64(ms)
}

//ConvTimeUnit converts time value from one unit to another unit
func ConvTimeUnit(timeVal float64, fromUnit, toUnit time.Duration) float64 {
	return timeVal * float64(fromUnit) / float64(toUnit)
}
