package advisor

import (
	"github.com/carusyte/stock/util"
	"reflect"
)

type cols struct {
	Code       string
	Name       string
	Date       string `db:"last_price_date"`
	Close      float64
	Divi       float64
	ReportYear string `db:"report_year"`
	Dps        float64
}

func (a *advisor) HiDivi(firstN int) *Table {
	query, err := a.dotsql.Raw("HiDivi")
	util.CheckErr(err, "failed to fetch query string for HiDivi")
	r, err := a.dbMap.Select(cols{}, query, 25)
	util.CheckErr(err, "failed to execute query")
	return newTable(cols{}, r)
}

func newTable(i interface{}, rows []interface{}) *Table {
	t := reflect.TypeOf(i)
	h := make([]Head, t.NumField())
	for i, _ := range h {
		h[i] = Head{t.Field(i).Name, 0}
	}
	return &Table{h, rows}
}
