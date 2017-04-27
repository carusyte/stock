package advisor

import (
	"github.com/carusyte/stock/util"
	"reflect"
	"gopkg.in/gorp.v2"
)

type cols struct {
	Code       string
	Name       string
	Date       string `db:"last_price_date"`
	Close      float64
	Divi       float64
	Shares     float64
	ReportYear string `db:"report_year"`
	DPS        float64
	PE         float64
	ESP        float64
	BVPS       float64
	PB         float64
	Undp       float64
	REV        float64
	Profit     float64
	GPR        float64
	NPR        float64
	K_D        float64
	K_W        float64
	K_M        float64
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
