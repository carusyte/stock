package main

import (
	"fmt"
	"github.com/carusyte/stock/db"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"github.com/gchaincl/dotsql"
	"gopkg.in/gorp.v2"
	"strings"
	"sync"
	"testing"
)

var dbMap *gorp.DbMap

func init() {
	var err error
	dot, err = dotsql.LoadFromFile("/Users/jx/ProgramData/go/src/github.com/carusyte/stock/ask/sql.txt")
	util.CheckErr(err, "failed to init dotsql")
	dbMap = db.Get(false, false)
}

func TestGorpSelect(t *testing.T) {
	var daw model.KlineW
	daws, err := dbMap.Select(&daw, "select * from kline_w where code = ? order by date desc limit 1", "123123")
	if err != nil {
		t.Error(err)
	}
	log.Printf("%v", daw)
	log.Printf("%v", len(daws))
	log.Printf("%v", daws)

	if daws == nil {
		log.Printf("daws is nil")
	}

	if len(daws) == 0 {
		log.Printf("length is 0")
	}

	_, err = dbMap.Select(&daw, "select * from kline_w where code = ? order by date desc limit 1", "600104")
	log.Printf("%v", daw)
}

func TestGorpSelectStr(t *testing.T) {
	str, e := dbMap.SelectStr("select code from kline_w where code = ?", "123123")
	if e != nil {
		t.Error(e)
	}
	if str == "" {
		log.Println("str is empty")
	}
	log.Printf("%s", str)
}

func TestSelectNullInt(t *testing.T) {
	mxid, mxiw, mxim := -1, -1, -1
	code := "603988"
	mxidn, err := dbMap.SelectNullInt("select max(klid) from indicator_d where code=?", code)
	checkErr(err, "failed to query max klid in indicator_d for "+code)
	mxiwn, err := dbMap.SelectNullInt("select max(klid) from indicator_w where code=?", code)
	checkErr(err, "failed to query max klid in indicator_w for "+code)
	mximn, err := dbMap.SelectNullInt("select max(klid) from indicator_m where code=?", code)
	checkErr(err, "failed to query max klid in indicator_m for "+code)
	if mxidn.Valid {
		mxid = int(mxidn.Int64)
	}
	if mxiwn.Valid {
		mxiw = int(mxiwn.Int64)
	}
	if mximn.Valid {
		mxim = int(mximn.Int64)
	}
	log.Printf("%d, %d, %d", mxid, mxiw, mxim)
}

func TestGorpSelectOne(t *testing.T) {
	var k *model.KlineW
	e := dbMap.SelectOne(&k, "select * from kline_w where code = ? limit 1", "610104")
	// k will be nil if no rows in result set
	log.Printf("%+v", k)
	log.Printf("%v", k == nil)
	log.Printf("%+v", e)
	daw, dam := getMaxDates("600104")
	log.Printf("%+v, %+v", daw, dam)
}

func TestGorpSupplementKlid(t *testing.T) {
	supplementKlid("600105")
}

// func TestMySqlMultiStatement(t *testing.T) {
// 	// mdb := db.GetMySql()
// 	res, err := mdb.Start("set @i:=123;select @i")
// 	util.CheckErr(err, "failed to query using multi statements")
// 	res, err = res.NextResult()
// 	util.CheckErr(err, "failed to get result")
// 	if res == nil {
// 		panic("Hmm, there is no second result. Why?!")
// 	}
// 	for {
// 		row, err := res.GetRow()
// 		util.CheckErr(err, "failed to get row")
// 		if row == nil {
// 			log.Fatalln("no rows?")
// 		} else {
// 			log.Printf("%+v", row.Str(0))
// 		}
// 	}
// }

func TestGetKlines(t *testing.T) {
	s := &model.Stock{}
	dbMap.SelectOne(s, "select * from basics where code = '000090'")
	getKlines(*s)
}

func TestStock(t *testing.T) {
	var s model.Stock
	dbMap.SelectOne(&s, "select * from basics where code = '000021'")
	var wg sync.WaitGroup
	wg.Add(1)
	sem := make(chan bool, 1)
	sem <- true
	caljob(&wg, s)
}

func TestGroupConcat(t *testing.T) {
	var q []*model.Quote
	_, e := dbMap.Select(&q, `select code, date, klid, high,low,close,open,xrate,volume,amount from kline_d where code = '600104' and date >= '2016' order by date asc`)
	checkErr(e, "failed")
	var h, l, c []float64
	for _, qe := range q {
		h = append(h, qe.High)
		l = append(l, qe.Low)
		c = append(c, qe.Close)
	}

	log.Printf("h:%+v", h)
	log.Printf("l:%+v", l)
	log.Printf("c:%+v", c)

	log.Printf("high: %s", strings.Trim(strings.Replace(fmt.Sprint(h), " ", ",", -1), "[]"))
	log.Printf("low: %s", strings.Trim(strings.Replace(fmt.Sprint(l), " ", ",", -1), "[]"))
	log.Printf("close: %s", strings.Trim(strings.Replace(fmt.Sprint(c), " ", ",", -1), "[]"))
}
