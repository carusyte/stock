//
// Calculates week and month kline based on daily kline data.
//
package main

import (
	"flag"
	"fmt"
	"github.com/carusyte/stock/db"
	"github.com/carusyte/stock/indc"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"github.com/gchaincl/dotsql"
	"github.com/ziutek/mymysql/mysql"
	"gopkg.in/gorp.v2"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

const APP_VERSION = "0.1"
const PARALLEL = 256

// The flag package provides a default help printer via -h switch
var versionFlag *bool = flag.Bool("v", false, "Print the version number.")
var dot *dotsql.DotSql

func init() {
	logFile, err := os.OpenFile("calk.log", os.O_CREATE|os.O_RDWR, 0666)
	util.CheckErr(err, "failed to open log file")
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)
}

//TODO refactor to run in single process?
func main() {
	start := time.Now()
	defer func() {
		log.Printf("Time Elapsed: %f sec", time.Since(start).Seconds())
	}()

	flag.Parse() // Scan the arguments list

	if *versionFlag {
		fmt.Println("Version:", APP_VERSION)
	}

	var err error
	// initialize the DbMap
	dot, err = dotsql.LoadFromFile("/Users/jx/ProgramData/go/src/github.com/carusyte/stock/ask/sql.txt")
	util.CheckErr(err, "failed to init dotsql")
	dbmap := db.Get(true, false)

	cal(dbmap)
}

func getStocks(dbmap *gorp.DbMap) []model.Stock {
	var stocks []model.Stock
	_, err := dbmap.Select(&stocks, "select * from basics order by code")
	checkErr(err, "Select failed")
	log.Printf("number of stock: %d\n", len(stocks))
	return stocks
}

func checkErr(err error, msg string) {
	if err != nil {
		log.Fatalf("%s\n %+v\n", msg, err)
	}
}

func cal(dbmap *gorp.DbMap) {
	purgeOld(dbmap)
	stocks := getStocks(dbmap)

	var wg sync.WaitGroup
	log.Printf("stocks to process: %d", len(stocks))
	wg.Add(len(stocks))
	sem := make(chan bool, PARALLEL)

	for i, s := range stocks {
		log.Printf("ready to process [%d]%s", i, s.Code)
		sem <- true
		go caljob(wg, s, dbmap, sem)
		//time.Sleep(1 * time.Second)
	}

	wg.Wait()
	close(sem)
	log.Println("Finished processing")
}

func supplementKlid(code string) {
	log.Printf("supplementing klid for kline_d...")
	supKlid, err := dot.Raw("supKlid")
	supKlid = strings.Replace(supKlid, "?", fmt.Sprintf("'%s'", code), 1)
	checkErr(err, "failed to get supKlid query")
	mysql := db.GetMySql()
	defer func() {
		e := mysql.Release()
		util.CheckErrNop(e, code+" failed to release mysql connection")
	}()
	res, err := mysql.Start(supKlid)
	checkErr(err, "failed to supplement klid")
	readResults(res)
	log.Printf("klid supplemented for %s", code)
}
func readResults(result mysql.Result) {
	result, err := result.NextResult()
	checkErr(err, "failed to get result")
	if result != nil {
		for {
			row, err := result.GetRow()
			util.CheckErr(err, "failed to get row")
			if row == nil {
				break
			}
		}
		readResults(result)
	}
}

func caljob(wg sync.WaitGroup, s model.Stock, dbmap *gorp.DbMap, sem chan bool) {
	start := time.Now()
	defer func() {
		wg.Done()
		<-sem
	}()
	log.Println("Calculating week kline for " + s.Code)
	supplementKlid(s.Code)
	klines, mxw, mxm := getKlines(s, dbmap)
	q := make([]*model.Quote, len(klines))
	var qw []*model.Quote
	var qm []*model.Quote
	var klinesw []*model.KlineW
	var klinesm []*model.KlineM

	klw := newKlinew()
	klm := newKlinem()
	var lastWeekDay, lastMonth int = 7, 0
	var klid_w, klid_m int = 0, 0
	if mxw != nil {
		klid_w = mxw.Klid + 1
	}
	if mxm != nil {
		klid_m = mxm.Klid + 1
	}
	for i, k := range klines {
		q[i] = &k.Quote
		t, err := time.Parse("2006-01-02T00:00:00-07:00", k.Date)
		checkErr(err, "failed to parse date from kline_d "+k.Date)
		tw, err := time.Parse("2006-01-02", klw.Date)
		checkErr(err, "failed to parse date in KlineW "+klw.Date)

		if (int(t.Weekday()) <= lastWeekDay || t.Add(-1 * time.Duration(7) * time.Hour * 24).After(tw)) &&
			((mxw != nil && k.Date[:10] > mxw.Date) || mxw == nil) {

			klw = newKlinew()
			klinesw = append(klinesw, klw)
			klw.Code = k.Code

			klw.Klid = klid_w
			klid_w++

			klw.Open = k.Open
			klw.Low = k.Low
			lastWeekDay = int(t.Weekday())

			qw = append(qw, &klw.Quote)
		}

		if int(t.Month()) != lastMonth && ((mxm != nil && k.Date[:10] > mxm.Date) || mxm == nil) {
			klm = newKlinem()
			klinesm = append(klinesm, klm)
			klm.Code = k.Code

			klm.Klid = klid_m
			klid_m++

			klm.Open = k.Open
			klm.Low = k.Low
			lastMonth = int(t.Month())

			qm = append(qm, &klm.Quote)
		}

		klw.Date, klm.Date = k.Date[:10], k.Date[:10]

		klw.Amount += k.Amount
		klm.Amount += k.Amount
		klw.Volume += k.Volume
		klm.Volume += k.Volume
		if klw.High < k.High {
			klw.High = k.High
		}
		if klm.High < k.High {
			klm.High = k.High
		}
		if klw.Low > k.Low {
			klw.Low = k.Low
		}
		if klm.Low > k.Low {
			klm.Low = k.Low
		}
		klw.Close, klm.Close = k.Close, k.Close
	}

	mxid, mxiw, mxim := getMaxIdcDates(dbmap, s.Code)

	kdj := subidc(indc.DeftKDJ(q), mxid)
	kdjw := subidcw(indc.DeftKDJ_W(qw), mxiw)
	kdjm := subidcm(indc.DeftKDJ_M(qm), mxim)
	batchInsert(dbmap, klinesw, klinesm, kdj, kdjw, kdjm)

	log.Printf("Complete in %f s: %s, dy: %d, wk: %d, mo: %d\n", time.Since(start).Seconds(),
		s.Code, len(klines), len(klinesw), len(klinesm))
}

func getMaxIdcDates(dbmap *gorp.DbMap, code string) (mxid, mxiw, mxim int) {
	mxid, mxiw, mxim = -1, -1, -1
	mxidn, err := dbmap.SelectNullInt("select max(klid) from indicator_d where code=?", code)
	checkErr(err, "failed to query max klid in indicator_d for "+code)
	mxiwn, err := dbmap.SelectNullInt("select max(klid) from indicator_w where code=?", code)
	checkErr(err, "failed to query max klid in indicator_w for "+code)
	mximn, err := dbmap.SelectNullInt("select max(klid) from indicator_m where code=?", code)
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
	return
}

func subidc(q []*model.Indicator, klid int) (ret []*model.Indicator) {
	ret = make([]*model.Indicator, 0)
	for i, qe := range q {
		if qe.Klid > klid {
			ret = q[i:]
			return
		}
	}
	return
}

func subidcw(q []*model.IndicatorW, klid int) (ret []*model.IndicatorW) {
	ret = make([]*model.IndicatorW, 0)
	for i, qe := range q {
		if qe.Klid > klid {
			ret = q[i:]
			return
		}
	}
	return
}

func subidcm(q []*model.IndicatorM, klid int) (ret []*model.IndicatorM) {
	ret = make([]*model.IndicatorM, 0)
	for i, qe := range q {
		if qe.Klid > klid {
			ret = q[i:]
			return
		}
	}
	return
}

// Fetch all klines, latest kline_w and kline_m. Nil will be return if there's no such record.
func getKlines(s model.Stock, dbmap *gorp.DbMap) ([]*model.Kline, *model.KlineW, *model.KlineM) {
	mxw, mxm := getMaxDates(s.Code, dbmap)
	var klines []*model.Kline
	_, err := dbmap.Select(&klines, "select * from kline_d where code = ? order by date", s.Code)
	checkErr(err, "Failed to query kline_d for "+s.Code)
	log.Printf("%s, kline(s) of day: %d", s.Code, len(klines))
	return klines, mxw, mxm
}

func getMaxDates(stock string, dbMap *gorp.DbMap) (daw *model.KlineW, dam *model.KlineM) {
	e := dbMap.SelectOne(&daw, "select * from kline_w where code = ? order by date desc limit 1", stock)
	util.CheckErrNop(e, "")
	e = dbMap.SelectOne(&dam, "select * from kline_m where code = ? order by date desc limit 1", stock)
	util.CheckErrNop(e, "")
	return
}

func purgeOld(dbmap *gorp.DbMap) {
	lastNTD, err := dot.Raw("lastNTD")
	checkErr(err, "failed to fetch lastNTD from sql file")
	lst7, err := dbmap.SelectStr(lastNTD, 7)
	checkErr(err, "failed to query last 7 trade date")
	_, err = dbmap.Exec("delete from kline_w where date >= ?", lst7)
	checkErr(err, "failed to purge kline_w")
	_, err = dbmap.Exec("delete from indicator_d where date >= ?", lst7)
	checkErr(err, "failed to purge indicator_d")
	_, err = dbmap.Exec("delete from indicator_w where date >= ?", lst7)
	checkErr(err, "failed to purge indicator_w")

	lstm, err := dbmap.SelectStr(lastNTD, 32)
	checkErr(err, "failed to query last 32 trade date")
	_, err = dbmap.Exec("delete from kline_m where date >= ?", lstm)
	checkErr(err, "failed to purge kline_m")
	_, err = dbmap.Exec("delete from indicator_m where date >= ?", lstm)
	checkErr(err, "failed to purge indicator_m")
}

func newKlinew() *model.KlineW {
	klw := &model.KlineW{}
	klw.Klid = -1
	klw.Date = "1900-01-01"
	return klw
}

func newKlinem() *model.KlineM {
	klm := &model.KlineM{}
	klm.Klid = -1
	klm.Date = "1900-01-01"
	return klm
}

func dotSql() *dotsql.DotSql {
	if dot == nil {
		var err error
		dot, err = dotsql.LoadFromFile("/Users/jx/ProgramData/go/src/github.com/carusyte/stock/ask/sql.txt")
		util.CheckErr(err, "failed to init dotsql")
	}
	return dot
}

func batchInsert(dbmap *gorp.DbMap, klinesw []*model.KlineW, klinesm []*model.KlineM, indc []*model.Indicator, indcw []*model.IndicatorW, indcm []*model.IndicatorM) {
	binsKlw(dbmap, klinesw)
	binsKlm(dbmap, klinesm)
	binsIndc(dbmap, indc)
	binsIndcw(dbmap, indcw)
	binsIndcm(dbmap, indcm)
}

func binsIndcm(db *gorp.DbMap, indcm []*model.IndicatorM) {
	if len(indcm) > 0 {
		valueStrings := make([]string, 0, len(indcm))
		valueArgs := make([]interface{}, 0, len(indcm)*6)
		var code string
		for _, i := range indcm {
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, i.Code)
			valueArgs = append(valueArgs, i.Date)
			valueArgs = append(valueArgs, i.Klid)
			valueArgs = append(valueArgs, i.KDJ_K)
			valueArgs = append(valueArgs, i.KDJ_D)
			valueArgs = append(valueArgs, i.KDJ_J)
			code = i.Code
		}
		stmt := fmt.Sprintf("INSERT INTO indicator_m (code,date,klid,kdj_k,kdj_d,kdj_j) VALUES %s",
			strings.Join(valueStrings, ","))
		_, err := db.Exec(stmt, valueArgs...)
		if !util.CheckErr(err, code+" failed to bulk insert indicator_m") {
			log.Printf("%s: %d records saved to indicator_m", code, len(indcm))
		}
	}
}
func binsIndcw(db *gorp.DbMap, indcw []*model.IndicatorW) {
	if len(indcw) > 0 {
		valueStrings := make([]string, 0, len(indcw))
		valueArgs := make([]interface{}, 0, len(indcw)*6)
		var code string
		for _, i := range indcw {
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, i.Code)
			valueArgs = append(valueArgs, i.Date)
			valueArgs = append(valueArgs, i.Klid)
			valueArgs = append(valueArgs, i.KDJ_K)
			valueArgs = append(valueArgs, i.KDJ_D)
			valueArgs = append(valueArgs, i.KDJ_J)
			code = i.Code
		}
		stmt := fmt.Sprintf("INSERT INTO indicator_w (code,date,klid,kdj_k,kdj_d,kdj_j) VALUES %s",
			strings.Join(valueStrings, ","))
		_, err := db.Exec(stmt, valueArgs...)
		if !util.CheckErr(err, code+" failed to bulk insert indicator_w") {
			log.Printf("%s: %d records saved to indicator_w", code, len(indcw))
		}
	}
}
func binsIndc(db *gorp.DbMap, indc []*model.Indicator) {
	if len(indc) > 0 {
		valueStrings := make([]string, 0, len(indc))
		valueArgs := make([]interface{}, 0, len(indc)*6)
		var code string
		for _, i := range indc {
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, i.Code)
			valueArgs = append(valueArgs, i.Date)
			valueArgs = append(valueArgs, i.Klid)
			valueArgs = append(valueArgs, i.KDJ_K)
			valueArgs = append(valueArgs, i.KDJ_D)
			valueArgs = append(valueArgs, i.KDJ_J)
			code = i.Code
		}
		stmt := fmt.Sprintf("INSERT INTO indicator_d (code,date,klid,kdj_k,kdj_d,kdj_j) VALUES %s",
			strings.Join(valueStrings, ","))
		ps, err := db.Prepare(stmt)
		util.CheckErrNop(err, code+" failed to prepare statement")
		_, err = ps.Exec(valueArgs...)
		if !util.CheckErr(err, code+" failed to bulk insert indicator_d") {
			log.Printf("%s: %d records saved to indicator_d", code, len(indc))
		}
	}
}
func binsKlm(db *gorp.DbMap, klinesm []*model.KlineM) {
	if len(klinesm) > 0 {
		valueStrings := make([]string, 0, len(klinesm))
		valueArgs := make([]interface{}, 0, len(klinesm)*9)
		var code string
		for _, klm := range klinesm {
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, klm.Code)
			valueArgs = append(valueArgs, klm.Date)
			valueArgs = append(valueArgs, klm.Klid)
			valueArgs = append(valueArgs, klm.Open)
			valueArgs = append(valueArgs, klm.High)
			valueArgs = append(valueArgs, klm.Close)
			valueArgs = append(valueArgs, klm.Low)
			valueArgs = append(valueArgs, klm.Volume)
			valueArgs = append(valueArgs, klm.Amount)
			code = klm.Code
		}
		stmt := fmt.Sprintf("INSERT INTO kline_m (code,date,klid,open,high,close,low,"+
			"volume,amount) VALUES %s", strings.Join(valueStrings, ","))
		_, err := db.Exec(stmt, valueArgs...)
		if !util.CheckErr(err, code+" failed to bulk insert kline_m") {
			log.Printf("%s: %d records saved to kline_m", code, len(klinesm))
		}
	}
}

func binsKlw(db *gorp.DbMap, klws []*model.KlineW) {
	if len(klws) > 0 {
		valueStrings := make([]string, 0, len(klws))
		valueArgs := make([]interface{}, 0, len(klws)*9)
		var code string
		for _, klw := range klws {
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, klw.Code)
			valueArgs = append(valueArgs, klw.Date)
			valueArgs = append(valueArgs, klw.Klid)
			valueArgs = append(valueArgs, klw.Open)
			valueArgs = append(valueArgs, klw.High)
			valueArgs = append(valueArgs, klw.Close)
			valueArgs = append(valueArgs, klw.Low)
			valueArgs = append(valueArgs, klw.Volume)
			valueArgs = append(valueArgs, klw.Amount)
			code = klw.Code
		}
		stmt := fmt.Sprintf("INSERT INTO kline_w (code,date,klid,open,high,close,low,"+
			"volume,amount) VALUES %s", strings.Join(valueStrings, ","))
		_, err := db.Exec(stmt, valueArgs...)
		if !util.CheckErr(err, code+" failed to bulk insert kline_w") {
			log.Printf("%s: %d records saved to kline_w", code, len(klws))
		}
	}
}
