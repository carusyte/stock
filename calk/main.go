//
// Calculates week and month kline based on daily kline data.
//
package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/gorp.v2"
	"log"
	"os"
	"sync"
	"time"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/indc"
)

const APP_VERSION = "0.1"

// The flag package provides a default help printer via -h switch
var versionFlag *bool = flag.Bool("v", false, "Print the version number.")

func main() {
	start := time.Now()
	flag.Parse() // Scan the arguments list

	if *versionFlag {
		fmt.Println("Version:", APP_VERSION)
	}

	// initialize the DbMap
	dbmap := initDb(true)

	cal(dbmap)

	dbmap.Db.Close()
	log.Printf("Time Elapsed: %f sec", time.Since(start).Seconds())
}

func getStocks(dbmap *gorp.DbMap) []model.Stock {
	var stocks []model.Stock
	_, err := dbmap.Select(&stocks, "select * from basics order by code")
	checkErr(err, "Select failed")
	log.Printf("number of stock: %d\n", len(stocks))
	return stocks
}

func initDb(full bool) *gorp.DbMap {
	// connect to db using standard Go database/sql API
	// use whatever database/sql driver you wish
	db, err := sql.Open("mysql", "mysql:123456@/secu")
	checkErr(err, "sql.Open failed,")

	db.SetMaxOpenConns(64)

	// construct a gorp DbMap
	dbmap := &gorp.DbMap{Db: db, Dialect: gorp.MySQLDialect{"InnoDB", "utf8"}}

	if full {
		dbmap.AddTableWithName(model.KlineW{}, "kline_w").SetKeys(false, "Code", "Date", "Klid")
		dbmap.AddTableWithName(model.KlineM{}, "kline_m").SetKeys(false, "Code", "Date", "Klid")
		dbmap.AddTableWithName(model.Indicator{},"indicator_d").SetKeys(false,"Code","Date","Klid")
		dbmap.AddTableWithName(model.IndicatorW{},"indicator_w").SetKeys(false,"Code","Date","Klid")
		dbmap.AddTableWithName(model.IndicatorM{},"indicator_m").SetKeys(false,"Code","Date","Klid")
		err = dbmap.CreateTablesIfNotExists()
		checkErr(err, "Create tables failed,")
		//err = dbmap.TruncateTables()
		//checkErr(err, "Truncate tables failed,")
	}

	checkErr(db.Ping(), "Failed to ping db,")

	return dbmap
}

func checkErr(err error, msg string) {
	if err != nil {
		log.Fatalln(msg, err)
		os.Exit(1)
	}
}

func cal(dbmap *gorp.DbMap) {
	stocks := getStocks(dbmap)

	var wg sync.WaitGroup
	wg.Add(len(stocks))

	for _, s := range stocks {
		go func(s model.Stock, dbmap *gorp.DbMap) {
			defer wg.Done()

			log.Println("Calculating week kline for " + s.Code)
			var klines []*model.Kline
			_, err := dbmap.Select(&klines, "select * from kline_d where code = ? order by code, date", s.Code)
			checkErr(err, "Failed to query kline_d for "+s.Code)

			log.Printf("%s, kline(s) of day: %d", s.Code, len(klines))

			q :=  make([]*model.Quote, len(klines))
			var qw []*model.Quote
			var qm []*model.Quote
			var klinesw []*model.KlineW
			var klinesm []*model.KlineM

			klw := newKlinew()
			klm := newKlinem()
			var lastWeekDay, lastMonth int = 7, 0
			var klid_w, klid_m int = 0, 0
			for i, k := range klines {
				q[i] = &k.Quote
				t, err := time.Parse("2006-01-02 15:04:05", k.Date)
				checkErr(err, "failed to parse date from kline_d "+k.Date)
				tw, err := time.Parse("2006-01-02", klw.Date)
				checkErr(err, "failed to parse date in KlineW "+klw.Date)

				if int(t.Weekday()) <= lastWeekDay || t.Add(-1*time.Duration(7)*time.Hour*24).After(tw) {
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

				if int(t.Month()) != lastMonth {
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

			kdj := indc.DeftKDJ(q)
			kdjw := indc.DeftKDJ_W(qw)
			kdjm := indc.DeftKDJ_M(qm)
			batchInsert(dbmap, klinesw, klinesm, kdj, kdjw, kdjm)

			log.Printf("Complete: %s, day: %d, week: %d, month: %d\n", s.Code, len(klines), len(klinesw), len(klinesm))
		}(s, dbmap)
	}

	wg.Wait()

	log.Println("Finished processing")
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

func batchInsert(dbmap *gorp.DbMap, klinesw []*model.KlineW, klinesm []*model.KlineM, indc []*model.Indicator, indcw []*model.IndicatorW, indcm []*model.IndicatorM) {
	var code string
	// Start a new transaction
	/*
	if len(klinesw) > 0 {
		trans, err := dbmap.Begin()
		checkErr(err, "failed to start a new transaction")
		for i, _ := range klinesw {
			k := klinesw[i]
			trans.Insert(k)
			if i%200 == 0 {
				checkErr(trans.Commit(), "failed to commit transaction")
				trans, err = dbmap.Begin()
				checkErr(err, "failed to start a new transaction")
			}
			code = k.Code
		}
		checkErr(trans.Commit(), "failed to commit transaction")
		log.Printf("%s: %d records saved to klines_w", code, len(klinesw))
	}

	if len(klinesm) > 0 {
		trans, err := dbmap.Begin()
		checkErr(err, "failed to start a new transaction")
		for i, _ := range klinesm {
			k := klinesm[i]
			trans.Insert(k)
			if i%200 == 0 {
				checkErr(trans.Commit(), "failed to commit transaction")
				trans, err = dbmap.Begin()
				checkErr(err, "failed to start a new transaction")
			}
		}
		checkErr(trans.Commit(), "failed to commit transaction")
		log.Printf("%s: %d records saved to klines_m", code, len(klinesm))
	}*/

	if len(indc) > 0 {
		trans, err := dbmap.Begin()
		checkErr(err, "failed to start a new transaction")

		for i, _ := range indc {
			k := indc[i]
			trans.Insert(k)
			if i%200 == 0 {
				checkErr(trans.Commit(), "failed to commit transaction")
				trans, err = dbmap.Begin()
				checkErr(err, "failed to start a new transaction")
			}
		}
		checkErr(trans.Commit(), "failed to commit transaction")
		log.Printf("%s: %d records saved to indicator_d", code, len(indc))
	}

	if len(indcw) > 0 {
		trans, err := dbmap.Begin()
		checkErr(err, "failed to start a new transaction")

		for i, _ := range indcw {
			k := indcw[i]
			trans.Insert(k)
			if i%200 == 0 {
				checkErr(trans.Commit(), "failed to commit transaction")
				trans, err = dbmap.Begin()
				checkErr(err, "failed to start a new transaction")
			}
		}
		checkErr(trans.Commit(), "failed to commit transaction")
		log.Printf("%s: %d records saved to indicator_w", code, len(indcw))
	}

	if len(indcm) > 0 {
		trans, err := dbmap.Begin()
		checkErr(err, "failed to start a new transaction")

		for i, _ := range indcm {
			k := indcm[i]
			trans.Insert(k)
			if i%200 == 0 {
				checkErr(trans.Commit(), "failed to commit transaction")
				trans, err = dbmap.Begin()
				checkErr(err, "failed to start a new transaction")
			}
		}
		checkErr(trans.Commit(), "failed to commit transaction")
		log.Printf("%s: %d records saved to indicator_m", code, len(indcm))
	}
}
