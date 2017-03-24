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
)

type Stock struct {
	Code             string
	Name             string
	Industry         sql.NullString
	Area             sql.NullString
	Pe               float32
	Outstanding      float32
	Totals           float32
	TotalAssets      float64
	LiquidAssets     float64
	FixedAssets      float64
	Reserved         float64
	ReservedPerShare float32
	Esp              float32
	Bvps             float32
	Pb               float32
	TimeToMarket     string
	Undp             float64
	Perundp          float32
	Rev              float32
	Profit           float32
	Gpr              float32
	Npr              float32
	Holders          int64
}

type Kline struct {
	Code   string
	Date   string
	Open   float32
	High   float32
	Close  float32
	Low    float32
	Volume float64
	Amount float64
	Factor sql.NullFloat64
}

type Klinew struct {
	Code   string `db:",size:6"`
	Date   string `db:",size:10"`
	Klid   int
	Open   float32
	High   float32
	Close  float32
	Low    float32
	Volume float64
	Amount float64
}

type Klinem struct {
	Code   string `db:",size:6"`
	Date   string `db:",size:10"`
	Klid   int
	Open   float32
	High   float32
	Close  float32
	Low    float32
	Volume float64
	Amount float64
}

const APP_VERSION = "0.1"

// The flag package provides a default help printer via -h switch
var versionFlag *bool = flag.Bool("v", false, "Print the version number.")

func main() {
	flag.Parse() // Scan the arguments list

	if *versionFlag {
		fmt.Println("Version:", APP_VERSION)
	}

	// initialize the DbMap
	dbmap := initDb(true)

	cal(dbmap)

	dbmap.Db.Close()
}

func getStocks(dbmap *gorp.DbMap) []Stock {
	var stocks []Stock
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
		dbmap.AddTableWithName(Klinew{}, "kline_w").SetKeys(false, "Code", "Date", "Klid")
		dbmap.AddTableWithName(Klinem{}, "kline_m").SetKeys(false, "Code", "Date", "Klid")
		err = dbmap.CreateTablesIfNotExists()
		checkErr(err, "Create tables failed,")
		err = dbmap.TruncateTables()
		checkErr(err, "Truncate tables failed,")
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
		go func(s Stock, dbmap *gorp.DbMap) {
			defer wg.Done()

			log.Println("Calculating week kline for " + s.Code)
			var klines []Kline
			_, err := dbmap.Select(&klines, "select * from kline_d where code = ? order by code, date", s.Code)
			checkErr(err, "Failed to query kline_d for "+s.Code)

			log.Printf("%s, %d", s.Code, len(klines))

			var klinesw []*Klinew
			var klinesm []*Klinem

			klw := newKlinew()
			klm := newKlinem()
			var lastWeekDay, lastMonth int = 7, 0
			var klid_w, klid_m int = 0, 0
			for _, k := range klines {
				//log.Printf("%v\n", k)
				t, err := time.Parse("2006-01-02 15:04:05", k.Date)
				checkErr(err, "failed to parse date from kline_d "+k.Date)
				tw, err := time.Parse("2006-01-02", klw.Date)
				checkErr(err, "failed to parse date in Klinew "+klw.Date)

				if int(t.Weekday()) <= lastWeekDay || t.Add(-1*time.Duration(7)*time.Hour*24).After(tw) {
					klw = newKlinew()
					klinesw = append(klinesw, klw)
					klw.Code = k.Code

					klw.Klid = klid_w
					klid_w++

					klw.Open = k.Open
					klw.Low = k.Low
					lastWeekDay = int(t.Weekday())
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
			batchInsert(dbmap, klinesw, klinesm)

			log.Printf("Complete: %s, week: %d, month: %d\n", s.Code, len(klinesw), len(klinesm))
		}(s, dbmap)
	}

	wg.Wait()

	log.Println("Finished processing")
}

func newKlinew() *Klinew {
	klw := &Klinew{}
	klw.Klid = -1
	klw.Date = "1900-01-01"
	return klw
}

func newKlinem() *Klinem {
	klm := &Klinem{}
	klm.Klid = -1
	klm.Date = "1900-01-01"
	return klm
}

func batchInsert(dbmap *gorp.DbMap, klinesw []*Klinew, klinesm []*Klinem) {
	var code string
	// Start a new transaction
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
	}
}
