package main

import (
	"github.com/carusyte/stock/db"
	"github.com/carusyte/stock/util"
	"gopkg.in/gorp.v2"
	"io"
	"log"
	"os"
	"time"
)

const MAX_CONCURRENCY = 128
const JOB_CAPACITY = 256
const LOGFILE = "getd.log"
// will make some of the requests via proxy, 0.6 = 3/5
const PART_PROXY = 0
const PROXY_ADDR = "127.0.0.1:1080"

var (
	dbmap *gorp.DbMap
)

func init() {
	if _, err := os.Stat(LOGFILE); err == nil {
		os.Remove(LOGFILE)
	}
	logFile, err := os.OpenFile(LOGFILE, os.O_CREATE|os.O_RDWR, 0666)
	util.CheckErr(err, "failed to open log file")
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)
	dbmap = db.Get(true, false)
	util.PART_PROXY = PART_PROXY
	util.PROXY_ADDR = PROXY_ADDR
}

func main() {
	start := time.Now()
	defer stop("GETD_TOTAL",start)
	stks := GetStockInfo()
	stop("STOCK_LIST",start)

	stgx := time.Now()
	GetXDXRs(stks)
	stop("GET_XDXR",stgx)

	stgfi := time.Now()
	GetFinance(stks)
	stop("GET_FINANCE",stgfi)

	stgkl := time.Now()
	GetKlines(stks)
	stop("GET_KLINES",stgkl)

	//stci := time.Now()
	//CalcIndics(stks)
	//stop("CALC_INDICS",stci)
}

func stop(code string, start time.Time) {
	ss := start.Format("2006-01-02 15:04:05")
	end := time.Now().Format("2006-01-02 15:04:05")
	dur := time.Since(start).Seconds()
	log.Printf("%s Complete. Time Elapsed: %f sec", code, dur)
	dbmap.Exec("insert into stats (code, start, end, dur) values (?, ?, ?, ?) "+
		"on duplicate key update start=values(start), end=values(end), dur=values(dur)",
		code, ss, end, dur)
}
