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

var (
	dbmap *gorp.DbMap
)

func init() {
	logFile, err := os.OpenFile("calk.log", os.O_CREATE|os.O_RDWR, 0666)
	util.CheckErr(err, "failed to open log file")
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)
	dbmap = db.Get(true, false)
}

func main() {
	start := time.Now()
	defer stop("GETD_TOTAL",start)
	stks := GetStockInfo()
	stop("STOCK_LIST",start)

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
	log.Printf("%s Complete. Time Elapsed: %f sec", code, time.Since(start).Seconds())
	dbmap.Exec("insert into stats (code, start, end, dur) values (?, ?, ?, ?) "+
		"on duplicate key update start=values(start), end=values(end), dur=values(dur)",
		code, ss, end, dur)
}
