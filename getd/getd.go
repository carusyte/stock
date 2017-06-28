package getd

import (
	"log"
	"time"
)

func Get(){
	start := time.Now()
	defer stop("GETD_TOTAL", start)
	stks := GetStockInfo()
	stop("STOCK_LIST", start)

	stgfi := time.Now()
	GetFinance(stks)
	stop("GET_FINANCE", stgfi)

	stgkdn := time.Now()
	GetKlines(stks, DAY_N)
	stop("GET_KLINES_DN", stgkdn)

	stgx := time.Now()
	GetXDXRs(stks)
	stop("GET_XDXR", stgx)

	stgkl := time.Now()
	GetKlines(stks, DAY, WEEK, MONTH)
	stop("GET_KLINES", stgkl)

	updb := time.Now()
	UpdBasics()
	stop("UPD_BASICS", updb)

	stci := time.Now()
	CalcIndics(stks)
	stop("CALC_INDICS", stci)
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
