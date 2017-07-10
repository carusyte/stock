package getd

import (
	"log"
	"time"
	"github.com/carusyte/stock/model"
	"fmt"
	"github.com/carusyte/stock/util"
)

func Get() {
	start := time.Now()
	defer stop("GETD_TOTAL", start)
	allstks := GetStockInfo()
	stop("STOCK_LIST", start)

	//every step here and after returns only the stocks successfully processed
	stgfi := time.Now()
	stks := GetFinance(allstks)
	stop("GET_FINANCE", stgfi)

	stgkdn := time.Now()
	stks = GetKlines(stks, model.KLINE_DAY_NR)
	stop("GET_KLINES_DN", stgkdn)

	stgx := time.Now()
	stks = GetXDXRs(stks)
	stop("GET_XDXR", stgx)

	stgkl := time.Now()
	stks = GetKlines(stks, model.KLINE_DAY, model.KLINE_WEEK, model.KLINE_MONTH)
	stop("GET_KLINES", stgkl)

	updb := time.Now()
	stks = updBasics(stks)
	stop("UPD_BASICS", updb)

	stci := time.Now()
	stks = CalcIndics(stks)
	stop("CALC_INDICS", stci)

	finMark(stks)

	rptFailed(allstks, stks)
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

func finMark(stks *model.Stocks) *model.Stocks {
	//update xpriced flag in xdxr to mark that all price related data has been reinstated
	sql, e := dot.Raw("UPD_XPRICE")
	util.CheckErr(e, "failed to get UPD_XPRICE sql")
	sql = fmt.Sprintf(sql, util.Join(stks.Codes, ",", true))
	_, e = dbmap.Exec(sql)
	util.CheckErr(e, "failed to update xprice, sql:\n"+sql)
	log.Printf("%d xprice mark updated", stks.Size())
	return stks
}

func rptFailed(all *model.Stocks, fin *model.Stocks) {
	log.Printf("Finish:[%d]\tTotal:[%d]", fin.Size(), all.Size())
	if fin.Size() != all.Size() {
		same, skp := all.Diff(fin)
		if !same {
			log.Printf("Unfinished: %+v", skp)
		}
	}
}