package getd

import (
	"fmt"
	"log"
	"time"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
)

func Get() {
	var allstks, stks *model.Stocks
	if !conf.Args.Datasource.SkipStocks {
		start := time.Now()
		defer stop("GETD_TOTAL", start)
		allstks = GetStockInfo()
		stop("STOCK_LIST", start)
	} else {
		log.Printf("skipped stock data from web")
		allstks = new(model.Stocks)
		stks := StocksDb()
		log.Printf("%d stocks queried from db", len(stks))
		allstks.Add(stks...)
	}

	//every step here and after returns only the stocks successfully processed
	if !conf.Args.Datasource.SkipFinance {
		stgfi := time.Now()
		stks = GetFinance(allstks)
		stop("GET_FINANCE", stgfi)
	} else {
		log.Printf("skipped finance data from web")
		stks = allstks
	}

	if !conf.Args.Datasource.SkipKlineDn {
		stgkdn := time.Now()
		stks = GetKlines(stks, model.KLINE_DAY_NR)
		stop("GET_KLINES_DN", stgkdn)
	} else {
		log.Printf("skipped non-reinstated daily kline data from web")
	}

	if !conf.Args.Datasource.SkipFinancePrediction {
		fipr := time.Now()
		stks = GetFinPrediction(stks)
		stop("GET_FIN_PREDICT", fipr)
	} else {
		log.Printf("skipped financial prediction data from web")
	}

	if !conf.Args.Datasource.SkipXdxr {
		stgx := time.Now()
		stks = GetXDXRs(stks)
		stop("GET_XDXR", stgx)
	} else {
		log.Printf("skipped xdxr data from web")
	}

	if !conf.Args.Datasource.SkipKlines {
		stgkl := time.Now()
		stks = GetKlines(stks, model.KLINE_DAY, model.KLINE_WEEK, model.KLINE_MONTH)
		stop("GET_KLINES", stgkl)
	} else {
		log.Printf("skipped klines data from web")
	}

	var allIdx, sucIdx []*model.IdxLst
	if !conf.Args.Datasource.SkipIndices {
		stidx := time.Now()
		allIdx, sucIdx = GetIndices()
		stop("GET_INDICES", stidx)
		for _, idx := range allIdx {
			allstks.Add(&model.Stock{Code: idx.Code, Name: idx.Name})
		}
	} else {
		log.Printf("skipped index data from web")
	}

	if !conf.Args.Datasource.SkipBasicsUpdate {
		updb := time.Now()
		stks = updBasics(stks)
		stop("UPD_BASICS", updb)
	} else {
		log.Printf("skipped updating basics table")
	}

	// Add indices pending to be calculated
	for _, idx := range sucIdx {
		stks.Add(&model.Stock{Code: idx.Code, Name: idx.Name})
	}
	if !conf.Args.Datasource.SkipIndexCalculation {
		stci := time.Now()
		stks = CalcIndics(stks)
		stop("CALC_INDICS", stci)
	} else {
		log.Printf("skipped index calculation")
	}

	if !conf.Args.Datasource.SkipFinMark {
		finMark(stks)
	} else {
		log.Printf("skipped updating fin mark")
	}

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

//update xpriced flag in xdxr to mark that all price related data has been reinstated
func finMark(stks *model.Stocks) *model.Stocks {
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
