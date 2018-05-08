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
	if !conf.Args.DataSource.SkipStocks {
		start := time.Now()
		defer StopWatch("GETD_TOTAL", start)
		allstks = GetStockInfo()
		StopWatch("STOCK_LIST", start)
	} else {
		log.Printf("skipped stock data from web")
		allstks = new(model.Stocks)
		stks := StocksDb()
		log.Printf("%d stocks queried from db", len(stks))
		allstks.Add(stks...)
	}

	//every step here and after returns only the stocks successfully processed
	if !conf.Args.DataSource.SkipFinance {
		stgfi := time.Now()
		stks = GetFinance(allstks)
		StopWatch("GET_FINANCE", stgfi)
	} else {
		log.Printf("skipped finance data from web")
		stks = allstks
	}
	if !conf.Args.DataSource.SkipKlineVld {
		stgkvld := time.Now()
		stks = GetKlines(stks, model.KLINE_DAY_VLD, model.KLINE_WEEK_VLD, model.KLINE_MONTH_VLD)
		StopWatch("GET_KLINES_VLD", stgkvld)
	} else {
		log.Printf("skipped kline-vld data from web")
	}
	if !conf.Args.DataSource.SkipKlinePre {
		stgkpre := time.Now()
		stks = GetKlines(stks, model.KLINE_DAY_NR,
			model.KLINE_MONTH_NR, model.KLINE_WEEK_NR)
		StopWatch("GET_KLINES_PRE", stgkpre)
	} else {
		log.Printf("skipped kline-pre data from web")
	}

	if !conf.Args.DataSource.SkipFinancePrediction {
		fipr := time.Now()
		stks = GetFinPrediction(stks)
		StopWatch("GET_FIN_PREDICT", fipr)
	} else {
		log.Printf("skipped financial prediction data from web")
	}

	if !conf.Args.DataSource.SkipXdxr {
		stgx := time.Now()
		stks = GetXDXRs(stks)
		StopWatch("GET_XDXR", stgx)
	} else {
		log.Printf("skipped xdxr data from web")
	}

	if !conf.Args.DataSource.SkipKlines {
		stgkl := time.Now()
		stks = GetKlines(stks, model.KLINE_DAY,
			model.KLINE_WEEK, model.KLINE_MONTH,
			model.KLINE_MONTH_B, model.KLINE_DAY_B,
			model.KLINE_WEEK_B)
		stks = KlinePostProcess(stks)
		StopWatch("GET_KLINES", stgkl)
	} else {
		log.Printf("skipped klines data from web")
	}

	var allIdx, sucIdx []*model.IdxLst
	if !conf.Args.DataSource.SkipIndices {
		stidx := time.Now()
		allIdx, sucIdx = GetIndices()
		StopWatch("GET_INDICES", stidx)
		for _, idx := range allIdx {
			allstks.Add(&model.Stock{Code: idx.Code, Name: idx.Name})
		}
	} else {
		log.Printf("skipped index data from web")
	}

	if !conf.Args.DataSource.SkipBasicsUpdate {
		updb := time.Now()
		stks = updBasics(stks)
		StopWatch("UPD_BASICS", updb)
	} else {
		log.Printf("skipped updating basics table")
	}

	// Add indices pending to be calculated
	for _, idx := range sucIdx {
		stks.Add(&model.Stock{Code: idx.Code, Name: idx.Name, Source: idx.Src})
	}
	if !conf.Args.DataSource.SkipIndexCalculation {
		stci := time.Now()
		stks = CalcIndics(stks)
		StopWatch("CALC_INDICS", stci)
	} else {
		log.Printf("skipped index calculation")
	}

	if !conf.Args.DataSource.SkipFsStats {
		stfss := time.Now()
		CollectFsStats()
		StopWatch("FS_STATS", stfss)
	} else {
		log.Printf("skipped feature scaling stats")
	}

	if !conf.Args.DataSource.SkipFinMark {
		finMark(stks)
	} else {
		log.Printf("skipped updating fin mark")
	}

	rptFailed(allstks, stks)
}

func StopWatch(code string, start time.Time) {
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
