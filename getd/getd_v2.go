package getd

import (
	"math"
	"time"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/model"
)

//GetV2 gets miscellaneous stock info.
func GetV2() {
	var allstks, stks *model.Stocks
	start := time.Now()
	defer StopWatch("GETD_TOTAL", start)
	if !conf.Args.DataSource.SkipStocks {
		allstks = GetStockInfo()
		StopWatch("STOCK_LIST", start)
	} else {
		log.Printf("skipped stock data from web")
		allstks = new(model.Stocks)
		stks := StocksDb()
		log.Printf("%d stocks queried from db", len(stks))
		allstks.Add(stks...)
	}

	//every step hereafter returns only the stocks successfully processed
	if !conf.Args.DataSource.SkipFinance {
		stgfi := time.Now()
		stks = GetFinance(allstks)
		StopWatch("GET_FINANCE", stgfi)
	} else {
		log.Printf("skipped finance data from web")
		stks = allstks
	}

	if !conf.Args.DataSource.SkipFinancePrediction {
		fipr := time.Now()
		stks = GetFinPrediction(stks)
		StopWatch("GET_FIN_PREDICT", fipr)
	} else {
		log.Printf("skipped financial prediction data from web")
	}

	if !conf.Args.DataSource.SkipXdxr {
		// Validate Kline process already fetches XDXR info
		if conf.Args.DataSource.SkipKlineVld {
			stgx := time.Now()
			stks = GetXDXRs(stks)
			StopWatch("GET_XDXR", stgx)
		}
	} else {
		log.Printf("skipped xdxr data from web")
	}

	stks = getKlineVld(stks)

	src := model.DataSource(conf.Args.DataSource.Kline)
	frs := make([]FetchRequest, 3)
	cs := []model.CYTP{model.DAY, model.WEEK, model.MONTH}
	if !conf.Args.DataSource.SkipKlinePre {
		begin := time.Now()
		for i := range frs {
			frs[i] = FetchRequest{
				RemoteSource: src,
				LocalSource:  model.KlineMaster,
				Reinstate:    model.None,
				Cycle:        cs[i],
			}
		}
		stks = GetKlinesV2(stks, frs...)
		StopWatch("GET_KLINES_PRE", begin)
	} else {
		log.Printf("skipped kline-pre data from web (non-reinstated)")
	}

	if !conf.Args.DataSource.SkipKlines {
		begin := time.Now()
		frs = make([]FetchRequest, 6)
		for i := range frs {
			csi := int(math.Mod(float64(i), 3))
			r := model.Backward
			if i > 2 {
				r = model.Forward
			}
			frs[i] = FetchRequest{
				RemoteSource: src,
				LocalSource:  model.KlineMaster,
				Reinstate:    r,
				Cycle:        cs[csi],
			}
		}
		stks = GetKlinesV2(stks, frs...)
		StopWatch("GET_MASTER_KLINES", begin)
	} else {
		log.Printf("skipped klines data from web (backward & forward reinstated)")
	}

	FreeFetcherResources()
	stks = KlinePostProcess(stks)

	var allIdx, sucIdx []*model.IdxLst
	if !conf.Args.DataSource.SkipIndices {
		stidx := time.Now()
		allIdx, sucIdx = GetIndicesV2()
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

func getKlineVld(stks *model.Stocks) *model.Stocks {
	if conf.Args.DataSource.SkipKlineVld {
		log.Printf("skipped kline-vld data from web")
		return stks
	}

	vsrc := model.DataSource(conf.Args.DataSource.Validate.Source)
	cs := []model.CYTP{model.DAY, model.WEEK, model.MONTH}
	var frs []FetchRequest
	if conf.Args.DataSource.Validate.SkipKlinePre {
		log.Printf("skipped preliminary data for validate klines (non-reinstated)")
	} else {
		frs = make([]FetchRequest, 3)
		for i := range frs {
			frs[i] = FetchRequest{
				RemoteSource: vsrc,
				LocalSource:  vsrc,
				Reinstate:    model.None,
				Cycle:        cs[i],
			}
		}
		begin := time.Now()
		stks = GetKlinesV2(stks, frs...)
		UpdateValidateKlineParams()
		StopWatch("GET_KLINES_VLD_PRE", begin)
	}

	if conf.Args.DataSource.Validate.SkipKlines {
		log.Printf("skipped validate kline main data (backward & forward reinstated)")
	} else {
		frs = make([]FetchRequest, 6)
		for i := range frs {
			csi := int(math.Mod(float64(i), 3))
			r := model.Backward
			if i > 2 {
				r = model.Forward
			}
			frs[i] = FetchRequest{
				RemoteSource: vsrc,
				LocalSource:  vsrc,
				Reinstate:    r,
				Cycle:        cs[csi],
			}
		}
		begin := time.Now()
		stks = GetKlinesV2(stks, frs...)
		StopWatch("GET_KLINES_VLD_MAIN", begin)
	}

	FreeFetcherResources()

	return stks
}
