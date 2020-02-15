package getd

import (
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/indc"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
)

const (
	//HistDataSize history data size
	HistDataSize = 200
	//JobCapacity reference
	JobCapacity = global.JobCapacity
	//KdjFdPrunePrec the precision for KDJ feature data pruning
	KdjFdPrunePrec = 0.99
	//KdjPruneRate the rate for KDJ pruning
	KdjPruneRate = 0.1
)

//CalcIndics calculates various indicators for given stocks.
func CalcIndics(stocks *model.Stocks) (rstks *model.Stocks) {
	log.Println("calculating indices...")
	var wg sync.WaitGroup
	chstk := make(chan *model.Stock, JobCapacity)
	chrstk := make(chan *model.Stock, JobCapacity)
	rstks = new(model.Stocks)
	wgr := collect(rstks, chrstk)
	for i := 0; i < int(float64(runtime.NumCPU())*0.7); i++ {
		wg.Add(1)
		go doCalcIndices(chstk, &wg, chrstk)
	}
	for _, s := range stocks.List {
		chstk <- s
	}
	close(chstk)
	wg.Wait()
	close(chrstk)
	wgr.Wait()
	log.Printf("%d indicators updated", rstks.Size())
	if stocks.Size() != rstks.Size() {
		same, skp := stocks.Diff(rstks)
		if !same {
			log.Printf("Failed: %+v", skp)
		}
	}
	//Pruning takes too long to complete, make it a separate process
	//PruneKdjFeatDat(KdjFdPrunePrec, KDJ_FD_PRUNE_PASS)
	return
}

func doCalcIndices(chstk chan *model.Stock, wg *sync.WaitGroup, chrstk chan *model.Stock) {
	defer wg.Done()
	for stock := range chstk {
		code := stock.Code
		var offd, offw, offm int64 = 10, 5, 5
		lx := latestUFRXdxr(code)
		if lx != nil {
			offd, offw, offm = -1, -1, -1
		}
		purgeKdjFeatDat(code)
		calcDay(stock, offd)
		calcWeek(stock, offw)
		calcMonth(stock, offm)
		chrstk <- stock
	}
}

func calcWeek(stk *model.Stock, offset int64) {
	var (
		err  error
		code = stk.Code
	)
	tab := "index_w_n"
	if len(stk.Source) == 0 { // non index
		switch model.Rtype(conf.Args.DataSource.IndicatorSource) {
		case model.Forward:
			tab = "kline_w_f"
		case model.Backward:
			tab = "kline_w_b"
		case model.None:
			tab = "kline_w_n"
		default:
			panic("undefined reinstatement type:" + conf.Args.DataSource.IndicatorSource)
		}
	}

	var qw []*model.TradeDataBasic
	_, err = dbmap.Select(&qw, fmt.Sprintf("select code,date,klid,open,high,close,low,volume,amount,xrate "+
		"from %s where code = ? order by klid", tab), code)
	util.CheckErr(err, fmt.Sprintf("Failed to query %s for %s", tab, code))

	indicators := indc.DeftKDJ(qw)
	macd := indc.DeftMACD(qw)
	for i, idc := range indicators {
		idc.MACD = macd[i].MACD
		idc.MACD_diff = macd[i].MACD_diff
		idc.MACD_dea = macd[i].MACD_dea
	}
	rsi := indc.DeftRSI(qw)
	for i, idc := range indicators {
		idc.RSI1 = rsi[i].RSI1
		idc.RSI2 = rsi[i].RSI2
		idc.RSI3 = rsi[i].RSI3
	}
	bias := indc.DeftBIAS(qw)
	for i, idc := range indicators {
		idc.BIAS1 = bias[i].BIAS1
		idc.BIAS2 = bias[i].BIAS2
		idc.BIAS3 = bias[i].BIAS3
	}
	boll := indc.DeftBOLL(qw)
	for i, idc := range indicators {
		idc.BOLL_lower = boll[i].BOLL_lower
		idc.BOLL_lower_c = boll[i].BOLL_lower_c
		idc.BOLL_lower_h = boll[i].BOLL_lower_h
		idc.BOLL_lower_l = boll[i].BOLL_lower_l
		idc.BOLL_lower_o = boll[i].BOLL_lower_o
		idc.BOLL_mid = boll[i].BOLL_mid
		idc.BOLL_mid_c = boll[i].BOLL_mid_c
		idc.BOLL_mid_h = boll[i].BOLL_mid_h
		idc.BOLL_mid_l = boll[i].BOLL_mid_l
		idc.BOLL_mid_o = boll[i].BOLL_mid_o
		idc.BOLL_upper = boll[i].BOLL_upper
		idc.BOLL_upper_c = boll[i].BOLL_upper_c
		idc.BOLL_upper_h = boll[i].BOLL_upper_h
		idc.BOLL_upper_l = boll[i].BOLL_upper_l
		idc.BOLL_upper_o = boll[i].BOLL_upper_o
	}

	binsIndc(indicators, "indicator_w")

	if conf.Args.DataSource.SampleKdjFeature {
		panic("function not refactored yet")
		// SmpKdjFeat(code, model.WEEK, 5.0, 2.0, 2)
	}
}

func calcMonth(stk *model.Stock, offset int64) {
	var (
		err  error
		code = stk.Code
	)

	tab := "index_m_n"
	if len(stk.Source) == 0 { // non index
		switch model.Rtype(conf.Args.DataSource.IndicatorSource) {
		case model.Forward:
			tab = "kline_m_f"
		case model.Backward:
			tab = "kline_m_b"
		case model.None:
			tab = "kline_m_n"
		default:
			panic("undefined reinstatement type:" + conf.Args.DataSource.IndicatorSource)
		}
	}

	var qm []*model.TradeDataBasic
	_, err = dbmap.Select(&qm, fmt.Sprintf("select code,date,klid,open,high,close,low,volume,amount,xrate "+
		"from %s where code = ? order by klid", tab), code)
	util.CheckErr(err, fmt.Sprintf("Failed to query %s for %s", tab, code))

	indicators := indc.DeftKDJ(qm)
	macd := indc.DeftMACD(qm)
	for i, idc := range indicators {
		idc.MACD = macd[i].MACD
		idc.MACD_diff = macd[i].MACD_diff
		idc.MACD_dea = macd[i].MACD_dea
	}
	rsi := indc.DeftRSI(qm)
	for i, idc := range indicators {
		idc.RSI1 = rsi[i].RSI1
		idc.RSI2 = rsi[i].RSI2
		idc.RSI3 = rsi[i].RSI3
	}
	bias := indc.DeftBIAS(qm)
	for i, idc := range indicators {
		idc.BIAS1 = bias[i].BIAS1
		idc.BIAS2 = bias[i].BIAS2
		idc.BIAS3 = bias[i].BIAS3
	}
	boll := indc.DeftBOLL(qm)
	for i, idc := range indicators {
		idc.BOLL_lower = boll[i].BOLL_lower
		idc.BOLL_lower_c = boll[i].BOLL_lower_c
		idc.BOLL_lower_h = boll[i].BOLL_lower_h
		idc.BOLL_lower_l = boll[i].BOLL_lower_l
		idc.BOLL_lower_o = boll[i].BOLL_lower_o
		idc.BOLL_mid = boll[i].BOLL_mid
		idc.BOLL_mid_c = boll[i].BOLL_mid_c
		idc.BOLL_mid_h = boll[i].BOLL_mid_h
		idc.BOLL_mid_l = boll[i].BOLL_mid_l
		idc.BOLL_mid_o = boll[i].BOLL_mid_o
		idc.BOLL_upper = boll[i].BOLL_upper
		idc.BOLL_upper_c = boll[i].BOLL_upper_c
		idc.BOLL_upper_h = boll[i].BOLL_upper_h
		idc.BOLL_upper_l = boll[i].BOLL_upper_l
		idc.BOLL_upper_o = boll[i].BOLL_upper_o
	}

	binsIndc(indicators, "indicator_m")

	if conf.Args.DataSource.SampleKdjFeature {
		panic("function not refactored yet")
		// SmpKdjFeat(code, model.MONTH, 5.0, 2.0, 2)
	}
}

func calcDay(stk *model.Stock, offset int64) {
	var (
		err  error
		code = stk.Code
	)

	tab := "index_d_n"
	if len(stk.Source) == 0 { //non index
		switch model.Rtype(conf.Args.DataSource.IndicatorSource) {
		case model.Forward:
			tab = "kline_d_f"
		case model.Backward:
			tab = "kline_d_b"
		case model.None:
			tab = "kline_d_n"
		default:
			panic("undefined reinstatement type:" + conf.Args.DataSource.IndicatorSource)
		}
	}

	var qd []*model.TradeDataBasic
	_, err = dbmap.Select(&qd, fmt.Sprintf("select code,date,klid,open,high,close,low,volume,amount,xrate from "+
		"%s where code = ? order by klid", tab), code)
	util.CheckErr(err, fmt.Sprintf("Failed to query %s for %s", tab, code))

	indicators := indc.DeftKDJ(qd)
	macd := indc.DeftMACD(qd)
	for i, idc := range indicators {
		idc.MACD = macd[i].MACD
		idc.MACD_diff = macd[i].MACD_diff
		idc.MACD_dea = macd[i].MACD_dea
	}
	rsi := indc.DeftRSI(qd)
	for i, idc := range indicators {
		idc.RSI1 = rsi[i].RSI1
		idc.RSI2 = rsi[i].RSI2
		idc.RSI3 = rsi[i].RSI3
	}
	bias := indc.DeftBIAS(qd)
	for i, idc := range indicators {
		idc.BIAS1 = bias[i].BIAS1
		idc.BIAS2 = bias[i].BIAS2
		idc.BIAS3 = bias[i].BIAS3
	}
	boll := indc.DeftBOLL(qd)
	for i, idc := range indicators {
		idc.BOLL_lower = boll[i].BOLL_lower
		idc.BOLL_lower_c = boll[i].BOLL_lower_c
		idc.BOLL_lower_h = boll[i].BOLL_lower_h
		idc.BOLL_lower_l = boll[i].BOLL_lower_l
		idc.BOLL_lower_o = boll[i].BOLL_lower_o
		idc.BOLL_mid = boll[i].BOLL_mid
		idc.BOLL_mid_c = boll[i].BOLL_mid_c
		idc.BOLL_mid_h = boll[i].BOLL_mid_h
		idc.BOLL_mid_l = boll[i].BOLL_mid_l
		idc.BOLL_mid_o = boll[i].BOLL_mid_o
		idc.BOLL_upper = boll[i].BOLL_upper
		idc.BOLL_upper_c = boll[i].BOLL_upper_c
		idc.BOLL_upper_h = boll[i].BOLL_upper_h
		idc.BOLL_upper_l = boll[i].BOLL_upper_l
		idc.BOLL_upper_o = boll[i].BOLL_upper_o
	}

	binsIndc(indicators, "indicator_d")

	if conf.Args.DataSource.SampleKdjFeature {
		panic("function not refactored yet")
		// SmpKdjFeat(code, model.DAY, 5.0, 2.0, 2)
	}
}

func binsIndc(indc []*model.Indicator, table string) (c int) {
	if len(indc) == 0 {
		return
	}
	retry := conf.Args.DeadlockRetry
	rt := 0
	sklid := indc[0].Klid
	if len(indc) > 5 {
		sklid = indc[len(indc)-5].Klid
	}
	code := indc[0].Code
	var e error
	for ; rt < retry; rt++ {
		stmt := fmt.Sprintf("delete from %s where code = ? and klid >= ?", table)
		_, e = dbmap.Exec(stmt, code, sklid)
		if e != nil {
			fmt.Println(e)
			if strings.Contains(e.Error(), "Deadlock") {
				continue
			} else {
				log.Panicf("%s failed to delete stale %s data\n%+v", code, table, e)
			}
		}
		break
	}
	if rt >= retry {
		log.Panicf("%s failed to delete %s where klid > %d", code, table, sklid)
	}
	batchSize := 200
	for idx := 0; idx < len(indc); idx += batchSize {
		end := int(math.Min(float64(len(indc)), float64(idx+batchSize)))
		c += insertIndicMiniBatch(indc[idx:end], table)
	}
	return
}

func insertIndicMiniBatch(indc []*model.Indicator, table string) (c int) {
	numFields := 32
	holders := make([]string, numFields)
	for i := range holders {
		holders[i] = "?"
	}
	holderString := fmt.Sprintf("(%s)", strings.Join(holders, ","))
	valueStrings := make([]string, 0, len(indc))
	valueArgs := make([]interface{}, 0, len(indc)*numFields)
	code := indc[0].Code
	var e error
	for _, i := range indc {
		d, t := util.TimeStr()
		i.Udate.Valid = true
		i.Utime.Valid = true
		i.Udate.String = d
		i.Utime.String = t
		valueStrings = append(valueStrings, holderString)
		valueArgs = append(valueArgs, i.Code)
		valueArgs = append(valueArgs, i.Date)
		valueArgs = append(valueArgs, i.Klid)
		valueArgs = append(valueArgs, i.KDJ_K)
		valueArgs = append(valueArgs, i.KDJ_D)
		valueArgs = append(valueArgs, i.KDJ_J)
		valueArgs = append(valueArgs, i.MACD)
		valueArgs = append(valueArgs, i.MACD_diff)
		valueArgs = append(valueArgs, i.MACD_dea)
		valueArgs = append(valueArgs, i.RSI1)
		valueArgs = append(valueArgs, i.RSI2)
		valueArgs = append(valueArgs, i.RSI3)
		valueArgs = append(valueArgs, i.BIAS1)
		valueArgs = append(valueArgs, i.BIAS2)
		valueArgs = append(valueArgs, i.BIAS3)
		valueArgs = append(valueArgs, i.BOLL_lower)
		valueArgs = append(valueArgs, i.BOLL_lower_c)
		valueArgs = append(valueArgs, i.BOLL_lower_h)
		valueArgs = append(valueArgs, i.BOLL_lower_l)
		valueArgs = append(valueArgs, i.BOLL_lower_o)
		valueArgs = append(valueArgs, i.BOLL_mid)
		valueArgs = append(valueArgs, i.BOLL_mid_c)
		valueArgs = append(valueArgs, i.BOLL_mid_h)
		valueArgs = append(valueArgs, i.BOLL_mid_l)
		valueArgs = append(valueArgs, i.BOLL_mid_o)
		valueArgs = append(valueArgs, i.BOLL_upper)
		valueArgs = append(valueArgs, i.BOLL_upper_c)
		valueArgs = append(valueArgs, i.BOLL_upper_h)
		valueArgs = append(valueArgs, i.BOLL_upper_l)
		valueArgs = append(valueArgs, i.BOLL_upper_o)
		valueArgs = append(valueArgs, i.Udate)
		valueArgs = append(valueArgs, i.Utime)
	}

	retry := conf.Args.DeadlockRetry
	rt := 0
	stmt := fmt.Sprintf("INSERT INTO %s (code,date,klid,kdj_k,kdj_d,kdj_j,macd,macd_diff,macd_dea,"+
		"rsi1,rsi2,rsi3,bias1,bias2,bias3,"+
		"boll_lower,boll_lower_c,boll_lower_h,boll_lower_l,boll_lower_o,"+
		"boll_mid,boll_mid_c,boll_mid_h,boll_mid_l,boll_mid_o,"+
		"boll_upper,boll_upper_c,boll_upper_h,boll_upper_l,boll_upper_o,"+
		"udate,utime) VALUES %s on "+
		"duplicate key update date=values(date),kdj_k=values(kdj_k),kdj_d=values(kdj_d),kdj_j=values"+
		"(kdj_j),macd=values(macd),macd_diff=values(macd_diff),macd_dea=values(macd_dea),"+
		"rsi1=values(rsi1),rsi2=values(rsi2),rsi3=values(rsi3),"+
		"bias1=values(bias1),bias2=values(bias2),bias3=values(bias3),"+
		"boll_lower=values(boll_lower),boll_lower_c=values(boll_lower_c),boll_lower_h=values(boll_lower_h),"+
		"boll_lower_l=values(boll_lower_l),boll_lower_o=values(boll_lower_o),"+
		"boll_mid=values(boll_mid),boll_mid_c=values(boll_mid_c),boll_mid_h=values(boll_mid_h),"+
		"boll_mid_l=values(boll_mid_l),boll_mid_o=values(boll_mid_o),"+
		"boll_upper=values(boll_upper),boll_upper_c=values(boll_upper_c),boll_upper_h=values(boll_upper_h),"+
		"boll_upper_l=values(boll_upper_l),boll_upper_o=values(boll_upper_o),"+
		"udate=values(udate),utime=values(utime)",
		table, strings.Join(valueStrings, ","))
	for ; rt < retry; rt++ {
		_, e = dbmap.Exec(stmt, valueArgs...)
		if e != nil {
			fmt.Println(e)
			if strings.Contains(e.Error(), "Deadlock") {
				continue
			} else {
				log.Panicf("%s failed to overwrite %s: %+v", code, table, e)
			}
		}
		return len(indc)
	}
	log.Panicf("%s failed to overwrite %s: %+v", code, table, e)
	return
}
