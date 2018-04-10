package getd

import (
	"database/sql"
	"fmt"
	"log"
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
	HIST_DATA_SIZE    = 200
	JOB_CAPACITY      = global.JOB_CAPACITY
	KDJ_FD_PRUNE_PREC = 0.99
	KDJ_PRUNE_RATE    = 0.1
)

var (
	dbmap = global.Dbmap
	dot   = global.Dot
)

func CalcIndics(stocks *model.Stocks) (rstks *model.Stocks) {
	log.Println("calculating indices...")
	var wg sync.WaitGroup
	chstk := make(chan *model.Stock, JOB_CAPACITY)
	chrstk := make(chan *model.Stock, JOB_CAPACITY)
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
	//PruneKdjFeatDat(KDJ_FD_PRUNE_PREC, KDJ_FD_PRUNE_PASS)
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
		mxw  sql.NullInt64
		err  error
		code = stk.Code
	)
	if offset >= 0 {
		mxw, err = dbmap.SelectNullInt("select max(klid) from indicator_w where code=?", code)
		util.CheckErr(err, "failed to query max klid in indicator_w for "+code)
	}

	tab := "kline_w"
	switch conf.Args.DataSource.KdjSource {
	case model.Backward:
		tab = "kline_w_b"
	case model.None:
		tab = "kline_w_n"
	default:
		panic("undefined reinstatement type:" + conf.Args.DataSource.KdjSource)
	}

	var qw []*model.Quote
	if offset < 0 || !mxw.Valid || mxw.Int64-offset-HIST_DATA_SIZE <= 0 {
		_, err := dbmap.Select(&qw, fmt.Sprintf("select * from %s where code = ? order by klid", tab), code)
		util.CheckErr(err, fmt.Sprintf("Failed to query %s for %s", tab, code))
	} else {
		_, err := dbmap.Select(&qw, fmt.Sprintf("select * from %s where code = ? and klid >= ? "+
			"order by klid", tab), code, mxw.Int64-HIST_DATA_SIZE-offset)
		util.CheckErr(err, fmt.Sprintf("Failed to query %s for %s", tab, code))
	}

	kdjw := indc.DeftKDJ(qw)

	binsIndc(kdjw, "indicator_w")

	if conf.Args.DataSource.SampleKdjFeature {
		SmpKdjFeat(code, model.WEEK, 5.0, 2.0, 2)
	}
}

func calcMonth(stk *model.Stock, offset int64) {
	var (
		mxm  sql.NullInt64
		err  error
		code = stk.Code
	)
	if offset >= 0 {
		mxm, err = dbmap.SelectNullInt("select max(klid) from indicator_m where code=?", code)
		util.CheckErr(err, "failed to query max klid in indicator_m for "+code)
	}

	tab := "kline_m"
	switch conf.Args.DataSource.KdjSource {
	case model.Backward:
		tab = "kline_m_b"
	case model.None:
		tab = "kline_m_n"
	default:
		panic("undefined reinstatement type:" + conf.Args.DataSource.KdjSource)
	}

	var qm []*model.Quote
	if offset < 0 || !mxm.Valid || mxm.Int64-offset-HIST_DATA_SIZE <= 0 {
		_, err := dbmap.Select(&qm, fmt.Sprintf("select * from %s where code = ? order by klid", tab), code)
		util.CheckErr(err, fmt.Sprintf("Failed to query %s for %s", tab, code))
	} else {
		_, err := dbmap.Select(&qm, fmt.Sprintf("select * from %s where code = ? and klid >= ? "+
			"order by klid", tab), code, mxm.Int64-HIST_DATA_SIZE-offset)
		util.CheckErr(err, fmt.Sprintf("Failed to query %s for %s", tab, code))
	}

	kdjm := indc.DeftKDJ(qm)

	binsIndc(kdjm, "indicator_m")

	if conf.Args.DataSource.SampleKdjFeature {
		SmpKdjFeat(code, model.MONTH, 5.0, 2.0, 2)
	}
}

func calcDay(stk *model.Stock, offset int64) {
	var (
		mxd  sql.NullInt64
		err  error
		code = stk.Code
	)
	if offset >= 0 {
		mxd, err = dbmap.SelectNullInt("select max(klid) from indicator_d where code=?", code)
		util.CheckErr(err, "failed to query max klid in indicator_d for "+code)
	}

	tab := "kline_d"
	switch conf.Args.DataSource.KdjSource {
	case model.Backward:
		tab = "kline_d_b"
	case model.None:
		tab = "kline_d_n"
	default:
		panic("undefined reinstatement type:" + conf.Args.DataSource.KdjSource)
	}

	var qd []*model.Quote
	if offset < 0 || !mxd.Valid || mxd.Int64-offset-HIST_DATA_SIZE <= 0 {
		_, err := dbmap.Select(&qd, fmt.Sprintf("select code,date,klid,open,high,close,low,volume,amount,xrate from "+
			"%s where code = ? order by klid", tab), code)
		util.CheckErr(err, fmt.Sprintf("Failed to query %s for %s", tab, code))
	} else {
		_, err := dbmap.Select(&qd, fmt.Sprintf("select code,date,klid,open,high,close,low,volume,amount,xrate from "+
			"%s where code = ? and klid >= ? order by klid", tab), code, mxd.Int64-HIST_DATA_SIZE-offset)
		util.CheckErr(err, fmt.Sprintf("Failed to query %s for %s", tab, code))
	}

	kdjd := indc.DeftKDJ(qd)

	binsIndc(kdjd, "indicator_d")

	if conf.Args.DataSource.SampleKdjFeature {
		SmpKdjFeat(code, model.DAY, 5.0, 2.0, 2)
	}
}

func binsIndc(indc []*model.Indicator, table string) (c int) {
	//FIXME: avoid mysql deadlock issue
	if len(indc) > 0 {
		valueStrings := make([]string, 0, len(indc))
		valueArgs := make([]interface{}, 0, len(indc)*8)
		var code string
		for _, i := range indc {
			d, t := util.TimeStr()
			i.Udate.Valid = true
			i.Utime.Valid = true
			i.Udate.String = d
			i.Utime.String = t
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, i.Code)
			valueArgs = append(valueArgs, i.Date)
			valueArgs = append(valueArgs, i.Klid)
			valueArgs = append(valueArgs, i.KDJ_K)
			valueArgs = append(valueArgs, i.KDJ_D)
			valueArgs = append(valueArgs, i.KDJ_J)
			valueArgs = append(valueArgs, i.Udate)
			valueArgs = append(valueArgs, i.Utime)
			code = i.Code
		}
		sklid := indc[0].Klid
		if len(indc) > 5 {
			sklid = indc[len(indc)-5].Klid
		}
		stmt := fmt.Sprintf("delete from %s where code = ? and klid >= ?", table)
		_, e := dbmap.Exec(stmt, code, sklid)
		if e != nil {
			log.Panicf("%s failed to delete stale %s data\n%+v", code, table, e)
		}
		stmt = fmt.Sprintf("INSERT INTO %s (code,date,klid,kdj_k,kdj_d,kdj_j,udate,utime) VALUES %s on "+
			"duplicate key update date=values(date),kdj_k=values(kdj_k),kdj_d=values(kdj_d),kdj_j=values"+
			"(kdj_j),udate=values(udate),utime=values(utime)",
			table, strings.Join(valueStrings, ","))
		_, e = dbmap.Exec(stmt, valueArgs...)
		if e != nil {
			log.Panicf("%s failed to overwrite %s\n%+v", code, table, e)
		}
		c = len(indc)
	}
	return
}
