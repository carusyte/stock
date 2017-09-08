package getd

import (
	"fmt"
	"github.com/carusyte/stock/indc"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"log"
	"strings"
	"sync"
	"github.com/carusyte/stock/global"
	"database/sql"
	"runtime"
)

const (
	HIST_DATA_SIZE    = 200
	JOB_CAPACITY      = global.JOB_CAPACITY
	MAX_CONCURRENCY   = global.MAX_CONCURRENCY
	KDJ_FD_PRUNE_PREC = 0.99
	KDJ_FD_PRUNE_PASS = 3
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
		var offd, offw, offm int64 = 3, 2, 2
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
		mxk, err := dbmap.SelectNullInt("select max(klid) from kline_w where code=?", code)
		util.CheckErr(err, "failed to query max klid in kline_w for "+code)
		if !mxk.Valid || (mxw.Valid && mxk.Int64 < mxw.Int64) {
			//no new kline_w data yet
			return
		}
	}

	var qw []*model.Quote
	if offset < 0 || !mxw.Valid || mxw.Int64-offset-HIST_DATA_SIZE <= 0 {
		_, err := dbmap.Select(&qw, "select * from kline_w where code = ? order by klid", code)
		util.CheckErr(err, "Failed to query kline_w for "+code)
	} else {
		_, err := dbmap.Select(&qw, "select * from kline_w where code = ? and klid >= ? "+
			"order by klid", code, mxw.Int64-HIST_DATA_SIZE-offset)
		util.CheckErr(err, "Failed to query kline_w for "+code)
	}

	kdjw := indc.DeftKDJ(qw)

	binsIndc(kdjw, "indicator_w")

	SmpKdjFeat(code, model.WEEK, 5.0, 2.0, 2)
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
		mxk, err := dbmap.SelectNullInt("select max(klid) from kline_m where code=?", code)
		util.CheckErr(err, "failed to query max klid in kline_m for "+code)
		if !mxk.Valid || (mxm.Valid && mxk.Int64 < mxm.Int64) {
			//no new kline_d data yet
			return
		}
	}

	var qm []*model.Quote
	if offset < 0 || !mxm.Valid || mxm.Int64-offset-HIST_DATA_SIZE <= 0 {
		_, err := dbmap.Select(&qm, "select * from kline_m where code = ? order by klid", code)
		util.CheckErr(err, "Failed to query kline_m for "+code)
	} else {
		_, err := dbmap.Select(&qm, "select * from kline_m where code = ? and klid >= ? "+
			"order by klid", code, mxm.Int64-HIST_DATA_SIZE-offset)
		util.CheckErr(err, "Failed to query kline_m for "+code)
	}

	kdjm := indc.DeftKDJ(qm)

	binsIndc(kdjm, "indicator_m")

	SmpKdjFeat(code, model.MONTH, 5.0, 2.0, 2)
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
		mxk, err := dbmap.SelectNullInt("select max(klid) from kline_d where code=?", code)
		util.CheckErr(err, "failed to query max klid in kline_d for "+code)
		if !mxk.Valid || (mxd.Valid && mxk.Int64 < mxd.Int64) {
			//no new kline_d data yet
			return
		}
	}

	var qd []*model.Quote
	if offset < 0 || !mxd.Valid || mxd.Int64-offset-HIST_DATA_SIZE <= 0 {
		_, err := dbmap.Select(&qd, "select code,date,klid,open,high,close,low,volume,amount,xrate from "+
			"kline_d where code = ? order by klid", code)
		util.CheckErr(err, "Failed to query kline_d for "+code)
	} else {
		_, err := dbmap.Select(&qd, "select code,date,klid,open,high,close,low,volume,amount,xrate from "+
			"kline_d where code = ? and klid >= ? order by klid", code, mxd.Int64-HIST_DATA_SIZE-offset)
		util.CheckErr(err, "Failed to query kline_d for "+code)
	}

	kdjd := indc.DeftKDJ(qd)

	binsIndc(kdjd, "indicator_d")

	SmpKdjFeat(code, model.DAY, 5.0, 2.0, 2)
}

func binsIndc(indc []*model.Indicator, table string) (c int) {
	if len(indc) > 0 {
		valueStrings := make([]string, 0, len(indc))
		valueArgs := make([]interface{}, 0, len(indc)*6)
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
		tran, e := dbmap.Begin()
		if e != nil {
			log.Panicf("%s failed to start transaction\n%+v", code, e)
		}
		sklid := 0
		if len(indc) > 5 {
			sklid = indc[len(indc)-5].Klid
		} else {
			sklid = indc[0].Klid
		}
		stmt := fmt.Sprintf("delete from %s where code = ? and klid >= ?", table)
		_, e = tran.Exec(stmt, code, sklid)
		if e != nil {
			tran.Rollback()
			log.Panicf("%s failed to delete stale %s data", code, table)
		}
		stmt = fmt.Sprintf("INSERT INTO %s (code,date,klid,kdj_k,kdj_d,kdj_j,udate,utime) VALUES %s on "+
			"duplicate key update date=values(date),kdj_k=values(kdj_k),kdj_d=values(kdj_d),kdj_j=values"+
			"(kdj_j),udate=values(udate),utime=values(utime)",
			table, strings.Join(valueStrings, ","))
		_, e = tran.Exec(stmt, valueArgs...)
		if e != nil {
			tran.Rollback()
			log.Panicf("%s failed to overwrite %s", code, table)
		}
		c = len(indc)
		tran.Commit()
	}
	return
}
