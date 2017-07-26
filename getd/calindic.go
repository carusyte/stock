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
)

const HIST_DATA_SIZE = 200
const JOB_CAPACITY = global.JOB_CAPACITY
const MAX_CONCURRENCY = global.MAX_CONCURRENCY

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
	for i := 0; i < MAX_CONCURRENCY; i++ {
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
		calcDay(code, offd)
		calcWeek(code, offw)
		calcMonth(code, offm)
		chrstk <- stock
	}
}

func calcWeek(code string, offset int64) {
	var (
		mxw sql.NullInt64
		err error
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
	if mxw.Valid && len(qw) > HIST_DATA_SIZE {
		kdjw = kdjw[HIST_DATA_SIZE+1:]
	}

	binsIndc(kdjw, "indicator_w")
}

func calcMonth(code string, offset int64) {
	var (
		mxm sql.NullInt64
		err error
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
	if mxm.Valid && len(qm) > HIST_DATA_SIZE {
		kdjm = kdjm[HIST_DATA_SIZE+1:]
	}

	binsIndc(kdjm, "indicator_m")
}

func calcDay(code string, offset int64) {
	var (
		mxd sql.NullInt64
		err error
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
	if mxd.Valid && len(qd) > HIST_DATA_SIZE {
		kdjd = kdjd[HIST_DATA_SIZE+1:]
	}

	binsIndc(kdjd, "indicator_d")
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
		stmt := fmt.Sprintf("INSERT INTO %s (code,date,klid,kdj_k,kdj_d,kdj_j,udate,utime) VALUES %s on "+
			"duplicate key update date=values(date),kdj_k=values(kdj_k),kdj_d=values(kdj_d),kdj_j=values"+
			"(kdj_j),udate=values(udate),utime=values(utime)",
			table, strings.Join(valueStrings, ","))
		ps, err := dbmap.Prepare(stmt)
		defer ps.Close()
		util.CheckErrNop(err, code+" failed to prepare statement")
		_, err = ps.Exec(valueArgs...)
		if !util.CheckErr(err, code+" failed to bulk insert "+table) {
			c = len(indc)
		}
	}
	return
}
