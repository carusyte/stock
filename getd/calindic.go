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
)

const HIST_DATA_SIZE = 200
const JOB_CAPACITY = global.JOB_CAPACITY
const MAX_CONCURRENCY = global.MAX_CONCURRENCY

var(
	dbmap = global.Dbmap
	dot = global.Dot
)

func CalcIndics(stocks []*model.Stock) {
	log.Println("calculating indices...")
	var wg sync.WaitGroup
	chstk := make(chan *model.Stock, JOB_CAPACITY)
	for i := 0; i < MAX_CONCURRENCY; i++ {
		wg.Add(1)
		go doCalcIndices(chstk, &wg)
	}
	for _, s := range stocks {
		chstk <- s
	}
	close(chstk)
	wg.Wait()
}

func doCalcIndices(chstk chan *model.Stock, wg *sync.WaitGroup) {
	defer wg.Done()
	for stock := range chstk {
		code := stock.Code
		calcDay(code, 3)
		calcWeek(code, 2)
		calcMonth(code, 2)
	}
}

func calcWeek(code string, offset int64) {
	mxw, err := dbmap.SelectNullInt("select max(klid) from indicator_w where code=?", code)
	util.CheckErr(err, "failed to query max klid in indicator_w for "+code)
	mxk, err := dbmap.SelectNullInt("select max(klid) from kline_w where code=?", code)
	util.CheckErr(err, "failed to query max klid in kline_w for "+code)
	if !mxk.Valid || (mxw.Valid && mxk.Int64 < mxw.Int64) {
		//no new kline_w data yet
		return
	}

	var qw []*model.Quote
	if !mxw.Valid || mxw.Int64-offset-HIST_DATA_SIZE <= 0 {
		_, err := dbmap.Select(&qw, "select * from kline_w where code = ? order by klid", code)
		util.CheckErr(err, "Failed to query kline_w for "+code)
	} else {
		_, err := dbmap.Select(&qw, "select * from kline_w where code = ? and klid >= ? "+
			"order by klid", code, mxw.Int64-HIST_DATA_SIZE-offset)
		util.CheckErr(err, "Failed to query kline_w for "+code)
	}

	kdjw := indc.DeftKDJ(qw)
	if len(qw) > HIST_DATA_SIZE {
		kdjw = kdjw[HIST_DATA_SIZE+1:]
	}

	binsIndc(kdjw, "indicator_w")
}

func calcMonth(code string, offset int64) {
	mxm, err := dbmap.SelectNullInt("select max(klid) from indicator_m where code=?", code)
	util.CheckErr(err, "failed to query max klid in indicator_m for "+code)
	mxk, err := dbmap.SelectNullInt("select max(klid) from kline_m where code=?", code)
	util.CheckErr(err, "failed to query max klid in kline_m for "+code)
	if !mxk.Valid || (mxm.Valid && mxk.Int64 < mxm.Int64) {
		//no new kline_d data yet
		return
	}

	var qm []*model.Quote
	if !mxm.Valid || mxm.Int64-offset-HIST_DATA_SIZE <= 0 {
		_, err := dbmap.Select(&qm, "select * from kline_m where code = ? order by klid", code)
		util.CheckErr(err, "Failed to query kline_m for "+code)
	} else {
		_, err := dbmap.Select(&qm, "select * from kline_m where code = ? and klid >= ? "+
			"order by klid", code, mxm.Int64-HIST_DATA_SIZE-offset)
		util.CheckErr(err, "Failed to query kline_m for "+code)
	}

	kdjm := indc.DeftKDJ(qm)
	if len(qm) > HIST_DATA_SIZE {
		kdjm = kdjm[HIST_DATA_SIZE+1:]
	}

	binsIndc(kdjm, "indicator_m")
}

func calcDay(code string, offset int64) {
	mxd, err := dbmap.SelectNullInt("select max(klid) from indicator_d where code=?", code)
	util.CheckErr(err, "failed to query max klid in indicator_d for "+code)
	mxk, err := dbmap.SelectNullInt("select max(klid) from kline_d where code=?", code)
	util.CheckErr(err, "failed to query max klid in kline_d for "+code)
	if !mxk.Valid || (mxd.Valid && mxk.Int64 < mxd.Int64) {
		//no new kline_d data yet
		return
	}

	var qd []*model.Quote
	if !mxd.Valid || mxd.Int64-offset-HIST_DATA_SIZE <= 0 {
		_, err := dbmap.Select(&qd, "select code,date,klid,open,high,close,low,volume,amount,xrate from "+
			"kline_d where code = ? order by klid", code)
		util.CheckErr(err, "Failed to query kline_d for "+code)
	} else {
		_, err := dbmap.Select(&qd, "select code,date,klid,open,high,close,low,volume,amount,xrate from "+
			"kline_d where code = ? and klid >= ? order by klid", code, mxd.Int64-HIST_DATA_SIZE-offset)
		util.CheckErr(err, "Failed to query kline_d for "+code)
	}

	kdjd := indc.DeftKDJ(qd)
	if len(qd) > HIST_DATA_SIZE {
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
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, i.Code)
			valueArgs = append(valueArgs, i.Date)
			valueArgs = append(valueArgs, i.Klid)
			valueArgs = append(valueArgs, i.KDJ_K)
			valueArgs = append(valueArgs, i.KDJ_D)
			valueArgs = append(valueArgs, i.KDJ_J)
			code = i.Code
		}
		stmt := fmt.Sprintf("INSERT INTO %s (code,date,klid,kdj_k,kdj_d,kdj_j) VALUES %s on "+
			"duplicate key update date=values(date),kdj_k=values(kdj_k),kdj_d=values(kdj_d),kdj_j=values"+
			"(kdj_j)",
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
