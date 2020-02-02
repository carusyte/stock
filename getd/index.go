package getd

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"github.com/pkg/errors"
)

//GetIdxLst loads index data from database.
func GetIdxLst(code ...string) (idxlst []*model.IdxLst, e error) {
	sql := "select * from idxlst order by code"
	if len(code) > 0 {
		sql = fmt.Sprintf("select * from idxlst where code in (%s) order by code", util.Join(code,
			",", true))
	}
	_, e = dbmap.Select(&idxlst, sql)
	if e != nil {
		if "sql: no rows in result set" == e.Error() {
			log.Warnf("no data in idxlst table")
			return idxlst, nil
		}
		return idxlst, errors.Wrapf(e, "failed to query idxlst, sql: %s, \n%+v", sql, errors.WithStack(e))
	}
	return
}

//GetIndices fetches index data from configured source.
func GetIndices() (idxlst, suclst []*model.IdxLst) {
	var (
		wg, wgr sync.WaitGroup
	)
	_, e := dbmap.Select(&idxlst, `select * from idxlst`)
	util.CheckErr(e, "failed to query idxlst")
	log.Printf("# Indices: %d", len(idxlst))
	codes := make([]string, len(idxlst))
	idxMap := make(map[string]*model.IdxLst)
	for i, idx := range idxlst {
		codes[i] = idx.Code
		idxMap[idx.Code] = idx
	}
	chidx := make(chan *model.IdxLst, conf.Args.Concurrency)
	rchs := make(chan *model.Stock, conf.Args.Concurrency)
	wgr.Add(1)
	go func() {
		defer wgr.Done()
		rcodes := make([]string, 0, 16)
		for rc := range rchs {
			rcodes = append(rcodes, rc.Code)
			p := float64(len(rcodes)) / float64(len(idxlst)) * 100
			log.Printf("Progress: %d/%d, %.2f%%", len(rcodes), len(idxlst), p)
		}
		for _, sc := range rcodes {
			suclst = append(suclst, idxMap[sc])
		}
		log.Printf("Finished index data collecting")
		eq, fs, _ := util.DiffStrings(codes, rcodes)
		if !eq {
			log.Printf("Failed indices: %+v", fs)
		}
	}()
	chDbjob = createDbJobQueues(
		model.KLINE_DAY_F,
		model.KLINE_WEEK_F,
		model.KLINE_MONTH_F,
	)
	wgdb := saveQuotes(rchs)
	for _, idx := range idxlst {
		wg.Add(1)
		chidx <- idx
		go doGetIndex(idx, &wg, chidx)
	}
	wg.Wait()
	close(chidx)
	waitDbjob(wgdb)
	close(rchs)
	wgr.Wait()
	return
}

func doGetIndex(idx *model.IdxLst, wg *sync.WaitGroup, chidx chan *model.IdxLst) {
	defer func() {
		wg.Done()
		<-chidx
	}()
	stk := &model.Stock{Code: idx.Code, Source: idx.Src}
	ts := []model.DBTab{
		model.KLINE_DAY_F,
		model.KLINE_WEEK_F,
		model.KLINE_MONTH_F,
	}
	fetchRemoteKline(stk, ts)
}

// func idxFromQQ(code string, tab model.DBTab) (suc, rt bool) {
// 	var (
// 		ldate, per string
// 		sklid      = -1
// 		cycle      model.CYTP
// 	)
// 	switch tab {
// 	case model.KLINE_MONTH:
// 		per = "month"
// 		cycle = model.MONTH
// 	case model.KLINE_WEEK:
// 		per = "week"
// 		cycle = model.WEEK
// 	case model.KLINE_DAY:
// 		per = "day"
// 		cycle = model.DAY
// 	default:
// 		panic("Unsupported period: " + tab)
// 	}
// 	// check history from db
// 	lq := getLatestTradeDataBasic(code, cycle, model.Forward, 5+1) // plus one for varate calculation
// 	if lq != nil {
// 		sklid = lq.Klid
// 		ldate = lq.Date
// 	}

// 	url := fmt.Sprintf(`http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?`+
// 		`param=%[1]s,%[2]s,%[3]s,,87654,qfq`, code, per, ldate)
// 	d, e := util.HttpGetBytes(url)
// 	if e != nil {
// 		log.Printf("%s failed to get %s from %s\n%+v", code, tab, url, e)
// 		return false, true
// 	}
// 	qj := &model.QQJson{}
// 	qj.Code = code
// 	qj.Fcode = code
// 	qj.Period = per
// 	e = json.Unmarshal(d, qj)
// 	if e != nil {
// 		log.Printf("failed to parse json from %s\n%+v", url, e)
// 		return false, true
// 	}
// 	if len(qj.Quotes) > 0 && ldate != "" && qj.Quotes[0].Date != ldate {
// 		log.Printf("start date %s not matched database: %s", qj.Quotes[0], ldate)
// 		return false, true
// 	}
// 	supplementMisc(qj.Quotes, tab, sklid)
// 	CalLogReturns(qj.Quotes)
// 	if sklid != -1 {
// 		qj.Quotes = qj.Quotes[1:]
// 	}
// 	binsert(qj.Quotes, string(tab), sklid)
// 	return true, false
// }

func idxFromXq(code string, tab model.DBTab) (suc, rt bool) {
	var (
		bg, per string
		sklid   int
		cycle   model.CYTP
	)
	switch tab {
	case model.KLINE_MONTH_F:
		per = "1month"
		cycle = model.MONTH
	case model.KLINE_WEEK_F:
		per = "1week"
		cycle = model.WEEK
	case model.KLINE_DAY_F:
		per = "1day"
		cycle = model.DAY
	default:
		panic("Unsupported period: " + tab)
	}
	// check history from db
	lq := getLatestTradeDataBasic(code, model.KlineMaster, cycle, model.Forward, 5)
	if lq != nil {
		tm, e := time.Parse(global.DateFormat, lq.Date)
		util.CheckErr(e, fmt.Sprintf("%s[%s] failed to parse date", code, tab))
		bg = fmt.Sprintf("&begin=%d", tm.UnixNano()/int64(time.Millisecond))
		sklid = lq.Klid
	}
	url := fmt.Sprintf(`https://xueqiu.com/stock/forchartk/stocklist.json?`+
		`symbol=%s&period=%s&type=normal%s`, code, per, bg)
	d, e := util.HttpGetBytes(url)
	if e != nil {
		log.Printf("%s failed to get %s\n%+v", code, tab, e)
		return false, true
	}
	xqj := &model.XQJson{}
	e = json.Unmarshal(d, xqj)
	if e != nil {
		log.Printf("failed to parse json from %s\n%+v", url, e)
		return false, true
	}
	if xqj.Success != "true" {
		log.Printf("target server failed: %s\n%+v\n%+v", url, xqj, e)
		return false, true
	}
	xqj.Save(dbmap, sklid, string(tab))
	//saveIndex(xqj, sklid, string(tab))
	return true, false
}
