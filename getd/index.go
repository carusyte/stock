package getd

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
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
			logrus.Warnf("no data in idxlst table")
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
	rchs := make(chan string, conf.Args.Concurrency)
	wgr.Add(1)
	go func() {
		defer wgr.Done()
		rcodes := make([]string, 0, 16)
		for rc := range rchs {
			if rc != "" {
				rcodes = append(rcodes, rc)
				p := float64(len(rcodes)) / float64(len(idxlst)) * 100
				log.Printf("Progress: %d/%d, %.2f%%", len(rcodes), len(idxlst), p)
			}
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
	for _, idx := range idxlst {
		wg.Add(1)
		chidx <- idx
		go doGetIndex(idx, 3, &wg, chidx, rchs)
	}
	wg.Wait()
	close(chidx)
	close(rchs)
	wgr.Wait()
	return
}

func doGetIndex(idx *model.IdxLst, retry int, wg *sync.WaitGroup, chidx chan *model.IdxLst, rchs chan string) {
	defer func() {
		wg.Done()
		<-chidx
	}()
	stk := &model.Stock{Code: idx.Code, Source: idx.Src}
	ts := []model.DBTab{
		model.KLINE_DAY,
		model.KLINE_WEEK,
		model.KLINE_MONTH,
	}
	if getKlineAndSave(stk, ts) {
		rchs <- idx.Code
	} else {
		rchs <- ""
	}
}

func idxFromQQ(code string, tab model.DBTab) (suc, rt bool) {
	var (
		ldate, per string
		sklid      = -1
	)
	// check history from db
	lq := getLatestKl(code, tab, 5+1) // plus one for varate calculation
	if lq != nil {
		sklid = lq.Klid
		ldate = lq.Date
	}
	switch tab {
	case model.KLINE_MONTH:
		per = "month"
	case model.KLINE_WEEK:
		per = "week"
	case model.KLINE_DAY:
		per = "day"
	default:
		panic("Unsupported period: " + tab)
	}
	url := fmt.Sprintf(`http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?`+
		`param=%[1]s,%[2]s,%[3]s,,87654,qfq`, code, per, ldate)
	d, e := util.HttpGetBytes(url)
	if e != nil {
		log.Printf("%s failed to get %s from %s\n%+v", code, tab, url, e)
		return false, true
	}
	qj := &model.QQJson{}
	qj.Code = code
	qj.Fcode = code
	qj.Period = per
	e = json.Unmarshal(d, qj)
	if e != nil {
		log.Printf("failed to parse json from %s\n%+v", url, e)
		return false, true
	}
	if len(qj.Quotes) > 0 && ldate != "" && qj.Quotes[0].Date != ldate {
		log.Printf("start date %s not matched database: %s", qj.Quotes[0], ldate)
		return false, true
	}
	supplementMisc(qj.Quotes, tab, sklid)
	CalLogReturns(qj.Quotes)
	if sklid != -1 {
		qj.Quotes = qj.Quotes[1:]
	}
	binsert(qj.Quotes, string(tab), sklid)
	return true, false
}

func idxFromXq(code string, tab model.DBTab) (suc, rt bool) {
	var (
		bg, per string
		sklid   int
	)
	// check history from db
	lq := getLatestKl(code, tab, 5)
	if lq != nil {
		tm, e := time.Parse("2006-01-02", lq.Date)
		util.CheckErr(e, fmt.Sprintf("%s[%s] failed to parse date", code, tab))
		bg = fmt.Sprintf("&begin=%d", tm.UnixNano()/int64(time.Millisecond))
		sklid = lq.Klid
	}
	switch tab {
	case model.KLINE_MONTH:
		per = "1month"
	case model.KLINE_WEEK:
		per = "1week"
	case model.KLINE_DAY:
		per = "1day"
	default:
		panic("Unsupported period: " + tab)
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
