package getd

import (
	"github.com/carusyte/stock/util"
	"github.com/carusyte/stock/conf"
	"sync"
	"log"
	"encoding/json"
	"fmt"
	"github.com/carusyte/stock/model"
	"github.com/pkg/errors"
	"time"
	"strings"
)

func GetIndices() {
	var (
		codes   []string
		wg, wgr sync.WaitGroup
	)
	_, e := dbmap.Select(&codes, `select code from idxlst`)
	util.CheckErr(e, "failed to query idxlst")
	log.Printf("# Indices: %d", len(codes))
	chs := make(chan string, conf.Args.Concurrency)
	rchs := make(chan string, conf.Args.Concurrency)
	wgr.Add(1)
	go func() {
		defer wgr.Done()
		rcodes := make([]string, 0, 16)
		for rc := range rchs {
			if rc != "" {
				rcodes = append(rcodes, rc)
				p := float64(len(rcodes)) / float64(len(codes))
				log.Printf("Progress: %d/%d, %.2f%%", len(rcodes), len(codes), p)
			}
		}
		eq, fs, _ := util.DiffStrings(codes, rcodes)
		if !eq {
			log.Printf("Failed indices: %+v", fs)
		}
	}()
	for _, c := range codes {
		wg.Add(1)
		chs <- c
		go doGetIndex(c, 3, &wg, chs, rchs)
	}
	wg.Wait()
	close(chs)
	close(rchs)
	wgr.Wait()
}

func doGetIndex(code string, retry int, wg *sync.WaitGroup, chs chan string, rchs chan string) {
	defer func() {
		wg.Done()
		<-chs
	}()
	ts := []model.DBTab{
		model.KLINE_DAY,
		model.KLINE_WEEK,
		model.KLINE_MONTH,
	}
	for _, t := range ts {
		e := getIndexFor(code, retry, t)
		if e != nil {
			rchs <- ""
			log.Println(e)
			return
		}
	}
	rchs <- code
}

func getIndexFor(code string, retry int, tab model.DBTab) error {
	for i := 0; i < retry; i++ {
		suc, rt := tryGetIndex(code, tab)
		if suc {
			return nil
		} else if rt {
			log.Printf("%s[%s] retrying: %d", code, tab, i+1)
		} else {
			return errors.Errorf("Failed to get %s[%s]", code, tab)
		}
	}
	return errors.Errorf("Failed to get %s[%s]", code, tab)
}

func tryGetIndex(code string, tab model.DBTab) (suc, rt bool) {
	log.Printf("Fetching index %s", code)
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
	saveIndex(xqj, sklid, string(tab))
	return true, false
}

func saveIndex(xqj *model.XQJson, sklid int, table string) {
	//TODO implement
	if xqj != nil && len(xqj.Chartlist) > 0 {
		valueStrings := make([]string, 0, len(xqj.Chartlist))
		valueArgs := make([]interface{}, 0, len(xqj.Chartlist)*13)
		var code string
		for _, q := range xqj.Chartlist {
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, round(?,3), ?, ?)")
			valueArgs = append(valueArgs, xqj.Stock)
			valueArgs = append(valueArgs,
				time.Unix(q.Timestamp/int64(time.Microsecond), 0).Format("2006-01-02"))
			valueArgs = append(valueArgs, sklid)
			valueArgs = append(valueArgs, q.Open)
			valueArgs = append(valueArgs, q.High)
			valueArgs = append(valueArgs, q.Close)
			valueArgs = append(valueArgs, q.Low)
			valueArgs = append(valueArgs, q.Volume)
			//valueArgs = append(valueArgs, q.Amount)
			//valueArgs = append(valueArgs, q.Xrate)
			//valueArgs = append(valueArgs, q.Varate)
			//valueArgs = append(valueArgs, q.Udate)
			//valueArgs = append(valueArgs, q.Utime)
			//code = q.Code
			sklid++
		}
		stmt := fmt.Sprintf("INSERT INTO %s (code,date,klid,open,high,close,low,"+
			"volume,amount,xrate,varate,udate,utime) VALUES %s on duplicate key update date=values(date),"+
			"open=values(open),high=values(high),close=values(close),low=values(low),"+
			"volume=values(volume),amount=values(amount),xrate=values(xrate),varate=values(varate),udate=values"+
			"(udate),utime=values(utime)",
			table, strings.Join(valueStrings, ","))
		_, err := dbmap.Exec(stmt, valueArgs...)
		util.CheckErr(err, code+" failed to bulk insert "+table)
	}
}
