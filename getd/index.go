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
)

func GetIndices() {
	var (
		idxlst  []*model.IdxLst
		wg, wgr sync.WaitGroup
	)
	_, e := dbmap.Select(&idxlst, `select * from idxlst`)
	util.CheckErr(e, "failed to query idxlst")
	log.Printf("# Indices: %d", len(idxlst))
	codes := make([]string, len(idxlst))
	for i, idx := range idxlst {
		codes[i] = idx.Code
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
				p := float64(len(rcodes)) / float64(len(idxlst))
				log.Printf("Progress: %d/%d, %.2f%%", len(rcodes), len(idxlst), p)
			}
		}
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
}

func doGetIndex(idx *model.IdxLst, retry int, wg *sync.WaitGroup, chidx chan *model.IdxLst, rchs chan string) {
	defer func() {
		wg.Done()
		<-chidx
	}()
	ts := []model.DBTab{
		model.KLINE_DAY,
		model.KLINE_WEEK,
		model.KLINE_MONTH,
	}
	for _, t := range ts {
		e := getIndexFor(idx, retry, t)
		if e != nil {
			rchs <- ""
			log.Println(e)
			return
		}
	}
	rchs <- idx.Code
}

func getIndexFor(idx *model.IdxLst, retry int, tab model.DBTab) error {
	for i := 0; i < retry; i++ {
		suc, rt := tryGetIndex(idx, tab)
		if suc {
			return nil
		} else if rt {
			log.Printf("%s[%s] retrying: %d", idx.Code, tab, i+1)
		} else {
			return errors.Errorf("Failed to get %s[%s]", idx.Code, tab)
		}
	}
	return errors.Errorf("Failed to get %s[%s]", idx.Code, tab)
}

func tryGetIndex(idx *model.IdxLst, tab model.DBTab) (suc, rt bool) {
	//TODO fetch index data according to various src url
	code := idx.Code
	log.Printf("Fetching index %s", code)
	switch idx.Src {
	case "https://xueqiu.com":
		return idxFromXq(code, tab)
	default:
		log.Panicf("%s unknown index src: %s", code, idx.Src)
	}
	panic(fmt.Sprintf("%s unknown index src: %s", code, idx.Src))
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
