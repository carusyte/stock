package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"log"
	"strconv"
	"strings"
	"sync"
)

func GetKlines(stks []*model.Stock) {
	log.Println("begin to fetch kline data")
	var wg sync.WaitGroup
	wf := make(chan int, MAX_CONCURRENCY)
	for _, stk := range stks {
		wg.Add(1)
		wf <- 1
		go getKline(stk, &wg, &wf)
	}
	wg.Wait()
	log.Printf("all period kline data updated.")
}

// TODO Fetch all period klines with offset from db, Nil will be return if there's no such record.
//func getAllKlines(code string, offDy, offWk, offMn) ([]*model.Quote, *model.Quote, *model.Quote) {
//	mxw, mxm := getMaxDates(code)
//	var klines []*model.Kline
//	_, err := dbmap.Select(&klines, "select * from kline_d where code = ? order by date", code)
//	checkErr(err, "Failed to query kline_d for "+code)
//	return klines, mxw, mxm
//}

func getKline(stk *model.Stock, wg *sync.WaitGroup, wf *chan int) {
	defer func() {
		wg.Done()
		<-*wf
	}()
	// Get latest kline from db, for day, week, month, with offset
	ldy, lwk, lmn := getLatestKl(stk.Code, 3, 2, 2)
	//get daily kline
	getDailyKlines(stk.Code, ldy)
	//get weekly kline
	getLongKlines(stk.Code, lwk, "http://d.10jqka.com.cn/v2/line/hs_%s/11/last.js", "kline_w")
	//get monthly kline
	getLongKlines(stk.Code, lmn, "http://d.10jqka.com.cn/v2/line/hs_%s/21/last.js", "kline_m")
}

func getDailyKlines(code string, ldy *model.Quote) (kldy []*model.Quote) {
	//get today kline data
	RETRIES := 5
	var (
		klast  model.Klast
		ktoday model.Ktoday
		ldate  string
		lklid  int
		body   []byte
		e      error
	)
RETRY:
	for rt := 0; rt < RETRIES; rt++ {
		url_today := fmt.Sprintf("http://d.10jqka.com.cn/v2/line/hs_%s/01/today.js", code)
		body, e = util.HttpGetBytes(url_today)
		if e != nil {
			log.Printf("stop retrying to get today kline for %s", code)
			return
		}
		ktoday = model.Ktoday{}
		e = json.Unmarshal(strip(body), &ktoday)
		if e != nil {
			if rt < RETRIES {
				log.Printf("retrying to parse kline json for %s [%d]: %+v\n%s", code, rt+1, e,
					string(body))
				continue
			} else {
				log.Printf("stop retrying to parse kline json for %s [%d]: %+v\n%s", code, rt+1,
					e, string(body))
				return
			}
		}
		kldy = append(kldy, &ktoday.Quote)

		//get last kline data
		url_last := fmt.Sprintf("http://d.10jqka.com.cn/v2/line/hs_%s/01/last.js", code)
		body, e = util.HttpGetBytes(url_last)
		if e != nil {
			log.Printf("stop retrying to get last kline for %s", code)
			return []*model.Quote{}
		}
		klast = model.Klast{}
		e = json.Unmarshal(strip(body), &klast)
		if e != nil {
			if rt < RETRIES {
				log.Printf("retrying to parse last kline json for %s [%d]: %+v\n%s", code, rt+1, e,
					string(body))
				continue
			} else {
				log.Printf("stop retrying to parse last kline json for %s [%d]: %+v\n%s", code, rt+1,
					e, string(body))
				return []*model.Quote{}
			}
		}

		if klast.Data == "" {
			log.Printf("%s last data may not be ready yet", code)
			return
		}

		ldate = ""
		lklid = 0
		if ldy != nil {
			ldate = ldy.Date
			lklid = ldy.Klid
		}

		kls, more := parseKlines(code, klast.Data, ldate)
		if len(kls) > 0 {
			if ktoday.Date == kls[0].Date {
				kldy = append(kldy, kls[1:]...)
			} else {
				kldy = append(kldy, kls...)
			}
		}

		//get hist kline data
		for more {
			yr, e := strconv.ParseInt(kls[0].Date[:4], 10, 32)
			if e != nil {
				log.Printf("failed to parse year for %+v, stop processing. error: %+v", code, e)
				return []*model.Quote{}
			}
			yr--
			start, e := strconv.ParseInt(klast.Start[:4], 10, 32)
			if e != nil {
				log.Printf("failed to parse json start year for %+v, stop processing. string:%s, error: %+v",
					code, klast.Start, e)
				return []*model.Quote{}
			}
			if yr < start {
				break
			}
			url_hist := fmt.Sprintf("http://d.10jqka.com.cn/v2/line/hs_%s/01/%d.js", code, yr)
			body, e = util.HttpGetBytes(url_hist)
			if e != nil {
				if rt < RETRIES {
					log.Printf("retrying to get hist daily quotes for %s, %d [%d]: %+v", code, yr,
						rt+1, e)
					continue RETRY
				} else {
					log.Printf("stop retrying to get hist daily quotes for %s, %d [%d]: %+v",
						code, yr, rt+1, e)
					return []*model.Quote{}
				}
			}
			khist := model.Khist{}
			e = json.Unmarshal(strip(body), &khist)
			if e != nil {
				if rt < RETRIES {
					log.Printf("retrying to parse hist kline json for %s, %d [%d]: %+v", code,
						yr, rt+1, e)
					continue RETRY
				} else {
					log.Printf("stop retrying to parse hist kline json for %s, %d [%d]: %+v",
						code, yr, rt+1, e)
					return []*model.Quote{}
				}
			}
			kls, more = parseKlines(code, khist.Data, ldate)
			if len(kls) > 0 {
				kldy = append(kldy, kls...)
			}
		}
	}

	assignKlid(kldy, lklid)
	binsert(kldy, "kline_d")
	return
}

func getLongKlines(code string, latest *model.Quote, url string, table string) (quotes []*model.Quote) {
	ldate := ""
	lklid := 0
	if latest != nil {
		ldate = latest.Date
		lklid = latest.Klid
	}
	RETRIES := 5
	url = fmt.Sprintf(url, code)
	for rt := 0; rt < RETRIES; rt++ {
		body, e := util.HttpGetBytes(url)
		if e != nil {
			log.Printf("can't get %s for %s. please try again later.", table, code)
			return
		}
		khist := model.Khist{}
		e = json.Unmarshal(strip(body), &khist)
		if e != nil {
			if rt < RETRIES {
				log.Printf("retrying to parse %s json for %s, [%d]: %+v", table, code, rt+1, e)
				continue
			} else {
				log.Printf("stop retrying to parse %s json for %s, [%d]: %+v", table, code, rt+1, e)
				return
			}
		}
		if khist.Data == "" {
			log.Printf("%s %s data may not be ready yet", code, table)
			return
		}
		kls, _ := parseKlines(code, khist.Data, ldate)
		if len(kls) > 0 {
			quotes = append(quotes, kls...)
		}
	}
	assignKlid(quotes, lklid)
	binsert(quotes, table)
	return
}

func assignKlid(klines []*model.Quote, start int) {
	for i := len(klines) - 1; i >= 0; i-- {
		start++
		klines[i].Klid = start
	}
}

func binsert(quotes []*model.Quote, table string) (c int) {
	if len(quotes) > 0 {
		valueStrings := make([]string, 0, len(quotes))
		valueArgs := make([]interface{}, 0, len(quotes)*10)
		var code string
		for _, q := range quotes {
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, q.Code)
			valueArgs = append(valueArgs, q.Date)
			valueArgs = append(valueArgs, q.Klid)
			valueArgs = append(valueArgs, q.Open)
			valueArgs = append(valueArgs, q.High)
			valueArgs = append(valueArgs, q.Close)
			valueArgs = append(valueArgs, q.Low)
			valueArgs = append(valueArgs, q.Volume)
			valueArgs = append(valueArgs, q.Amount)
			valueArgs = append(valueArgs, q.Xrate)
			code = q.Code
		}
		stmt := fmt.Sprintf("INSERT INTO %s (code,date,klid,open,high,close,low,"+
			"volume,amount,xrate) VALUES %s on duplicate key update date=values(date),"+
			"open=values(open),high=values(high),close=values(close),low=values(low),"+
			"volume=values(volume),amount=values(amount),xrate=values(xrate)",
			table, strings.Join(valueStrings, ","))
		_, err := dbmap.Exec(stmt, valueArgs...)
		if !util.CheckErr(err, code+" failed to bulk insert "+table) {
			c = len(quotes)
		}
	}
	return
}

func parseKlines(code string, data string, ldate string) (kls []*model.Quote, more bool) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				log.Println(e)
			}
			log.Printf("data:\n%s", data)
			kls = []*model.Quote{}
			more = false
		}
	}()
	more = true
	dates := strings.Split(data, ";")
DATES:
	for i := len(dates) - 1; i >= 0; i-- {
		// latest in the last
		es := strings.Split(strings.TrimSpace(dates[i]), ",")
		kl := &model.Quote{}
		for j, e := range es {
			e := strings.TrimSpace(e)
			//20170505,27.68,27.99,27.55,27.98,27457397,763643920.00,0.249
			//date, open, high, low, close, volume, amount, exchange
			switch j {
			case 0:
				kl.Date = e[:4] + "-" + e[4:6] + "-" + e[6:]
				if ldate != "" && kl.Date <= ldate {
					more = false
					break DATES
				}
			case 1:
				kl.Open = util.Str2F64(e)
			case 2:
				kl.High = util.Str2F64(e)
			case 3:
				kl.Low = util.Str2F64(e)
			case 4:
				kl.Close = util.Str2F64(e)
			case 5:
				kl.Volume = util.Str2F64(e)
			case 6:
				kl.Amount = util.Str2F64(e)
			case 7:
				kl.Xrate = util.Str2Fnull(e)
			default:
				//skip
			}
		}
		kl.Code = code
		kls = append(kls, kl)
	}
	return
}

func getLatestKl(code string, offDy, offWk, offMn int) (ldy *model.Quote, lwk *model.Quote, lmn *model.Quote) {
	dbmap.SelectOne(&ldy, "select * from kline_d where code = ? order by date desc limit 1 offset ?",
		code, offDy)
	dbmap.SelectOne(&lwk, "select * from kline_w where code = ? order by date desc limit 1 offset ?",
		code, offWk)
	dbmap.SelectOne(&lmn, "select * from kline_m where code = ? order by date desc limit 1 offset ?",
		code, offMn)
	return
}

func strip(data []byte) []byte {
	s := bytes.IndexByte(data, 40)     // first occurrence of '('
	e := bytes.LastIndexByte(data, 41) // last occurrence of ')'
	if s >= 0 && e >= 0 {
		return data[s+1:e]
	} else {
		return data
	}
}
