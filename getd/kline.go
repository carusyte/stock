package main

import (
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
	wf := make(chan int, 50)
	for _, stk := range stks {
		wg.Add(1)
		wf <- 1
		go getKline(stk, &wg, &wf)
	}
	wg.Wait()
	log.Printf("kline data updated.")
}

// Fetch all period klines with offset, Nil will be return if there's no such record.
func getAllKlines(code string, offDy, offWk, offMn) ([]*model.Quote, *model.Quote, *model.Quote) {
	mxw, mxm := getMaxDates(code)
	var klines []*model.Kline
	_, err := dbmap.Select(&klines, "select * from kline_d where code = ? order by date", code)
	checkErr(err, "Failed to query kline_d for "+code)
	return klines, mxw, mxm
}

func getKline(stk *model.Stock, wg *sync.WaitGroup, wf *chan int) {
	defer func(){
		wg.Done()
		<- *wf
	}()
	// Get latest kline from db, for day, week, month, with offset
	ldy, lwk, lmn := getLatestKl(stk.Code, 3, 2, 2)
	//get daily kline
	kldy := getDailyKlines(stk.Code, ldy)
	//get weekly kline
	klwk := getLongKlines(stk.Code, lwk, "http://d.10jqka.com.cn/v2/line/hs_%s/11/last.js", "kline_w")
	//get monthly kline
	klmn := getLongKlines(stk.Code, lmn, "http://d.10jqka.com.cn/v2/line/hs_%s/21/last.js", "kline_m")
	log.Printf("%s klines fetched: %d, %d, %d", stk.Code, len(kldy), len(klwk), len(klmn))
}

func getDailyKlines(code string, ldy *model.Quote) (kldy []*model.Quote) {
	//get today kline data
	url_today := fmt.Sprintf("http://d.10jqka.com.cn/v2/line/hs_%s/01/today.js", code)
	body, e := HttpGetBytes(url_today)
	if e!=nil{
		return
	}
	ktoday := model.Ktoday{}
	jsonErr := json.Unmarshal(strip(body), &ktoday)
	if jsonErr != nil {
		log.Printf("failed to parse kline json for %s: %+v\n%s", code, jsonErr, string(body))
	}
	kldy = append(kldy, &ktoday.Quote)

	//get last kline data
	url_last := fmt.Sprintf("http://d.10jqka.com.cn/v2/line/hs_%s/01/last.js", code)
	body,e = HttpGetBytes(url_last)
	if e!=nil{
		return
	}
	klast := model.Klast{}
	jsonErr = json.Unmarshal(strip(body), &klast)
	if jsonErr != nil {
		log.Printf("failed to parse kline json for %s: %+v", code, jsonErr)
	}

	ldate := ""
	lklid := 0
	if ldy != nil {
		ldate = ldy.Date
		lklid = ldy.Klid
	}

	if klast.Data == "" {
		log.Printf("%s last data may not be ready yet", code)
		return
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
			log.Printf("failed to parse year for %+v, error: %+v", code, e)
			break
		}
		yr--
		start, e := strconv.ParseInt(klast.Start[:4], 10, 32)
		if e != nil {
			log.Printf("failed to parse json start year for %+v, string:%s, error: %+v", code, klast.Start, e)
			break
		}
		if yr < start {
			break
		}
		url_hist := fmt.Sprintf("http://d.10jqka.com.cn/v2/line/hs_%s/01/%d.js", code, yr)
		body,e = HttpGetBytes(url_hist)
		if e!=nil{
			log.Printf("can't get hist daily quotes for %s, please try again later.", code)
			return kldy[len(kldy):]
		}
		khist := model.Khist{}
		jsonErr = json.Unmarshal(strip(body), &khist)
		if jsonErr != nil {
			log.Printf("failed to parse hist kline json for %s: %+v", code, jsonErr)
		}
		kls, more = parseKlines(code, khist.Data, ldate)
		if len(kls) > 0 {
			kldy = append(kldy, kls...)
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
	url = fmt.Sprintf(url, code)
	body, e := HttpGetBytes(url)
	if e!=nil{
		log.Printf("can't get long klines for %s, %s. please try again later.", code,table)
		return
	}
	khist := model.Khist{}
	jsonErr := json.Unmarshal(strip(body), &khist)
	if jsonErr != nil {
		log.Printf("failed to parse %s json for %s: %+v", table, code, jsonErr)
	}
	if khist.Data == "" {
		log.Printf("%s %s data may not be ready yet", code, table)
		return
	}
	kls, _ := parseKlines(code, khist.Data, ldate)
	if len(kls) > 0 {
		quotes = append(quotes, kls...)
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
	defer func(){
		if r := recover(); r != nil {
			if e,ok := r.(error);ok{
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
				kl.Open = util.Str2f64(e)
			case 2:
				kl.High = util.Str2f64(e)
			case 3:
				kl.Low = util.Str2f64(e)
			case 4:
				kl.Close = util.Str2f64(e)
			case 5:
				kl.Volume = util.Str2f64(e)
			case 6:
				kl.Amount = util.Str2f64(e)
			case 7:
				kl.Xrate = util.Str2fnull(e)
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