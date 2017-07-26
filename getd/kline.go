package getd

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
	"math"
)

//Get various types of kline data for the given stocks. Returns the stocks that have been successfully processed.
func GetKlines(stks *model.Stocks, kltype ... model.DBTab) (rstks *model.Stocks) {
	//TODO find a way to get minute level klines
	log.Printf("begin to fetch kline data: %+v", kltype)
	var wg sync.WaitGroup
	wf := make(chan int, MAX_CONCURRENCY)
	outstks := make(chan *model.Stock, JOB_CAPACITY)
	rstks = new(model.Stocks)
	wgr := collect(rstks, outstks)
	for _, stk := range stks.List {
		wg.Add(1)
		wf <- 1
		go getKline(stk, kltype, &wg, &wf, outstks)
	}
	wg.Wait()
	close(wf)
	close(outstks)
	wgr.Wait()
	log.Printf("%d stocks %s data updated.", rstks.Size(), strings.Join(kt2strs(kltype), ", "))
	if stks.Size() != rstks.Size() {
		same, skp := stks.Diff(rstks)
		if !same {
			log.Printf("Failed: %+v", skp)
		}
	}
	return
}

func GetKlineDb(code string, tab model.DBTab, limit int, desc bool) (hist []*model.Quote) {
	if limit <= 0 {
		sql := fmt.Sprintf("select * from %s where code = ? order by klid", tab)
		if desc {
			sql += " desc"
		}
		_, e := dbmap.Select(&hist, sql, code)
		util.CheckErr(e, "failed to query "+string(tab)+" for "+code)
	} else {
		d := ""
		if desc {
			d = "desc"
		}
		sql := fmt.Sprintf("select * from %s where code = ? order by klid %s limit ?", tab, d)
		_, e := dbmap.Select(&hist, sql, code, limit)
		util.CheckErr(e, "failed to query "+string(tab)+" for "+code)
	}
	return
}

//convert slice of KLType to slice of string
func kt2strs(kltype []model.DBTab) (s []string) {
	s = make([]string, len(kltype))
	for i, e := range kltype {
		s[i] = string(e)
	}
	return
}

func getKline(stk *model.Stock, kltype []model.DBTab, wg *sync.WaitGroup, wf *chan int, outstks chan *model.Stock) {
	defer func() {
		wg.Done()
		<-*wf
	}()
	xdxr := latestUFRXdxr(stk.Code)
	suc := false
	for _, t := range kltype {
		switch t {
		case model.KLINE_DAY:
			_, suc = getDailyKlines(stk.Code, t, xdxr == nil)
		case model.KLINE_DAY_NR:
			_, suc = getDailyKlines(stk.Code, t, true)
		case model.KLINE_WEEK, model.KLINE_MONTH:
			_, suc = getLongKlines(stk.Code, t, xdxr == nil)
		default:
			log.Panicf("unhandled kltype: %s", t)
		}
	}
	if suc {
		outstks <- stk
	}
}

func getDailyKlines(code string, klt model.DBTab, incr bool) (kldy []*model.Quote, suc bool) {
	RETRIES := 5
	var (
		klast  model.Klast
		ktoday model.Ktoday
		ldate  string
		lklid  int
		body   []byte
		e      error
		mode   string
	)
	//mode:
	// 00-no reinstatement
	// 01-forward reinstatement
	// 02-backward reinstatement
	switch klt {
	case model.KLINE_DAY:
		mode = "01"
	case model.KLINE_DAY_NR:
		mode = "00"
	default:
		log.Panicf("unhandled kltype: %s", klt)
	}
RETRY:
	for rt := 0; rt < RETRIES; rt++ {
		url_today := fmt.Sprintf("http://d.10jqka.com.cn/v2/line/hs_%s/%s/today.js", code, mode)
		body, e = util.HttpGetBytes(url_today)
		if e != nil {
			log.Printf("stop retrying to get today kline for %s", code)
			return
		}
		ktoday = model.Ktoday{}
		e = json.Unmarshal(strip(body), &ktoday)
		if e != nil {
			if rt+1 < RETRIES {
				log.Printf("retrying to parse kline json for %s [%d]: %+v\n%s", code, rt+1, e,
					string(body))
				continue
			} else {
				log.Printf("stop retrying to parse kline json for %s [%d]: %+v\n%s", code, rt+1,
					e, string(body))
				return
			}
		}
		if ktoday.Code != "" {
			kldy = append(kldy, &ktoday.Quote)
		} else {
			log.Printf("kline today skipped: %s", url_today)
		}

		//get last kline data
		url_last := fmt.Sprintf("http://d.10jqka.com.cn/v2/line/hs_%s/%s/last.js", code, mode)
		body, e = util.HttpGetBytes(url_last)
		if e != nil {
			log.Printf("stop retrying to get last kline for %s", code)
			return []*model.Quote{}, false
		}
		klast = model.Klast{}
		e = json.Unmarshal(strip(body), &klast)
		if e != nil {
			if rt+1 < RETRIES {
				log.Printf("retrying to parse last kline json for %s [%d]: %+v\n%s\n%s", code, rt+1, e,
					url_last, string(body))
				continue
			} else {
				log.Printf("stop retrying to parse last kline json for %s [%d]: %+v\n%s\n%s", code,
					rt+1, e, url_last, string(body))
				return []*model.Quote{}, false
			}
		}

		if klast.Data == "" {
			if rt+1 < RETRIES {
				log.Printf("retrying to parse last kline json for %s [%d]: %+v\n%s\n%s", code, rt+1, e,
					url_last, string(body))
				continue
			} else {
				log.Printf("%s last data may not be ready yet, please check with the web site. retried:%d"+
					"\n%s\n%s", code, rt+1, url_last, string(body))
				return []*model.Quote{}, false
			}
		}

		ldate = ""
		lklid = -1
		if incr {
			ldy := getLatestKl(code, klt, 3)
			if ldy != nil {
				ldate = ldy.Date
				lklid = ldy.Klid
			} else {
				log.Printf("%s latest kline data not found, will be fully refreshed", code)
			}
		} else {
			log.Printf("%s kline data will be fully refreshed", code)
		}

		kls, more := parseKlines(code, klast.Data, ldate, "")
		if len(kls) > 0 {
			if ktoday.Date == kls[0].Date {
				kldy = append(kldy, kls[1:]...)
			} else {
				kldy = append(kldy, kls...)
			}
		} else {
			break
		}
		if more {
			//get hist kline data
			yr, e := strconv.ParseInt(kls[0].Date[:4], 10, 32)
			if e != nil {
				log.Printf("failed to parse year for %+v, stop processing. error: %+v",
					code, e)
				return []*model.Quote{}, false
			}
			start, e := strconv.ParseInt(klast.Start[:4], 10, 32)
			if e != nil {
				log.Printf("failed to parse json start year for %+v, stop processing. "+
					"string:%s, error: %+v", code, klast.Start, e)
				return []*model.Quote{}, false
			}
			for more {
				yr--
				if yr < start {
					break
				}
				// test if yr is in klast.Year map
				if _, in := klast.Year[strconv.FormatInt(yr, 10)]; !in {
					continue
				}
				url_hist := fmt.Sprintf("http://d.10jqka.com.cn/v2/line/hs_%s/%s/%d.js", code, mode,
					yr)
				body, e = util.HttpGetBytes(url_hist)
				if e != nil {
					if rt+1 < RETRIES {
						log.Printf("retrying to get hist daily quotes for %s, %d [%d]: %+v",
							code, yr, rt+1, e)
						continue RETRY
					} else {
						log.Printf("stop retrying to get hist daily quotes for %s, %d [%d]: %+v",
							code, yr, rt+1, e)
						return []*model.Quote{}, false
					}
				}
				khist := model.Khist{}
				e = json.Unmarshal(strip(body), &khist)
				if e != nil {
					if rt+1 < RETRIES {
						log.Printf("retrying to parse hist kline json for %s, %d [%d]: %+v", code,
							yr, rt+1, e)
						continue RETRY
					} else {
						log.Printf("stop retrying to parse hist kline json for %s, %d [%d]: %+v",
							code, yr, rt+1, e)
						return []*model.Quote{}, false
					}
				}
				kls, more = parseKlines(code, khist.Data, ldate, kldy[len(kldy)-1].Date)
				if len(kls) > 0 {
					kldy = append(kldy, kls...)
				}
			}
		}
		break
	}
	supplementMisc(kldy, lklid)
	if ldate != "" {
		//skip last record which is for varate calculation
		kldy = kldy[:len(kldy)-1]
	}
	binsert(kldy, string(klt))
	return kldy, true
}

func getToday(code string, typ string) (q *model.Quote, ok, retry bool) {
	url_today := fmt.Sprintf("http://d.10jqka.com.cn/v2/line/hs_%s/%s/today.js", code, typ)
	body, e := util.HttpGetBytes(url_today)
	if e != nil {
		return nil, false, false
	}

	ktoday := &model.Ktoday{}
	e = json.Unmarshal(strip(body), ktoday)
	if e != nil {
		return nil, false, true
	}
	return &ktoday.Quote, true, false
}

func getLongKlines(code string, klt model.DBTab, incr bool) (quotes []*model.Quote, suc bool) {
	urlt := "http://d.10jqka.com.cn/v2/line/hs_%s/%s/last.js"
	var typ string
	switch klt {
	case model.KLINE_WEEK:
		typ = "11"
	case model.KLINE_MONTH:
		typ = "21"
	default:
		log.Panicf("unhandled kltype: %s", klt)
	}
	ldate := ""
	lklid := -1
	if incr {
		latest := getLatestKl(code, klt, 2)
		if latest != nil {
			ldate = latest.Date
			lklid = latest.Klid
		} else {
			log.Printf("%s latest kline data not found, will be fully refreshed", code)
		}
	} else {
		log.Printf("%s kline data will be fully refreshed", code)
	}
	RETRIES := 5
	url := fmt.Sprintf(urlt, code, typ)
	for rt := 0; rt < RETRIES; rt++ {
		ktoday, ok, retry := getToday(code, typ)
		if !ok {
			if retry {
				log.Printf("retrying to parse %s json for %s [%d]", klt, code, rt+1)
				continue
			} else {
				log.Printf("stop retrying to parse %s json for %s [%d]", klt, code, rt+1)
				return
			}
		}
		body, e := util.HttpGetBytes(url)
		if e != nil {
			log.Printf("can't get %s for %s. please try again later.", klt, code)
			return
		}
		khist := model.Khist{}
		e = json.Unmarshal(strip(body), &khist)
		if e != nil {
			if rt+1 < RETRIES {
				log.Printf("retrying to parse %s json for %s, [%d]: %+v", klt, code, rt+1, e)
				continue
			} else {
				log.Printf("stop retrying to parse %s json for %s, [%d]: %+v", klt, code, rt+1, e)
				return
			}
		}
		if khist.Data == "" {
			log.Printf("%s %s data may not be ready yet", code, klt)
			return
		}
		kls, _ := parseKlines(code, khist.Data, ldate, "")
		quotes = append(quotes, ktoday)
		if len(kls) > 0 {
			//always remove the last/latest one from /last.js
			//substitute by that from /today.js
			quotes = append(quotes, kls[1:]...)
		}
		break
	}
	supplementMisc(quotes, lklid)
	if ldate != "" {
		//skip last record which is for varate calculation
		quotes = quotes[:len(quotes)-1]
	}
	binsert(quotes, string(klt))
	return quotes, true
}

//Assign KLID, calculate Varate, add update datetime
func supplementMisc(klines []*model.Quote, start int) {
	d, t := util.TimeStr()
	preclose := math.NaN()
	for i := len(klines) - 1; i >= 0; i-- {
		start++
		klines[i].Klid = start
		klines[i].Udate.Valid = true
		klines[i].Utime.Valid = true
		klines[i].Udate.String = d
		klines[i].Utime.String = t
		klines[i].Varate.Valid = true
		if math.IsNaN(preclose) {
			klines[i].Varate.Float64 = 0
		} else if preclose == 0 {
			klines[i].Varate.Float64 = 100
		} else {
			klines[i].Varate.Float64 = (klines[i].Close - preclose) / math.Abs(preclose) * 100
		}
		preclose = klines[i].Close
	}
}

func binsert(quotes []*model.Quote, table string) (c int) {
	if len(quotes) > 0 {
		valueStrings := make([]string, 0, len(quotes))
		valueArgs := make([]interface{}, 0, len(quotes)*13)
		var code string
		for _, q := range quotes {
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, round(?,3), ?, ?)")
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
			valueArgs = append(valueArgs, q.Varate)
			valueArgs = append(valueArgs, q.Udate)
			valueArgs = append(valueArgs, q.Utime)
			code = q.Code
		}
		stmt := fmt.Sprintf("INSERT INTO %s (code,date,klid,open,high,close,low,"+
			"volume,amount,xrate,varate,udate,utime) VALUES %s on duplicate key update date=values(date),"+
			"open=values(open),high=values(high),close=values(close),low=values(low),"+
			"volume=values(volume),amount=values(amount),xrate=values(xrate),varate=values(varate),udate=values"+
			"(udate),utime=values(utime)",
			table, strings.Join(valueStrings, ","))
		_, err := dbmap.Exec(stmt, valueArgs...)
		if !util.CheckErr(err, code+" failed to bulk insert "+table) {
			c = len(quotes)
		}
	}
	return
}

//parse semi-colon separated string to quotes, with latest in the head (reverse order of the string data).
func parseKlines(code, data, ldate, skipto string) (kls []*model.Quote, more bool) {
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
				} else if skipto != "" && kl.Date >= skipto {
					continue DATES
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

func getLatestKl(code string, klt model.DBTab, offset int) (q *model.Quote) {
	e := dbmap.SelectOne(&q, fmt.Sprintf("select code, date, klid from %s where code = ? order by klid desc "+
		"limit 1 offset ?", klt), code, offset+1) //plus one offset for pre-close, varate calculation
	if e != nil {
		if "sql: no rows in result set" == e.Error() {
			return nil
		} else {
			log.Panicln("failed to run sql", e)
		}
		return nil
	} else {
		return
	}
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
