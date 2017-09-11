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
	"sort"
	"time"
)

//Get various types of kline data for the given stocks. Returns the stocks that have been successfully processed.
func GetKlines(stks *model.Stocks, kltype ... model.DBTab) (rstks *model.Stocks) {
	//TODO find a way to get minute level klines and SH, SZ market index
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
		sql := fmt.Sprintf("select * from (select * from %s where code = ? order by klid desc limit ?) t "+
			"order by t.klid %s", tab, d)
		_, e := dbmap.Select(&hist, sql, code, limit)
		util.CheckErr(e, "failed to query "+string(tab)+" for "+code)
	}
	return
}

func GetKlBtwn(code string, tab model.DBTab, dt1, dt2 string, desc bool) (hist []*model.Quote) {
	var (
		dt1cond, dt2cond string
	)
	if dt1 != "" {
		op := ">"
		if strings.HasPrefix(dt1, "[") {
			op += "="
			dt1 = dt1[1:]
		}
		dt1cond = fmt.Sprintf("and date %s '%s'", op, dt1)
	}
	if dt2 != "" {
		op := "<"
		if strings.HasSuffix(dt2, "]") {
			op += "="
			dt2 = dt2[:len(dt2)-1]
		}
		dt2cond = fmt.Sprintf("and date %s '%s'", op, dt2)
	}
	d := ""
	if desc {
		d = "desc"
	}
	sql := fmt.Sprintf("select * from %s where code = ? %s %s order by klid %s",
		tab, dt1cond, dt2cond, d)
	_, e := dbmap.Select(&hist, sql, code)
	util.CheckErr(e, "failed to query "+string(tab)+" for "+code)
	return
}

func ToOne(qs []*model.Quote, preClose float64, preKlid int) *model.Quote {
	oq := new(model.Quote)
	if len(qs) == 0 {
		return nil
	} else if len(qs) == 1 {
		return qs[0]
	} else {
		oq.Low = math.Inf(0)
		oq.High = math.Inf(-1)
		oq.Code = qs[0].Code
		oq.Klid = preKlid + 1
		oq.Open = qs[0].Open
		oq.Close = qs[len(qs)-1].Close
		oq.Date = qs[len(qs)-1].Date
		oq.Varate.Valid = true
		denom := preClose
		if preClose == 0 {
			denom = .01
		}
		oq.Varate.Float64 = (oq.Close - preClose) / math.Abs(denom)
		d, t := util.TimeStr()
		oq.Udate.Valid = true
		oq.Utime.Valid = true
		oq.Udate.String = d
		oq.Utime.String = t
		for _, q := range qs {
			if q.Low < oq.Low {
				oq.Low = q.Low
			}
			if q.High > oq.High {
				oq.High = q.High
			}
			oq.Volume += q.Volume
			if q.Xrate.Valid {
				oq.Xrate.Valid = true
			}
			oq.Xrate.Float64 += q.Xrate.Float64
			oq.Amount += q.Amount
		}
		// no handling of oq.Time yet
	}
	return oq
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
		case model.KLINE_60M:
			_, suc = getMinuteKlines(stk.Code, t)
		case model.KLINE_DAY:
			_, suc = getDailyKlines(stk, t, xdxr == nil)
		case model.KLINE_DAY_NR:
			_, suc = getDailyKlines(stk, t, true)
		case model.KLINE_WEEK, model.KLINE_MONTH:
			_, suc = getLongKlines(stk, t, xdxr == nil)
		default:
			log.Panicf("unhandled kltype: %s", t)
		}
		if !suc {
			break
		}
	}
	if suc {
		outstks <- stk
	}
}

func getMinuteKlines(code string, tab model.DBTab) (klmin []*model.Quote, suc bool) {
	RETRIES := 5
	for rt := 0; rt < RETRIES; rt++ {
		kls, suc, retry := tryMinuteKlines(code, tab)
		if suc {
			return kls, true
		} else {
			if retry && rt+1 < RETRIES {
				log.Printf("%s retrying to get %s [%d]", code, tab, rt+1)
				continue
			} else {
				log.Printf("%s failed getting %s", code, tab)
				return klmin, false
			}
		}
	}
	return klmin, false
}

func tryMinuteKlines(code string, tab model.DBTab) (klmin []*model.Quote, suc, retry bool) {
	//TODO implement minute klines
	//urlt := `https://xueqiu.com/stock/forchartk/stocklist.json?symbol=%s&period=60m&type=before`
	panic("implement me ")
}

func getDailyKlines(stk *model.Stock, klt model.DBTab, incr bool) (kldy []*model.Quote, suc bool) {
	RETRIES := 20
	var (
		ldate string
		lklid int
		mode  string
		code  string = stk.Code
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

	for rt := 0; rt < RETRIES; rt++ {
		kls, suc, retry := tryDailyKlines(stk, mode, klt, incr, &ldate, &lklid)
		if suc {
			kldy = kls
			break
		} else {
			if retry && rt+1 < RETRIES {
				log.Printf("%s retrying to get %s [%d]", code, klt, rt+1)
				time.Sleep(time.Millisecond * 500)
				continue
			} else {
				log.Printf("%s failed to get %s", code, klt)
				return kldy, false
			}
		}
	}

	supplementMisc(kldy, lklid)
	if ldate != "" {
		//skip the first record which is for varate calculation
		kldy = kldy[1:]
	}
	binsert(kldy, string(klt), lklid)
	return kldy, true
}

func tryDailyKlines(stk *model.Stock, mode string, klt model.DBTab, incr bool, ldate *string, lklid *int) (kldy []*model.Quote, suc, retry bool) {
	var (
		code   string = stk.Code
		klast  model.Klast
		ktoday model.Ktoday
		body   []byte
		e      error
		dkeys  []string                = make([]string, 0, 16)         // date as keys to sort
		klmap  map[string]*model.Quote = make(map[string]*model.Quote) // date - quote map to eliminate duplicates
		oldest string                                                  // stores the oldest date
	)
	url_today := fmt.Sprintf("http://d.10jqka.com.cn/v2/line/hs_%s/%s/today.js", code, mode)
	body, e = util.HttpGetBytes(url_today)
	if e != nil {
		log.Printf("%s error visiting %s: \n%+v", code, url_today, e)
		return kldy, false, false
	}
	ktoday = model.Ktoday{}
	e = json.Unmarshal(strip(body), &ktoday)
	if e != nil {
		log.Printf("%s error parsing json from %s: %s\n%+v", code, url_today, string(body), e)
		return kldy, false, true
	}
	if ktoday.Code != "" {
		klmap[ktoday.Date] = &ktoday.Quote
		dkeys = append(dkeys, ktoday.Date)
		oldest = ktoday.Date
	} else {
		log.Printf("kline today skipped: %s", url_today)
	}

	// If it is an IPO, return immediately
	_, e = time.Parse("2006-01-02", ktoday.Date)
	if e != nil {
		log.Printf("%s invalid date format today: %s\n%+v", code, ktoday.Date, e)
		return kldy, false, true
	}
	if stk.TimeToMarket.Valid && len(stk.TimeToMarket.String) == 10 && ktoday.Date == stk.TimeToMarket.String {
		log.Printf("%s IPO day: %s fetch data for today only", code, stk.TimeToMarket.String)
		return append(kldy, &ktoday.Quote), true, false
	}

	//get last kline data
	url_last := fmt.Sprintf("http://d.10jqka.com.cn/v2/line/hs_%s/%s/last.js", code, mode)
	body, e = util.HttpGetBytes(url_last)
	if e != nil {
		log.Printf("%s error visiting %s: \n%+v", code, url_last, e)
		return kldy, false, true
	}
	klast = model.Klast{}
	e = json.Unmarshal(strip(body), &klast)
	if e != nil {
		log.Printf("%s error parsing json from %s: %s\n%+v", code, url_last, string(body), e)
		return kldy, false, true
	} else if klast.Data == "" {
		log.Printf("%s empty data in json response from %s: %s", code, url_last, string(body))
		return kldy, false, true
	}

	*ldate = ""
	*lklid = -1
	if incr {
		ldy := getLatestKl(code, klt, 5+1) //plus one offset for pre-close, varate calculation
		if ldy != nil {
			*ldate = ldy.Date
			*lklid = ldy.Klid
		} else {
			log.Printf("%s latest %s data not found, will be fully refreshed", code, klt)
		}
	} else {
		log.Printf("%s %s data will be fully refreshed", code, klt)
	}

	kls, more := parseKlines(code, klast.Data, *ldate, "")
	if len(kls) > 0 {
		for _, k := range kls {
			if _, exists := klmap[k.Date]; !exists {
				klmap[k.Date] = k
				dkeys = append(dkeys, k.Date)
				oldest = k.Date
			}
		}
	} else if len(kls) <= 0 || !more {
		return kldy, true, false
	}
	//get hist kline data
	yr, e := strconv.ParseInt(kls[0].Date[:4], 10, 32)
	if e != nil {
		log.Printf("failed to parse year for %+v, stop processing. error: %+v",
			code, e)
		return kldy, false, false
	}
	start, e := strconv.ParseInt(klast.Start[:4], 10, 32)
	if e != nil {
		log.Printf("failed to parse json start year for %+v, stop processing. "+
			"string:%s, error: %+v", code, klast.Start, e)
		return kldy, false, false
	}
	yr++
	for more {
		yr--
		if yr < start {
			break
		}
		// test if yr is in klast.Year map
		if _, in := klast.Year[strconv.FormatInt(yr, 10)]; !in {
			continue
		}
		ok := false
		for tries := 1; tries <= 3; tries++ {
			url_hist := fmt.Sprintf("http://d.10jqka.com.cn/v2/line/hs_%s/%s/%d.js", code, mode,
				yr)
			body, e = util.HttpGetBytes(url_hist)
			if e != nil {
				log.Printf("%s [%d] error visiting %s: \n%+v", code, tries, url_hist, e)
				ok = false
				continue
			}
			khist := model.Khist{}
			e = json.Unmarshal(strip(body), &khist)
			if e != nil {
				log.Printf("%s [%d], error parsing json from %s: %s\n%+v", code, tries, url_hist, string(body), e)
				ok = false
				continue
			}
			kls, more = parseKlines(code, khist.Data, *ldate, oldest)
			if len(kls) > 0 {
				for _, k := range kls {
					if _, exists := klmap[k.Date]; !exists {
						klmap[k.Date] = k
						dkeys = append(dkeys, k.Date)
						oldest = k.Date
					}
				}
			}
			ok = true
			break
		}
		if !ok {
			return kldy, false, false
		}
	}
	sort.Strings(dkeys)
	kldy = make([]*model.Quote, len(dkeys))
	for i, k := range dkeys {
		kldy[i] = klmap[k]
	}
	return kldy, true, false
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

func getLongKlines(stk *model.Stock, klt model.DBTab, incr bool) (quotes []*model.Quote, suc bool) {
	urlt := "http://d.10jqka.com.cn/v2/line/hs_%s/%s/last.js"
	var (
		code  = stk.Code
		typ   string
		dkeys []string                = make([]string, 0, 16)         // date as keys to sort
		klmap map[string]*model.Quote = make(map[string]*model.Quote) // date - quote map to eliminate duplicates
	)
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
		latest := getLatestKl(code, klt, 5+1) //plus one offset for pre-close, varate calculation
		if latest != nil {
			ldate = latest.Date
			lklid = latest.Klid
		} else {
			log.Printf("%s latest %s data not found, will be fully refreshed", code, klt)
		}
	} else {
		log.Printf("%s %s data will be fully refreshed", code, klt)
	}
	RETRIES := 10
	url := fmt.Sprintf(urlt, code, typ)
	for rt := 0; rt < RETRIES; rt++ {
		ktoday, ok, retry := getToday(code, typ)
		if !ok {
			if retry {
				log.Printf("retrying to parse %s json for %s [%d]", klt, code, rt+1)
				ms := time.Duration(500 + rt*500)
				time.Sleep(time.Millisecond * ms)
				continue
			} else {
				log.Printf("stop retrying to parse %s json for %s [%d]", klt, code, rt+1)
				return
			}
		}
		klmap[ktoday.Date] = ktoday
		dkeys = append(dkeys, ktoday.Date)
		// If in IPO week, skip the rest chores
		if stk.TimeToMarket.Valid && len(stk.TimeToMarket.String) == 10 {
			ttm, e := time.Parse("2006-01-02", stk.TimeToMarket.String)
			if e != nil {
				log.Printf("%s invalid date format for \"time to market\": %s\n%+v",
					code, stk.TimeToMarket.String, e)
			} else {
				ttd, e := time.Parse("2006-01-02", ktoday.Date)
				if e != nil {
					log.Printf("%s invalid date format for \"kline today\": %s\n%+v",
						code, ktoday.Date, e)
				} else {
					y1, w1 := ttm.ISOWeek()
					y2, w2 := ttd.ISOWeek()
					if y1 == y2 && w1 == w2 {
						log.Printf("%s IPO week %s fetch data for today only", code, stk.TimeToMarket.String)
						break
					}
				}
			}
		}
		body, e := util.HttpGetBytes(url)
		if e != nil {
			log.Printf("can't get %s for %s. please try again later.", klt, code)
			return
		}
		khist := model.Khist{}
		e = json.Unmarshal(strip(body), &khist)
		if e != nil || khist.Data == "" {
			if rt+1 < RETRIES {
				log.Printf("retrying to parse %s json for %s, [%d]: %+v", klt, code, rt+1, e)
				ms := time.Duration(500 + rt*500)
				time.Sleep(time.Millisecond * ms)
				continue
			} else {
				log.Printf("stop retrying to parse %s json for %s, [%d]: %+v", klt, code, rt+1, e)
				return
			}
		}
		kls, _ := parseKlines(code, khist.Data, ldate, "")
		if len(kls) > 0 {
			// if ktoday and kls[0] in the same week, remove kls[0]
			tToday, e := time.Parse("2006-01-02", ktoday.Date)
			if e != nil {
				log.Printf("%s %s [%d] invalid date format %+v", code, klt, rt+1, e)
				continue
			}
			yToday, wToday := tToday.ISOWeek()
			tHead, e := time.Parse("2006-01-02", kls[0].Date)
			if e != nil {
				log.Printf("%s %s [%d] invalid date format %+v", code, klt, rt+1, e)
				continue
			}
			yLast, wLast := tHead.ISOWeek()
			if yToday == yLast && wToday == wLast {
				kls = kls[1:]
			}
			// if cytp is month, and ktoday and kls[0] in the same month, remove kls[0]
			if len(kls) > 0 && klt == model.KLINE_MONTH && kls[0].Date[:8] == ktoday.Date[:8] {
				kls = kls[1:]
			}
			for _, k := range kls {
				if _, exists := klmap[k.Date]; !exists {
					klmap[k.Date] = k
					dkeys = append(dkeys, k.Date)
				}
			}
		}
		break
	}
	if len(dkeys) > 0 {
		sort.Strings(dkeys)
		quotes = make([]*model.Quote, len(dkeys))
		for i, k := range dkeys {
			quotes[i] = klmap[k]
		}
		supplementMisc(quotes, lklid)
		if ldate != "" {
			// skip the first record which is for varate calculation
			quotes = quotes[1:]
		}
		binsert(quotes, string(klt), lklid)
	}
	return quotes, true
}

//Assign KLID, calculate Varate, add update datetime
func supplementMisc(klines []*model.Quote, start int) {
	d, t := util.TimeStr()
	preclose := math.NaN()
	for i := 0; i < len(klines); i++ {
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

func binsert(quotes []*model.Quote, table string, lklid int) (c int) {
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

		tran, e := dbmap.Begin()
		util.CheckErr(e, "failed to start transaction")
		if lklid > 0 {
			lklid++
			_, e = tran.Exec(fmt.Sprintf("delete from %s where code = ? and klid > ?", table), code, lklid)
			if e != nil {
				log.Printf("%s failed to delete %s where klid > %d", code, table, lklid)
				tran.Rollback()
				panic(code)
			}
		}
		stmt := fmt.Sprintf("INSERT INTO %s (code,date,klid,open,high,close,low,"+
			"volume,amount,xrate,varate,udate,utime) VALUES %s on duplicate key update date=values(date),"+
			"open=values(open),high=values(high),close=values(close),low=values(low),"+
			"volume=values(volume),amount=values(amount),xrate=values(xrate),varate=values(varate),udate=values"+
			"(udate),utime=values(utime)",
			table, strings.Join(valueStrings, ","))
		_, e = tran.Exec(stmt, valueArgs...)
		if e != nil {
			tran.Rollback()
			log.Panicf("%s failed to bulk insert %s", code, table)
		}
		c = len(quotes)
		tran.Commit()
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
		"limit 1 offset ?", klt), code, offset)
	if e != nil {
		if "sql: no rows in result set" == e.Error() {
			return nil
		} else {
			log.Panicln("failed to run sql", e)
		}
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
