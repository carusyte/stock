package getd

import (
	"time"
	"fmt"
	"encoding/json"
	"sort"
	"log"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"github.com/carusyte/stock/conf"
	"strconv"
	"github.com/knq/chromedp"
	"github.com/knq/chromedp/runner"
	"context"
	"github.com/knq/chromedp/cdp"
	"github.com/knq/chromedp/cdp/network"
	"strings"
	"github.com/pkg/errors"
)

var (
	pool *chromedp.Pool
)

// order: from oldest to latest
func klineThsCDP(stk *model.Stock, klt model.DBTab, incr bool, ldate *string, lklid *int) (
	quotes []*model.Quote, suc, retry bool) {
	var (
		code   string = stk.Code
		kall   model.KlAll
		ktoday model.Ktoday
		body   []byte
		e      error
		mode   string
	)
	*ldate = ""
	*lklid = -1
	//mode:
	// 00-no reinstatement
	// 01-forward reinstatement
	// 02-backward reinstatement
	switch klt {
	case model.KLINE_DAY:
		mode = "01"
	case model.KLINE_DAY_NR:
		mode = "00"
	case model.KLINE_WEEK:
		mode = "11"
	case model.KLINE_MONTH:
		mode = "21"
	default:
		log.Panicf("unhandled kltype: %s", klt)
	}
	url_today := fmt.Sprintf("http://d.10jqka.com.cn/v6/line/hs_%s/%s/today.js", code, mode)
	body, e = util.HttpGetBytesUsingHeaders(url_today,
		map[string]string{
			"Referer": "http://stockpage.10jqka.com.cn/HQ_v4.html",
			"Cookie":  conf.Args.Datasource.ThsCookie})
	if e != nil {
		log.Printf("%s error visiting %s: \n%+v", code, url_today, e)
		return quotes, false, false
	}
	ktoday = model.Ktoday{}
	e = json.Unmarshal(strip(body), &ktoday)
	if e != nil {
		log.Printf("%s error parsing json from %s: %s\n%+v", code, url_today, string(body), e)
		return quotes, false, true
	}
	if ktoday.Code != "" {
		quotes = append(quotes, &ktoday.Quote)
	} else {
		log.Printf("kline today skipped: %s", url_today)
	}

	_, e = time.Parse("2006-01-02", ktoday.Date)
	if e != nil {
		log.Printf("%s invalid date format today: %s\n%+v", code, ktoday.Date, e)
		return quotes, false, true
	}
	// If it is an IPO, return immediately
	if stk.TimeToMarket.Valid && len(stk.TimeToMarket.String) == 10 && ktoday.Date == stk.TimeToMarket.String {
		log.Printf("%s IPO day: %s fetch data for today only", code, stk.TimeToMarket.String)
		return quotes, true, false
	}
	// If in IPO week, skip the rest chores
	if (klt == model.KLINE_WEEK || klt == model.KLINE_MONTH) &&
		stk.TimeToMarket.Valid && len(stk.TimeToMarket.String) == 10 {
		ttm, e := time.Parse("2006-01-02", stk.TimeToMarket.String)
		if e != nil {
			log.Printf("%s invalid date format for \"time to market\": %s\n%+v",
				code, stk.TimeToMarket.String, e)
			return quotes, false, true
		} else {
			ttd, e := time.Parse("2006-01-02", ktoday.Date)
			if e != nil {
				log.Printf("%s invalid date format for \"kline today\": %s\n%+v",
					code, ktoday.Date, e)
				return quotes, false, true
			} else {
				y1, w1 := ttm.ISOWeek()
				y2, w2 := ttd.ISOWeek()
				if y1 == y2 && w1 == w2 {
					log.Printf("%s IPO week %s fetch data for today only", code, stk.TimeToMarket.String)
					return quotes, true, false
				}
			}
		}
	}

	//get all kline data
	//e.g: http://d.10jqka.com.cn/v6/line/hs_000001/01/all.js
	url_all := fmt.Sprintf("http://d.10jqka.com.cn/v6/line/hs_%s/%s/all.js", code, mode)
	body, e = util.HttpGetBytesUsingHeaders(url_all,
		map[string]string{
			"Referer": "http://stockpage.10jqka.com.cn/HQ_v4.html",
			"Cookie":  conf.Args.Datasource.ThsCookie})
	//body, e = util.HttpGetBytes(url_all)
	if e != nil {
		log.Printf("%s error visiting %s: \n%+v", code, url_all, e)
		return quotes, false, true
	}
	kall = model.KlAll{}
	e = json.Unmarshal(strip(body), &kall)
	if e != nil {
		log.Printf("%s error parsing json from %s: %s\n%+v", code, url_all, string(body), e)
		return quotes, false, true
	} else if kall.Price == "" {
		log.Printf("%s empty data in json response from %s: %s", code, url_all, string(body))
		return quotes, false, true
	}

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

	kls, e := parseThsKlinesV6(code, klt, &kall, *ldate)
	if e != nil {
		log.Printf("failed to parse data, %s, %+v, %+v, %+v\n%+v", code, klt, *ldate, e, kall)
		return quotes, false, true
	} else if len(kls) == 0 {
		return quotes, true, false
	}

	if (klt == model.KLINE_DAY || klt == model.KLINE_DAY_NR) && kls[0].Date == ktoday.Date {
		// if ktoday and kls[0] in the same month, remove kls[0]
		kls = kls[1:]
	} else if klt == model.KLINE_MONTH && kls[0].Date[:8] == ktoday.Date[:8] {
		// if ktoday and kls[0] in the same month, remove kls[0]
		kls = kls[1:]
	} else if klt == model.KLINE_WEEK {
		// if ktoday and kls[0] in the same week, remove kls[0]
		tToday, e := time.Parse("2006-01-02", ktoday.Date)
		if e != nil {
			log.Printf("%s %s invalid date format: %+v \n %+v", code, klt, ktoday.Date, e)
			return quotes, false, true
		}
		yToday, wToday := tToday.ISOWeek()
		tHead, e := time.Parse("2006-01-02", kls[0].Date)
		if e != nil {
			log.Printf("%s %s invalid date format: %+v \n %+v", code, klt, kls[0].Date, e)
			return quotes, false, true
		}
		yLast, wLast := tHead.ISOWeek()
		if yToday == yLast && wToday == wLast {
			kls = kls[1:]
		}
	}

	quotes = append(quotes, kls...)
	//reverse order
	for i, j := 0, len(quotes)-1; i < j; i, j = i+1, j-1 {
		quotes[i], quotes[j] = quotes[j], quotes[i]
	}

	return quotes, true, false
}

func runCpd(code string, tab model.DBTab) (ok, retry bool, today, all string) {
	// create context
	ctxt, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// get chrome runner from the pool
	pr, err := getCdpPool().Allocate(ctxt,
		runner.Flag("headless", true),
		runner.Flag("no-default-browser-check", true),
		runner.Flag("no-first-run", true),
		runner.ExecPath(`/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary`),
	)
	if err != nil {
		log.Printf("%s failed to allocate chrome runner from the pool: %+v\n", code, err)
		return false, true, today, all
	}
	defer pr.Release()
	err = pr.Run(ctxt, buildActions(code, tab, &today, &all))
	if err != nil {
		log.Printf("chrome runner reported error: %+v\n", err)
		return false, true, today, all
	}
	//TODO select ctxt.Done() and distinguish timeout from cancel(success)
	select {
	case <-ctxt.Done():
		if ctxt.Err() != nil {
			log.Printf("%s timeout waiting for network response", code)
			return false, true, today, all
		}
		return true, false, today, all
	}
}

func buildActions(code string, tab model.DBTab, today, all *string) chromedp.Tasks {
	url := fmt.Sprintf(`http://stockpage.10jqka.com.cn/HQ_v4.html#hs_%s`, code)
	fin := make(chan error)
	sel := ``
	mcode := ""
	switch tab {
	case model.KLINE_DAY_NR:
		sel = `a[hxc3-data-type="hxc3KlineQfqDay"]`
		mcode = "00"
		return chromedp.Tasks{
			chromedp.Navigate(url),
			chromedp.WaitVisible(sel, chromedp.ByQuery),
			chromedp.Click(sel, chromedp.ByQuery),
			chromedp.WaitVisible(`#changeFq`, chromedp.ByID),
			chromedp.Click(`#changeFq`, chromedp.ByID),
			chromedp.WaitVisible(`a[data-type="Bfq"]`, chromedp.ByQuery),
			captureData(today, all, mcode, fin),
			chromedp.Click(`a[data-type="Bfq"]`, chromedp.ByQuery),
			wait(fin),
		}
	case model.KLINE_DAY:
		mcode = "01"
		sel = `a[hxc3-data-type="hxc3KlineQfqDay"]`
	case model.KLINE_WEEK:
		mcode = "11"
		sel = `a[hxc3-data-type="hxc3KlineQfqWeek"]`
	case model.KLINE_MONTH:
		mcode = "21"
		sel = `a[hxc3-data-type="hxc3KlineQfqMonth"]`
	}
	return chromedp.Tasks{
		chromedp.Navigate(url),
		chromedp.WaitVisible(sel, chromedp.ByQuery),
		captureData(today, all, mcode, fin),
		chromedp.Click(sel, chromedp.ByQuery),
		wait(fin),
	}
}

func wait(fin chan error) chromedp.Action {
	return chromedp.ActionFunc(func(ctxt context.Context, h cdp.Handler) error {
		select {
		case <-ctxt.Done():
			return nil
		case e := <-fin:
			return e
		}
	})
}

func captureData(today, all *string, mcode string, fin chan error) chromedp.Action {
	return chromedp.ActionFunc(func(ctxt context.Context, h cdp.Handler) error {
		echan := h.Listen(cdp.EventNetworkRequestWillBeSent, cdp.EventNetworkLoadingFinished,
			cdp.EventNetworkLoadingFailed)
		go func(echan <-chan interface{}, ctxt context.Context, fin chan error) {
			defer func() {
				h.Release(echan)
				close(fin)
			}()
			var (
				reqIdTd, reqIdAll network.RequestID
				urlTd, urlAll     string
				finTd, finAll     = false, false
			)
			for {
				select {
				case d := <-echan:
					switch d.(type) {
					case *network.EventLoadingFailed:
						lfail := d.(*network.EventLoadingFailed)
						if reqIdTd == lfail.RequestID {
							fin <- errors.Errorf("network data loading failed: %s, %+v", urlTd, lfail)
							return
						} else if reqIdAll == lfail.RequestID {
							fin <- errors.Errorf("network data loading failed: %s, %+v", urlAll, lfail)
							return
						}
					case *network.EventRequestWillBeSent:
						req := d.(*network.EventRequestWillBeSent)
						if strings.HasSuffix(req.Request.URL, mcode+"/today.js") {
							urlTd = req.Request.URL
							reqIdTd = req.RequestID
						} else if strings.HasSuffix(req.Request.URL, mcode+"/all.js") {
							urlAll = req.Request.URL
							reqIdAll = req.RequestID
						}
					case *network.EventLoadingFinished:
						res := d.(*network.EventLoadingFinished)
						if reqIdTd == res.RequestID {
							data, e := network.GetResponseBody(reqIdTd).Do(ctxt, h)
							if e != nil {
								fin <- errors.Wrapf(e, "failed to get response body "+
									"from chrome, requestId: %+v, url: %s", reqIdTd, urlTd)
								return
							}
							*today = string(data)
							finTd = true
						} else if reqIdAll == res.RequestID {
							data, e := network.GetResponseBody(reqIdAll).Do(ctxt, h)
							if e != nil {
								fin <- errors.Wrapf(e, "failed to get response body "+
									"from chrome, requestId: %+v, url: %s", reqIdAll, urlAll)
							}
							*all = string(data)
							finAll = true
						}
					}
					if finTd && finAll {
						fin <- nil
						return
					}
				case <-ctxt.Done():
					return
				}
			}
		}(echan, ctxt, fin)
		return nil
	})
}

func getCdpPool() *chromedp.Pool {
	if pool != nil {
		return pool
	}
	var err error
	//cdp.PoolLog(nil, nil, log.Printf)
	pool, err = chromedp.NewPool()
	if err != nil {
		log.Fatal(err)
	}
	return pool
}

func cleanupTHS() {
	if pool != nil {
		err := pool.Shutdown()
		pool = nil
		if err != nil {
			log.Fatal(err)
		}
	}
}

func dKlineThsV2(stk *model.Stock, klt model.DBTab, incr bool, ldate *string, lklid *int) (kldy []*model.Quote, suc, retry bool) {
	var (
		code   string = stk.Code
		klast  model.Klast
		ktoday model.Ktoday
		body   []byte
		e      error
		dkeys  []string                = make([]string, 0, 16)         // date as keys to sort
		klmap  map[string]*model.Quote = make(map[string]*model.Quote) // date - quote map to eliminate duplicates
		oldest string                                                  // stores the oldest date
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
	url_today := fmt.Sprintf("http://d.10jqka.com.cn/v6/line/hs_%s/%s/today.js", code, mode)
	body, e = util.HttpGetBytesUsingHeaders(url_today,
		map[string]string{
			"Referer": "http://stockpage.10jqka.com.cn/HQ_v4.html",
			"Cookie":  conf.Args.Datasource.ThsCookie})
	//body, e = util.HttpGetBytes(url_today)
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
	url_last := fmt.Sprintf("http://d.10jqka.com.cn/v6/line/hs_%s/%s/last.js", code, mode)
	body, e = util.HttpGetBytesUsingHeaders(url_last,
		map[string]string{
			"Referer": "http://stockpage.10jqka.com.cn/HQ_v4.html",
			"Cookie":  conf.Args.Datasource.ThsCookie})
	//body, e = util.HttpGetBytes(url_last)
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
			url_hist := fmt.Sprintf("http://d.10jqka.com.cn/v6/line/hs_%s/%s/%d.js", code, mode,
				yr)
			body, e = util.HttpGetBytesUsingHeaders(url_hist,
				map[string]string{
					"Referer": "http://stockpage.10jqka.com.cn/HQ_v4.html",
					"Cookie":  conf.Args.Datasource.ThsCookie})
			//body, e = util.HttpGetBytes(url_hist)
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

// order: from oldest to latest
func klineThsV6(stk *model.Stock, klt model.DBTab, incr bool, ldate *string, lklid *int) (
	quotes []*model.Quote, suc, retry bool) {
	var (
		code   string = stk.Code
		kall   model.KlAll
		ktoday model.Ktoday
		body   []byte
		e      error
		mode   string
	)
	*ldate = ""
	*lklid = -1
	//mode:
	// 00-no reinstatement
	// 01-forward reinstatement
	// 02-backward reinstatement
	switch klt {
	case model.KLINE_DAY:
		mode = "01"
	case model.KLINE_DAY_NR:
		mode = "00"
	case model.KLINE_WEEK:
		mode = "11"
	case model.KLINE_MONTH:
		mode = "21"
	default:
		log.Panicf("unhandled kltype: %s", klt)
	}
	url_today := fmt.Sprintf("http://d.10jqka.com.cn/v6/line/hs_%s/%s/today.js", code, mode)
	body, e = util.HttpGetBytesUsingHeaders(url_today,
		map[string]string{
			"Referer": "http://stockpage.10jqka.com.cn/HQ_v4.html",
			"Cookie":  conf.Args.Datasource.ThsCookie})
	if e != nil {
		log.Printf("%s error visiting %s: \n%+v", code, url_today, e)
		return quotes, false, false
	}
	ktoday = model.Ktoday{}
	e = json.Unmarshal(strip(body), &ktoday)
	if e != nil {
		log.Printf("%s error parsing json from %s: %s\n%+v", code, url_today, string(body), e)
		return quotes, false, true
	}
	if ktoday.Code != "" {
		quotes = append(quotes, &ktoday.Quote)
	} else {
		log.Printf("kline today skipped: %s", url_today)
	}

	_, e = time.Parse("2006-01-02", ktoday.Date)
	if e != nil {
		log.Printf("%s invalid date format today: %s\n%+v", code, ktoday.Date, e)
		return quotes, false, true
	}
	// If it is an IPO, return immediately
	if stk.TimeToMarket.Valid && len(stk.TimeToMarket.String) == 10 && ktoday.Date == stk.TimeToMarket.String {
		log.Printf("%s IPO day: %s fetch data for today only", code, stk.TimeToMarket.String)
		return quotes, true, false
	}
	// If in IPO week, skip the rest chores
	if (klt == model.KLINE_WEEK || klt == model.KLINE_MONTH) &&
		stk.TimeToMarket.Valid && len(stk.TimeToMarket.String) == 10 {
		ttm, e := time.Parse("2006-01-02", stk.TimeToMarket.String)
		if e != nil {
			log.Printf("%s invalid date format for \"time to market\": %s\n%+v",
				code, stk.TimeToMarket.String, e)
			return quotes, false, true
		} else {
			ttd, e := time.Parse("2006-01-02", ktoday.Date)
			if e != nil {
				log.Printf("%s invalid date format for \"kline today\": %s\n%+v",
					code, ktoday.Date, e)
				return quotes, false, true
			} else {
				y1, w1 := ttm.ISOWeek()
				y2, w2 := ttd.ISOWeek()
				if y1 == y2 && w1 == w2 {
					log.Printf("%s IPO week %s fetch data for today only", code, stk.TimeToMarket.String)
					return quotes, true, false
				}
			}
		}
	}

	//get all kline data
	//e.g: http://d.10jqka.com.cn/v6/line/hs_000001/01/all.js
	url_all := fmt.Sprintf("http://d.10jqka.com.cn/v6/line/hs_%s/%s/all.js", code, mode)
	body, e = util.HttpGetBytesUsingHeaders(url_all,
		map[string]string{
			"Referer": "http://stockpage.10jqka.com.cn/HQ_v4.html",
			"Cookie":  conf.Args.Datasource.ThsCookie})
	//body, e = util.HttpGetBytes(url_all)
	if e != nil {
		log.Printf("%s error visiting %s: \n%+v", code, url_all, e)
		return quotes, false, true
	}
	kall = model.KlAll{}
	e = json.Unmarshal(strip(body), &kall)
	if e != nil {
		log.Printf("%s error parsing json from %s: %s\n%+v", code, url_all, string(body), e)
		return quotes, false, true
	} else if kall.Price == "" {
		log.Printf("%s empty data in json response from %s: %s", code, url_all, string(body))
		return quotes, false, true
	}

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

	kls, e := parseThsKlinesV6(code, klt, &kall, *ldate)
	if e != nil {
		log.Printf("failed to parse data, %s, %+v, %+v, %+v\n%+v", code, klt, *ldate, e, kall)
		return quotes, false, true
	} else if len(kls) == 0 {
		return quotes, true, false
	}

	if (klt == model.KLINE_DAY || klt == model.KLINE_DAY_NR) && kls[0].Date == ktoday.Date {
		// if ktoday and kls[0] in the same month, remove kls[0]
		kls = kls[1:]
	} else if klt == model.KLINE_MONTH && kls[0].Date[:8] == ktoday.Date[:8] {
		// if ktoday and kls[0] in the same month, remove kls[0]
		kls = kls[1:]
	} else if klt == model.KLINE_WEEK {
		// if ktoday and kls[0] in the same week, remove kls[0]
		tToday, e := time.Parse("2006-01-02", ktoday.Date)
		if e != nil {
			log.Printf("%s %s invalid date format: %+v \n %+v", code, klt, ktoday.Date, e)
			return quotes, false, true
		}
		yToday, wToday := tToday.ISOWeek()
		tHead, e := time.Parse("2006-01-02", kls[0].Date)
		if e != nil {
			log.Printf("%s %s invalid date format: %+v \n %+v", code, klt, kls[0].Date, e)
			return quotes, false, true
		}
		yLast, wLast := tHead.ISOWeek()
		if yToday == yLast && wToday == wLast {
			kls = kls[1:]
		}
	}

	quotes = append(quotes, kls...)
	//reverse order
	for i, j := 0, len(quotes)-1; i < j; i, j = i+1, j-1 {
		quotes[i], quotes[j] = quotes[j], quotes[i]
	}

	return quotes, true, false
}
