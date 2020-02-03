package getd

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"

	"github.com/chromedp/chromedp"
	"github.com/pkg/errors"
)

var (
	dt2mc = map[model.DBTab]string{
		model.KLINE_DAY_NR:   "00",
		model.KLINE_DAY_F:    "01",
		model.KLINE_DAY_B:    "02",
		model.KLINE_WEEK_NR:  "10",
		model.KLINE_WEEK_F:   "11",
		model.KLINE_WEEK_B:   "12",
		model.KLINE_MONTH_NR: "20",
		model.KLINE_MONTH_F:  "21",
		model.KLINE_MONTH_B:  "22",
	}
)

//AppendVarateRgl supplements varate_rgl field in relevant kline tables.
func AppendVarateRgl(stks ...*model.Stock) (e error) {
	for _, stk := range stks {
		qmap := make(map[model.DBTab][]*model.Quote)
		tabs := []model.DBTab{
			model.KLINE_DAY_F,
			model.KLINE_WEEK_F,
			model.KLINE_MONTH_F,
		}
		for _, t := range tabs {
			qmap[t] = GetKlineDb(stk.Code, t, 0, false)
		}
		e = calcVarateRgl(stk, qmap)
		if e != nil {
			return e
		}
		for t, qs := range qmap {
			stmt := fmt.Sprintf(`update %v set varate_rgl = round(?,3) where code = ? and klid = ?`, t)
			ps, e := dbmap.Prepare(stmt)
			if e != nil {
				return e
			}
			for _, q := range qs {
				_, e = ps.Exec(q.VarateRgl, q.Code, q.Klid)
				if e != nil {
					ps.Close()
					return e
				}
			}
			ps.Close()
			log.Printf("%s %v (%d) varate_rgl fixed", stk.Code, t, len(qs))
		}
	}
	return nil
}

// This version trys to run fetch for multiple kline types in a single chrome instance,
// may improve performance to some degree
func getKlineThs(stk *model.Stock, kltype []model.DBTab) (
	qmap map[model.DBTab][]*model.Quote, lkmap map[model.DBTab]int, suc bool) {
	RETRIES := conf.Args.DataSource.KlineFailureRetry
	qmap = make(map[model.DBTab][]*model.Quote)
	var (
		code = stk.Code
	)
	klts := make([]model.DBTab, len(kltype))
	copy(klts, kltype)
	for rt := 0; rt < RETRIES; rt++ {
		klsMap, lkdmap, suc, retry := klineThsCDPv2(stk, klts)
		if suc {
			qmap = klsMap
			lkmap = lkdmap
			break
		} else {
			if retry && rt+1 < RETRIES {
				//partial failure
				for k, v := range klsMap {
					qmap[k] = v
				}
				if len(klsMap) < len(klts) {
					nklts := make([]model.DBTab, 0, len(klts))
					for _, t := range klts {
						if _, ok := klsMap[t]; !ok {
							nklts = append(nklts, t)
						}
					}
					klts = nklts
				}
				log.Printf("%s retrying [%d] for %+v", code, rt+1, klts)
				time.Sleep(time.Millisecond * time.Duration(1500+rand.Intn(2000)))
				continue
			} else {
				log.Printf("%s failed", code)
				return qmap, lkmap, false
			}
		}
	}
	return qmap, lkmap, true
}

func klineThs(stk *model.Stock, klt model.DBTab, incr bool) (quotes []*model.Quote, suc bool) {
	RETRIES := conf.Args.DataSource.KlineFailureRetry
	var (
		ldate string
		lklid int
		code  = stk.Code
	)

	for rt := 0; rt < RETRIES; rt++ {
		kls, suc, retry := klineThsCDP(stk, klt, incr, &ldate, &lklid)
		if suc {
			quotes = kls
			break
		} else {
			if retry && rt+1 < RETRIES {
				log.Printf("%s retrying to get %s [%d]", code, klt, rt+1)
				time.Sleep(time.Millisecond * 2500)
				continue
			} else {
				//FIXME sometimes 10jqk nginx server redirects to the same server and replies empty data no matter how many times you try
				log.Printf("%s failed to get %s", code, klt)
				return quotes, false
			}
		}
	}

	supplementMisc(quotes, klt, lklid)
	if ldate != "" {
		//skip the first record which is for varate calculation
		quotes = quotes[1:]
	}
	binsert(quotes, string(klt), lklid)
	return quotes, true
}

func klineThsCDPv2(stk *model.Stock, kltype []model.DBTab) (
	qmap map[model.DBTab][]*model.Quote, lkmap map[model.DBTab]int, suc, retry bool) {
	qmap = make(map[model.DBTab][]*model.Quote)
	suc, retry, tdmap, hismap := runCdpV2(stk.Code, kltype)
	if !suc {
		return nil, nil, false, retry
	}
	xdxr := latestUFRXdxr(stk.Code)
	lkmap = make(map[model.DBTab]int)
	for t, tdat := range tdmap {
		var quotes []*model.Quote
		lklid := -1
		quotes, lklid, suc, retry = byte2Quote(stk, t, tdat, hismap[t], xdxr)
		if !suc {
			return nil, nil, false, retry
		}
		//validate against vld table
		if validateKline(stk, t, quotes, lklid) {
			qmap[t] = quotes
			lkmap[t] = lklid
		} else {
			suc = false
			retry = true
			//non-reinstated data is pre-requisite
			if t == model.KLINE_DAY_NR || t == model.KLINE_WEEK_NR || t == model.KLINE_MONTH_NR {
				return nil, nil, suc, retry
			}
		}
	}
	return
}

func byte2Quote(stk *model.Stock, klt model.DBTab, today, all []byte, xdxr *model.Xdxr) (
	quotes []*model.Quote, lklid int, suc, retry bool) {
	lklid = -1
	var (
		code   = stk.Code
		ktoday = model.Ktoday{}
		kall   = model.KlAll{}
		rtype  = model.Forward
		cycle  model.CYTP
		e      error
	)
	e = json.Unmarshal(strip(today), &ktoday)
	if e != nil {
		log.Printf("%s error parsing json for %+v: %s\n%+v", code, klt, string(today), e)
		return quotes, -1, false, true
	}
	if ktoday.Code != "" {
		quotes = append(quotes, &ktoday.Quote)
	} else {
		log.Printf("%s %+v kline today skipped: %s", klt, code, string(today))
	}

	ttd, e := time.Parse(global.DateFormat, ktoday.Date)
	if e != nil {
		log.Printf("%s invalid date format today: %s\n%+v", code, ktoday.Date, e)
		return quotes, -1, false, true
	}
	// If it is an IPO, return immediately
	if stk.TimeToMarket.Valid && len(stk.TimeToMarket.String) == 10 && ktoday.Date == stk.TimeToMarket.String {
		log.Printf("%s IPO day: %s fetch data for today only", code, stk.TimeToMarket.String)
		return quotes, -1, true, false
	}
	// If in IPO week, skip the rest chores
	switch klt {
	case model.KLINE_WEEK_F, model.KLINE_WEEK_NR, model.KLINE_WEEK_B,
		model.KLINE_MONTH_F, model.KLINE_MONTH_NR, model.KLINE_MONTH_B:
		if stk.TimeToMarket.Valid && len(stk.TimeToMarket.String) == 10 {
			ttm, e := time.Parse(global.DateFormat, stk.TimeToMarket.String)
			if e != nil {
				log.Printf("%s invalid date format for \"time to market\": %s\n%+v",
					code, stk.TimeToMarket.String, e)
			} else {
				y1, w1 := ttm.ISOWeek()
				y2, w2 := ttd.ISOWeek()
				if y1 == y2 && w1 == w2 {
					log.Printf("%s IPO week %s fetch data for today only", code, stk.TimeToMarket.String)
					return quotes, -1, true, false
				}
			}
		}
	}

	//get all kline data
	e = json.Unmarshal(strip(all), &kall)
	if e != nil {
		log.Printf("%s error parsing json for %+v: %s\n%+v", code, klt, string(all), e)
		return quotes, -1, false, true
	} else if kall.Price == "" {
		log.Printf("%s %+v empty price data in json response: %s", code, klt, string(all))
		return quotes, -1, false, true
	}

	incr := true
	switch klt {
	case model.KLINE_DAY_F, model.KLINE_WEEK_F, model.KLINE_MONTH_F:
		incr = xdxr == nil
	}
	switch klt {
	case model.KLINE_DAY_B, model.KLINE_DAY_NR, model.KLINE_DAY_F:
		cycle = model.DAY
	case model.KLINE_WEEK_B, model.KLINE_WEEK_NR, model.KLINE_WEEK_F:
		cycle = model.WEEK
	case model.KLINE_MONTH_B, model.KLINE_MONTH_NR, model.KLINE_MONTH_F:
		cycle = model.MONTH
	}
	switch klt {
	case model.KLINE_DAY_NR, model.KLINE_WEEK_NR, model.KLINE_MONTH_NR:
		rtype = model.None
	case model.KLINE_DAY_B, model.KLINE_WEEK_B, model.KLINE_MONTH_B:
		rtype = model.Backward
	}
	ldate := ""
	if incr {
		ldy := getLatestTradeDataBasic(code, model.KlineMaster, cycle, rtype, 5+1) //plus one offset for pre-close, varate calculation
		if ldy != nil {
			ldate = ldy.Date
			lklid = ldy.Klid
		} else {
			log.Printf("%s latest %s data not found, will be fully refreshed", code, klt)
		}
	} else {
		log.Printf("%s %s data will be fully refreshed", code, klt)
	}

	kls, e := parseThsKlinesV6(code, klt, &kall, ldate)
	if e != nil {
		log.Printf("failed to parse data, %s, %+v, %+v, %+v\n%+v", code, klt, ldate, e, kall)
		return quotes, -1, false, true
	} else if len(kls) == 0 {
		return quotes, -1, true, false
	}

	switch klt {
	case model.KLINE_DAY_F, model.KLINE_DAY_NR, model.KLINE_DAY_B:
		// if ktoday and kls[0] in the same day, remove kls[0]
		if kls[0].Date == ktoday.Date {
			kls = kls[1:]
		}
	case model.KLINE_WEEK_F, model.KLINE_WEEK_NR, model.KLINE_WEEK_B:
		// if ktoday and kls[0] in the same week, remove kls[0]
		yToday, wToday := ttd.ISOWeek()
		tHead, e := time.Parse(global.DateFormat, kls[0].Date)
		if e != nil {
			log.Printf("%s %s invalid date format: %+v \n %+v", code, klt, kls[0].Date, e)
			return quotes, -1, false, true
		}
		yLast, wLast := tHead.ISOWeek()
		if yToday == yLast && wToday == wLast {
			kls = kls[1:]
		}
	case model.KLINE_MONTH_F, model.KLINE_MONTH_NR, model.KLINE_MONTH_B:
		// if ktoday and kls[0] in the same month, remove kls[0]
		if kls[0].Date[:8] == ktoday.Date[:8] {
			kls = kls[1:]
		}
	}

	quotes = append(quotes, kls...)
	//reverse order
	for i, j := 0, len(quotes)-1; i < j; i, j = i+1, j-1 {
		quotes[i], quotes[j] = quotes[j], quotes[i]
	}

	return quotes, lklid, true, false
}

// order: from oldest to latest
func klineThsCDP(stk *model.Stock, klt model.DBTab, incr bool, ldate *string, lklid *int) (
	quotes []*model.Quote, suc, retry bool) {
	var (
		code       = stk.Code
		today, all []byte
		kall       model.KlAll
		ktoday     model.Ktoday
		cycle      model.CYTP
		rtype      = model.Forward
		e          error
	)
	*ldate = ""
	*lklid = -1
	suc, retry, today, all = runCdp(code, klt)
	if !suc {
		return quotes, false, retry
	}
	ktoday = model.Ktoday{}
	e = json.Unmarshal(strip(today), &ktoday)
	if e != nil {
		log.Printf("%s error parsing json for %+v: %s\n%+v", code, klt, string(today), e)
		return quotes, false, true
	}
	if ktoday.Code != "" {
		quotes = append(quotes, &ktoday.Quote)
	} else {
		log.Printf("%s %+v kline today skipped: %s", klt, code, string(today))
	}

	_, e = time.Parse(global.DateFormat, ktoday.Date)
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
	if (klt == model.KLINE_WEEK_F || klt == model.KLINE_MONTH_F) &&
		stk.TimeToMarket.Valid && len(stk.TimeToMarket.String) == 10 {
		ttm, e := time.Parse(global.DateFormat, stk.TimeToMarket.String)
		if e != nil {
			log.Printf("%s invalid date format for \"time to market\": %s\n%+v",
				code, stk.TimeToMarket.String, e)
			return quotes, false, true
		}
		ttd, e := time.Parse(global.DateFormat, ktoday.Date)
		if e != nil {
			log.Printf("%s invalid date format for \"kline today\": %s\n%+v",
				code, ktoday.Date, e)
			return quotes, false, true
		}
		y1, w1 := ttm.ISOWeek()
		y2, w2 := ttd.ISOWeek()
		if y1 == y2 && w1 == w2 {
			log.Printf("%s IPO week %s fetch data for today only", code, stk.TimeToMarket.String)
			return quotes, true, false
		}
	}

	//get all kline data
	kall = model.KlAll{}
	e = json.Unmarshal(strip(all), &kall)
	if e != nil {
		log.Printf("%s error parsing json for %+v: %s\n%+v", code, klt, string(all), e)
		return quotes, false, true
	} else if kall.Price == "" {
		log.Printf("%s %+v empty price data in json response: %s", code, klt, string(all))
		return quotes, false, true
	}

	switch klt {
	case model.KLINE_DAY_B, model.KLINE_DAY_NR, model.KLINE_DAY_F:
		cycle = model.DAY
	case model.KLINE_WEEK_B, model.KLINE_WEEK_NR, model.KLINE_WEEK_F:
		cycle = model.WEEK
	case model.KLINE_MONTH_B, model.KLINE_MONTH_NR, model.KLINE_MONTH_F:
		cycle = model.MONTH
	}
	switch klt {
	case model.KLINE_DAY_NR, model.KLINE_WEEK_NR, model.KLINE_MONTH_NR:
		rtype = model.None
	case model.KLINE_DAY_B, model.KLINE_WEEK_B, model.KLINE_MONTH_B:
		rtype = model.Backward
	}

	if incr {
		ldy := getLatestTradeDataBasic(code, model.KlineMaster, cycle, rtype, 5+1) //plus one offset for pre-close, varate calculation
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

	switch klt {
	case model.KLINE_DAY_F, model.KLINE_DAY_NR, model.KLINE_DAY_B:
		if kls[0].Date == ktoday.Date {
			// if ktoday and kls[0] in the same day, remove kls[0]
			kls = kls[1:]
		}
	case model.KLINE_MONTH_F, model.KLINE_MONTH_NR, model.KLINE_MONTH_B:
		if kls[0].Date[:8] == ktoday.Date[:8] {
			// if ktoday and kls[0] in the same month, remove kls[0]
			kls = kls[1:]
		}
	case model.KLINE_WEEK_F, model.KLINE_WEEK_NR, model.KLINE_WEEK_B:
		// if ktoday and kls[0] in the same week, remove kls[0]
		tToday, e := time.Parse(global.DateFormat, ktoday.Date)
		if e != nil {
			log.Printf("%s %s invalid date format: %+v \n %+v", code, klt, ktoday.Date, e)
			return quotes, false, true
		}
		yToday, wToday := tToday.ISOWeek()
		tHead, e := time.Parse(global.DateFormat, kls[0].Date)
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

func runCdpV2(code string, tabs []model.DBTab) (ok, retry bool, tdmap, hismap map[model.DBTab][]byte) {
	tdmap = make(map[model.DBTab][]byte)
	hismap = make(map[model.DBTab][]byte)
	// create context
	ctxt, cancel := context.WithTimeout(context.Background(), time.Duration(conf.Args.ChromeDP.Timeout)*time.Second)
	defer cancel()

	opts := []chromedp.ExecAllocatorOption{
		chromedp.NoFirstRun,
		chromedp.NoDefaultBrowserCheck,
		chromedp.ExecPath(conf.Args.ChromeDP.Path),
	}

	if conf.Args.ChromeDP.Headless {
		opts = append(opts, chromedp.Headless)
	}

	ctxt, cancel = chromedp.NewExecAllocator(ctxt, opts...)
	defer cancel()

	chr := make(chan bool)
	go func(chr chan bool) {
		e := chromedp.Run(ctxt, buildBatchActions(code, tabs, tdmap, hismap))
		if e != nil {
			log.Printf("chrome runner reported error: %+v\n", e)
			chr <- false
		} else {
			chr <- true
		}
	}(chr)
	select {
	case ok = <-chr:
		return ok, !ok, tdmap, hismap
	case <-ctxt.Done():
		if ctxt.Err() != nil {
			log.Printf("%s timeout waiting for chromedp response", code)
			return false, true, tdmap, hismap
		}
		return true, false, tdmap, hismap
	}
}

func runCdp(code string, tab model.DBTab) (ok, retry bool, today, all []byte) {
	return

	// // create context
	// ctxt, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	// defer cancel()

	// // get chrome runner from the pool
	// pr, err := getCdpPool().Allocate(ctxt,
	// 	runner.Flag("headless", true),
	// 	runner.Flag("no-default-browser-check", true),
	// 	runner.Flag("no-first-run", true),
	// 	runner.ExecPath(conf.Args.ChromeDP.Path),
	// )
	// if err != nil {
	// 	log.Printf("%s failed to allocate chrome runner from the pool: %+v\n", code, err)
	// 	return false, true, today, all
	// }
	// defer pr.Release()
	// chr := make(chan bool)
	// go func(chr chan bool) {
	// 	err = pr.Run(ctxt, buildActions(code, tab, &today, &all))
	// 	if err != nil {
	// 		log.Printf("chrome runner reported error: %+v\n", err)
	// 		chr <- false
	// 	} else {
	// 		chr <- true
	// 	}
	// }(chr)
	// select {
	// case ok = <-chr:
	// 	return ok, !ok, today, all
	// case <-ctxt.Done():
	// 	if ctxt.Err() != nil {
	// 		log.Printf("%s timeout waiting for chromedp response", code)
	// 		return false, true, today, all
	// 	}
	// 	return true, false, today, all
	// }
}

func buildBatchActions(code string, tabs []model.DBTab, tdmap, hismap map[model.DBTab][]byte) chromedp.Tasks {
	fin := make(chan error)
	prevTab := model.DBTab("NIL_DB_TAB")
	url := fmt.Sprintf(`http://stockpage.10jqka.com.cn/HQ_v4.html#hs_%s`, code)
	//.hxc3-hxc3KlinePricePane-hover visible indicates kline data loaded
	ltag := `.hxc3-hxc3KlinePricePane-hover`
	tasks := chromedp.Tasks{
		chromedp.Navigate(url),
		batchCaptureData(code, tdmap, hismap, tabs, fin),
	}
	for _, t := range tabs {
		if strings.Split(string(t), "_")[1] != strings.Split(string(prevTab), "_")[1] {
			sel := `a[hxc3-data-type^="hxc3Kline"][hxc3-data-type$="%s"]`
			switch t {
			case model.KLINE_DAY_NR, model.KLINE_DAY_F, model.KLINE_DAY_B:
				sel = fmt.Sprintf(sel, "Day")
			case model.KLINE_WEEK_NR, model.KLINE_WEEK_F, model.KLINE_WEEK_B:
				sel = fmt.Sprintf(sel, "Week")
			case model.KLINE_MONTH_NR, model.KLINE_MONTH_F, model.KLINE_MONTH_B:
				sel = fmt.Sprintf(sel, "Month")
			default:
				log.Panicf("unsupported DBTab: %+v", t)
			}
			tasks = append(tasks,
				chromedp.WaitVisible(sel, chromedp.ByQuery),
				chromedp.Click(sel, chromedp.ByQuery),
			)
		}
		if strings.HasSuffix(string(t), "_n") != strings.HasSuffix(string(prevTab), "_n") {
			fqSel := `a[data-type="%s"]`
			if t == model.KLINE_DAY_NR || t == model.KLINE_WEEK_NR || t == model.KLINE_MONTH_NR {
				fqSel = fmt.Sprintf(fqSel, "Bfq")
			} else if t == model.KLINE_DAY_B || t == model.KLINE_WEEK_B || t == model.KLINE_MONTH_B {
				fqSel = fmt.Sprintf(fqSel, "Hfq")
			} else {
				fqSel = fmt.Sprintf(fqSel, "Qfq")
			}
			tasks = append(tasks,
				chromedp.WaitVisible(`#changeFq`, chromedp.ByID),
				chromedp.Click(`#changeFq`, chromedp.ByID),
				chromedp.WaitVisible(fqSel, chromedp.ByQuery),
				chromedp.Click(fqSel, chromedp.ByQuery),
				chromedp.WaitVisible(ltag, chromedp.ByQuery))
		}
		prevTab = t
	}
	tasks = append(tasks, wait(fin))
	return tasks
}

func buildActions(code string, tab model.DBTab, today, all *[]byte) chromedp.Tasks {
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
	case model.KLINE_DAY_F:
		mcode = "01"
		sel = `a[hxc3-data-type="hxc3KlineQfqDay"]`
	case model.KLINE_WEEK_F:
		mcode = "11"
		sel = `a[hxc3-data-type="hxc3KlineQfqWeek"]`
	case model.KLINE_MONTH_F:
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
	return chromedp.ActionFunc(func(ctxt context.Context) error {
		select {
		case <-ctxt.Done():
			return nil
		case e := <-fin:
			return e
		}
	})
}

func batchCaptureData(code string, tdmap, hismap map[model.DBTab][]byte, tabs []model.DBTab, fin chan error) chromedp.Action {
	mcodes := make(map[string]model.DBTab)
	for _, t := range tabs {
		mcodes[dt2mc[t]] = t
	}
	return chromedp.ActionFunc(func(ctxt context.Context) error {
		// ** The API of chromedp has been revamped **

		// th, ok := h.(*chromedp.TargetHandler)
		// if !ok {
		// 	log.Fatal("invalid Executor type")
		// }
		// echan := th.Listen(cdproto.EventNetworkRequestWillBeSent, cdproto.EventNetworkLoadingFinished,
		// 	cdproto.EventNetworkLoadingFailed)
		// go func(echan <-chan interface{}, ctxt context.Context, fin chan error) {
		// 	defer func() {
		// 		th.Release(echan)
		// 		close(fin)
		// 	}()
		// 	reqIDTd := make(map[network.RequestID]model.DBTab)
		// 	reqIDAll := make(map[network.RequestID]model.DBTab)
		// 	urlmap := make(map[network.RequestID]string)
		// 	for {
		// 		select {
		// 		case d := <-echan:
		// 			switch d.(type) {
		// 			case *network.EventLoadingFailed:
		// 				lfail := d.(*network.EventLoadingFailed)
		// 				if _, ok := reqIDTd[lfail.RequestID]; ok {
		// 					fin <- errors.Errorf("network data loading failed: %s, %+v", urlmap[lfail.RequestID], lfail)
		// 					return
		// 				}
		// 				if _, ok := reqIDAll[lfail.RequestID]; ok {
		// 					fin <- errors.Errorf("network data loading failed: %s, %+v", urlmap[lfail.RequestID], lfail)
		// 					return
		// 				}
		// 			case *network.EventRequestWillBeSent:
		// 				req := d.(*network.EventRequestWillBeSent)
		// 				for mcode, t := range mcodes {
		// 					tdsuf := fmt.Sprintf("/hs_%s/%s/today.js", code, mcode)
		// 					allsuf := fmt.Sprintf("/hs_%s/%s/all.js", code, mcode)
		// 					if strings.HasSuffix(req.Request.URL, tdsuf) {
		// 						urlmap[req.RequestID] = req.Request.URL
		// 						reqIDTd[req.RequestID] = t
		// 					} else if strings.HasSuffix(req.Request.URL, allsuf) {
		// 						urlmap[req.RequestID] = req.Request.URL
		// 						reqIDAll[req.RequestID] = t
		// 					}
		// 				}
		// 			case *network.EventLoadingFinished:
		// 				res := d.(*network.EventLoadingFinished)
		// 				if t, ok := reqIDTd[res.RequestID]; ok {
		// 					data, e := network.GetResponseBody(res.RequestID).Do(ctxt, h)
		// 					if e != nil {
		// 						fin <- errors.Wrapf(e, "failed to get response body "+
		// 							"from chrome, requestId: %+v, url: %s", res.RequestID, urlmap[res.RequestID])
		// 						return
		// 					}
		// 					tdmap[t] = data
		// 				}
		// 				if t, ok := reqIDAll[res.RequestID]; ok {
		// 					data, e := network.GetResponseBody(res.RequestID).Do(ctxt, h)
		// 					if e != nil {
		// 						fin <- errors.Wrapf(e, "failed to get response body "+
		// 							"from chrome, requestId: %+v, url: %s", res.RequestID, urlmap[res.RequestID])
		// 						return
		// 					}
		// 					hismap[t] = data
		// 				}
		// 			}
		// 			if len(tdmap) == len(tabs) && len(hismap) == len(tabs) {
		// 				fin <- nil
		// 				return
		// 			}
		// 		case <-ctxt.Done():
		// 			return
		// 		}
		// 	}
		// }(echan, ctxt, fin)
		return nil
	})
}

func captureData(today, all *[]byte, mcode string, fin chan error) chromedp.Action {
	return chromedp.ActionFunc(func(ctxt context.Context) error {
		// ** The API of chromedp has been revamped **

		// th, ok := h.(*chromedp.TargetHandler)
		// if !ok {
		// 	log.Fatal("invalid Executor type")
		// }
		// echan := th.Listen(cdproto.EventNetworkRequestWillBeSent, cdproto.EventNetworkLoadingFinished,
		// 	cdproto.EventNetworkLoadingFailed)
		// go func(echan <-chan interface{}, ctxt context.Context, fin chan error) {
		// 	defer func() {
		// 		th.Release(echan)
		// 		close(fin)
		// 	}()
		// 	var (
		// 		reqIdTd, reqIdAll network.RequestID
		// 		urlTd, urlAll     string
		// 		finTd, finAll     = false, false
		// 	)
		// 	for {
		// 		select {
		// 		case d := <-echan:
		// 			switch d.(type) {
		// 			case *network.EventLoadingFailed:
		// 				lfail := d.(*network.EventLoadingFailed)
		// 				if reqIdTd == lfail.RequestID {
		// 					fin <- errors.Errorf("network data loading failed: %s, %+v", urlTd, lfail)
		// 					return
		// 				} else if reqIdAll == lfail.RequestID {
		// 					fin <- errors.Errorf("network data loading failed: %s, %+v", urlAll, lfail)
		// 					return
		// 				}
		// 			case *network.EventRequestWillBeSent:
		// 				req := d.(*network.EventRequestWillBeSent)
		// 				if strings.HasSuffix(req.Request.URL, mcode+"/today.js") {
		// 					urlTd = req.Request.URL
		// 					reqIdTd = req.RequestID
		// 				} else if strings.HasSuffix(req.Request.URL, mcode+"/all.js") {
		// 					urlAll = req.Request.URL
		// 					reqIdAll = req.RequestID
		// 				}
		// 			case *network.EventLoadingFinished:
		// 				res := d.(*network.EventLoadingFinished)
		// 				if reqIdTd == res.RequestID {
		// 					data, e := network.GetResponseBody(reqIdTd).Do(ctxt, h)
		// 					if e != nil {
		// 						fin <- errors.Wrapf(e, "failed to get response body "+
		// 							"from chrome, requestId: %+v, url: %s", reqIdTd, urlTd)
		// 						return
		// 					}
		// 					*today = data
		// 					finTd = true
		// 				} else if reqIdAll == res.RequestID {
		// 					data, e := network.GetResponseBody(reqIdAll).Do(ctxt, h)
		// 					if e != nil {
		// 						fin <- errors.Wrapf(e, "failed to get response body "+
		// 							"from chrome, requestId: %+v, url: %s", reqIdAll, urlAll)
		// 					}
		// 					*all = data
		// 					finAll = true
		// 				}
		// 			}
		// 			if finTd && finAll {
		// 				fin <- nil
		// 				return
		// 			}
		// 		case <-ctxt.Done():
		// 			return
		// 		}
		// 	}
		// }(echan, ctxt, fin)
		return nil
	})
}

// func getCdpPool() *chromedp.Pool {
// 	if pool != nil {
// 		return pool
// 	}
// 	c := make(chan os.Signal, 3)
// 	signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
// 	go func() {
// 		<-c
// 		cleanupTHS()
// 		os.Exit(1)
// 	}()
// 	var err error
// 	opt := make([]chromedp.PoolOption, 0, 4)
// 	if conf.Args.ChromeDP.Debug {
// 		opt = append(opt, chromedp.PoolLog(logrus.Infof, logrus.Debugf, logrus.Errorf))
// 	}
// 	pool, err = chromedp.NewPool(opt...)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	return pool
// }

func cleanupTHS() {
	// if pool != nil {
	// 	err := pool.Shutdown()
	// 	pool = nil
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// }
}

func dKlineThsV2(stk *model.Stock, klt model.DBTab, incr bool, ldate *string, lklid *int) (kldy []*model.Quote, suc, retry bool) {
	var (
		code   = stk.Code
		klast  model.Klast
		ktoday model.Ktoday
		body   []byte
		e      error
		dkeys  = make([]string, 0, 16)         // date as keys to sort
		klmap  = make(map[string]*model.Quote) // date - quote map to eliminate duplicates
		oldest string                          // stores the oldest date
		mode   string
		cycle  model.CYTP
		rtype  = model.Forward
	)
	//mode:
	// 00-no reinstatement
	// 01-forward reinstatement
	// 02-backward reinstatement
	switch klt {
	case model.KLINE_DAY_F:
		mode = "01"
	case model.KLINE_DAY_NR:
		mode = "00"
	default:
		log.Panicf("unhandled kltype: %s", klt)
	}
	urlToday := fmt.Sprintf("http://d.10jqka.com.cn/v6/line/hs_%s/%s/today.js", code, mode)
	body, e = util.HttpGetBytesUsingHeaders(urlToday,
		map[string]string{
			"Referer": "http://stockpage.10jqka.com.cn/HQ_v4.html",
			"Cookie":  conf.Args.DataSource.THS.Cookie})
	//body, e = util.HttpGetBytes(url_today)
	if e != nil {
		log.Printf("%s error visiting %s: \n%+v", code, urlToday, e)
		return kldy, false, false
	}
	ktoday = model.Ktoday{}
	e = json.Unmarshal(strip(body), &ktoday)
	if e != nil {
		log.Printf("%s error parsing json from %s: %s\n%+v", code, urlToday, string(body), e)
		return kldy, false, true
	}
	if ktoday.Code != "" {
		klmap[ktoday.Date] = &ktoday.Quote
		dkeys = append(dkeys, ktoday.Date)
		oldest = ktoday.Date
	} else {
		log.Printf("kline today skipped: %s", urlToday)
	}

	// If it is an IPO, return immediately
	_, e = time.Parse(global.DateFormat, ktoday.Date)
	if e != nil {
		log.Printf("%s invalid date format today: %s\n%+v", code, ktoday.Date, e)
		return kldy, false, true
	}
	if stk.TimeToMarket.Valid && len(stk.TimeToMarket.String) == 10 && ktoday.Date == stk.TimeToMarket.String {
		log.Printf("%s IPO day: %s fetch data for today only", code, stk.TimeToMarket.String)
		return append(kldy, &ktoday.Quote), true, false
	}

	//get last kline data
	urlLast := fmt.Sprintf("http://d.10jqka.com.cn/v6/line/hs_%s/%s/last.js", code, mode)
	body, e = util.HttpGetBytesUsingHeaders(urlLast,
		map[string]string{
			"Referer": "http://stockpage.10jqka.com.cn/HQ_v4.html",
			"Cookie":  conf.Args.DataSource.THS.Cookie})
	//body, e = util.HttpGetBytes(url_last)
	if e != nil {
		log.Printf("%s error visiting %s: \n%+v", code, urlLast, e)
		return kldy, false, true
	}
	klast = model.Klast{}
	e = json.Unmarshal(strip(body), &klast)
	if e != nil {
		log.Printf("%s error parsing json from %s: %s\n%+v", code, urlLast, string(body), e)
		return kldy, false, true
	} else if klast.Data == "" {
		log.Printf("%s empty data in json response from %s: %s", code, urlLast, string(body))
		return kldy, false, true
	}

	switch klt {
	case model.KLINE_DAY_B, model.KLINE_DAY_NR, model.KLINE_DAY_F:
		cycle = model.DAY
	case model.KLINE_WEEK_B, model.KLINE_WEEK_NR, model.KLINE_WEEK_F:
		cycle = model.WEEK
	case model.KLINE_MONTH_B, model.KLINE_MONTH_NR, model.KLINE_MONTH_F:
		cycle = model.MONTH
	}
	switch klt {
	case model.KLINE_DAY_NR, model.KLINE_WEEK_NR, model.KLINE_MONTH_NR:
		rtype = model.None
	case model.KLINE_DAY_B, model.KLINE_WEEK_B, model.KLINE_MONTH_B:
		rtype = model.Backward
	}

	*ldate = ""
	*lklid = -1
	if incr {
		ldy := getLatestTradeDataBasic(code, model.KlineMaster, cycle, rtype, 5+1) //plus one offset for pre-close, varate calculation
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
			urlHist := fmt.Sprintf("http://d.10jqka.com.cn/v6/line/hs_%s/%s/%d.js", code, mode,
				yr)
			body, e = util.HttpGetBytesUsingHeaders(urlHist,
				map[string]string{
					"Referer": "http://stockpage.10jqka.com.cn/HQ_v4.html",
					"Cookie":  conf.Args.DataSource.THS.Cookie})
			//body, e = util.HttpGetBytes(url_hist)
			if e != nil {
				log.Printf("%s [%d] error visiting %s: \n%+v", code, tries, urlHist, e)
				ok = false
				continue
			}
			khist := model.Khist{}
			e = json.Unmarshal(strip(body), &khist)
			if e != nil {
				log.Printf("%s [%d], error parsing json from %s: %s\n%+v", code, tries, urlHist, string(body), e)
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
		code   = stk.Code
		kall   model.KlAll
		ktoday model.Ktoday
		body   []byte
		e      error
		mode   string
		cycle  model.CYTP
		rtype  = model.Forward
	)
	*ldate = ""
	*lklid = -1
	//mode:
	// 00-no reinstatement
	// 01-forward reinstatement
	// 02-backward reinstatement
	switch klt {
	case model.KLINE_DAY_F:
		mode = "01"
	case model.KLINE_DAY_NR:
		mode = "00"
	case model.KLINE_WEEK_F:
		mode = "11"
	case model.KLINE_MONTH_F:
		mode = "21"
	default:
		log.Panicf("unhandled kltype: %s", klt)
	}
	urlToday := fmt.Sprintf("http://d.10jqka.com.cn/v6/line/hs_%s/%s/today.js", code, mode)
	body, e = util.HttpGetBytesUsingHeaders(urlToday,
		map[string]string{
			"Referer": "http://stockpage.10jqka.com.cn/HQ_v4.html",
			"Cookie":  conf.Args.DataSource.THS.Cookie})
	if e != nil {
		log.Printf("%s error visiting %s: \n%+v", code, urlToday, e)
		return quotes, false, false
	}
	ktoday = model.Ktoday{}
	e = json.Unmarshal(strip(body), &ktoday)
	if e != nil {
		log.Printf("%s error parsing json from %s: %s\n%+v", code, urlToday, string(body), e)
		return quotes, false, true
	}
	if ktoday.Code != "" {
		quotes = append(quotes, &ktoday.Quote)
	} else {
		log.Printf("kline today skipped: %s", urlToday)
	}

	_, e = time.Parse(global.DateFormat, ktoday.Date)
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
	if (klt == model.KLINE_WEEK_F || klt == model.KLINE_MONTH_F) &&
		stk.TimeToMarket.Valid && len(stk.TimeToMarket.String) == 10 {
		ttm, e := time.Parse(global.DateFormat, stk.TimeToMarket.String)
		if e != nil {
			log.Printf("%s invalid date format for \"time to market\": %s\n%+v",
				code, stk.TimeToMarket.String, e)
			return quotes, false, true
		}
		ttd, e := time.Parse(global.DateFormat, ktoday.Date)
		if e != nil {
			log.Printf("%s invalid date format for \"kline today\": %s\n%+v",
				code, ktoday.Date, e)
			return quotes, false, true
		}
		y1, w1 := ttm.ISOWeek()
		y2, w2 := ttd.ISOWeek()
		if y1 == y2 && w1 == w2 {
			log.Printf("%s IPO week %s fetch data for today only", code, stk.TimeToMarket.String)
			return quotes, true, false
		}
	}

	//get all kline data
	//e.g: http://d.10jqka.com.cn/v6/line/hs_000001/01/all.js
	urlAll := fmt.Sprintf("http://d.10jqka.com.cn/v6/line/hs_%s/%s/all.js", code, mode)
	body, e = util.HttpGetBytesUsingHeaders(urlAll,
		map[string]string{
			"Referer": "http://stockpage.10jqka.com.cn/HQ_v4.html",
			"Cookie":  conf.Args.DataSource.THS.Cookie})
	//body, e = util.HttpGetBytes(url_all)
	if e != nil {
		log.Printf("%s error visiting %s: \n%+v", code, urlAll, e)
		return quotes, false, true
	}
	kall = model.KlAll{}
	e = json.Unmarshal(strip(body), &kall)
	if e != nil {
		log.Printf("%s error parsing json from %s: %s\n%+v", code, urlAll, string(body), e)
		return quotes, false, true
	} else if kall.Price == "" {
		log.Printf("%s empty data in json response from %s: %s", code, urlAll, string(body))
		return quotes, false, true
	}

	switch klt {
	case model.KLINE_DAY_B, model.KLINE_DAY_NR, model.KLINE_DAY_F:
		cycle = model.DAY
	case model.KLINE_WEEK_B, model.KLINE_WEEK_NR, model.KLINE_WEEK_F:
		cycle = model.WEEK
	case model.KLINE_MONTH_B, model.KLINE_MONTH_NR, model.KLINE_MONTH_F:
		cycle = model.MONTH
	}
	switch klt {
	case model.KLINE_DAY_NR, model.KLINE_WEEK_NR, model.KLINE_MONTH_NR:
		rtype = model.None
	case model.KLINE_DAY_B, model.KLINE_WEEK_B, model.KLINE_MONTH_B:
		rtype = model.Backward
	}

	if incr {
		ldy := getLatestTradeDataBasic(code, model.KlineMaster, cycle, rtype, 5+1) //plus one offset for pre-close, varate calculation
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

	if (klt == model.KLINE_DAY_F || klt == model.KLINE_DAY_NR) && kls[0].Date == ktoday.Date {
		// if ktoday and kls[0] in the same month, remove kls[0]
		kls = kls[1:]
	} else if klt == model.KLINE_MONTH_F && kls[0].Date[:8] == ktoday.Date[:8] {
		// if ktoday and kls[0] in the same month, remove kls[0]
		kls = kls[1:]
	} else if klt == model.KLINE_WEEK_F {
		// if ktoday and kls[0] in the same week, remove kls[0]
		tToday, e := time.Parse(global.DateFormat, ktoday.Date)
		if e != nil {
			log.Printf("%s %s invalid date format: %+v \n %+v", code, klt, ktoday.Date, e)
			return quotes, false, true
		}
		yToday, wToday := tToday.ISOWeek()
		tHead, e := time.Parse(global.DateFormat, kls[0].Date)
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

//parse semi-colon separated string to quotes, with latest in the head (reverse order of the string data).
func parseThsKlinesV6(code string, klt model.DBTab, data *model.KlAll, ldate string) (kls []*model.Quote, e error) {
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				log.Println(err)
				e = err
			}
		}
	}()
	prices := strings.Split(data.Price, ",")
	vols := strings.Split(data.Volume, ",")
	dates := strings.Split(data.Dates, ",")
	if len(prices)/4 != len(vols) || len(vols) != len(dates) {
		return nil, errors.Errorf("%s data length mismatched: total:%d, price:%d, vols:%d, dates:%d",
			code, data.Total, len(prices), len(vols), len(dates))
	}
	pf := data.PriceFactor
	offset := 0
	for y := len(data.SortYear) - 1; y >= 0; y-- {
		yrd := data.SortYear[y].([]interface{})
		year := strconv.Itoa(int(yrd[0].(float64)))
		ynum := int(yrd[1].(float64))
		//last year's count might be one more than actually in the data string
		if y == len(data.SortYear)-1 && data.Total == len(dates)+1 {
			ynum--
			log.Printf("%s %s %+v %+v data length mismatch, auto corrected", code, data.Name, data.Total, klt)
		}
		for i := len(dates) - offset - 1; i >= len(dates)-offset-ynum; i-- {
			// latest in the last
			date := year + "-" + dates[i][0:2] + "-" + dates[i][2:]
			if ldate != "" && date <= ldate {
				return
			}
			kl := &model.Quote{}
			kl.Date = date
			kl.Low = util.Str2F64(prices[i*4]) / pf
			kl.Open = kl.Low + util.Str2F64(prices[i*4+1])/pf
			kl.High = kl.Low + util.Str2F64(prices[i*4+2])/pf
			kl.Close = kl.Low + util.Str2F64(prices[i*4+3])/pf
			kl.Volume.Float64 = util.Str2F64(vols[i])
			kl.Volume.Valid = true
			switch klt {
			case model.KLINE_DAY_NR, model.KLINE_WEEK_NR, model.KLINE_MONTH_NR:
				if kl.Open == 0 {
					kl.Open = kl.Close
				}
				if kl.Low == 0 {
					kl.Low = kl.Close
				}
				if kl.High == 0 {
					kl.High = kl.Close
				}
			case model.KLINE_DAY_F:
				if kl.Open == 0 && kl.Low == 0 && kl.High == 0 && kl.Close != 0 {
					kl.Open = kl.Close
					kl.Low = kl.Close
					kl.High = kl.Close
				}
			}
			kl.Code = code
			kls = append(kls, kl)
		}
		offset += ynum
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
					return
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
				kl.Volume = sql.NullFloat64{Float64: util.Str2F64(e), Valid: true}
			case 6:
				kl.Amount = sql.NullFloat64{Float64: util.Str2F64(e), Valid: true}
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

func longKlineThs(stk *model.Stock, klt model.DBTab, incr bool) (quotes []*model.Quote, suc bool) {
	urlt := "http://d.10jqka.com.cn/v6/line/hs_%s/%s/last.js"
	var (
		code  = stk.Code
		typ   string
		dkeys = make([]string, 0, 16)         // date as keys to sort
		klmap = make(map[string]*model.Quote) // date - quote map to eliminate duplicates
		cycle model.CYTP
		rtype = model.Forward
	)
	switch klt {
	case model.KLINE_WEEK_F:
		typ = "11"
	case model.KLINE_MONTH_F:
		typ = "21"
	default:
		log.Panicf("unhandled kltype: %s", klt)
	}

	switch klt {
	case model.KLINE_DAY_B, model.KLINE_DAY_NR, model.KLINE_DAY_F:
		cycle = model.DAY
	case model.KLINE_WEEK_B, model.KLINE_WEEK_NR, model.KLINE_WEEK_F:
		cycle = model.WEEK
	case model.KLINE_MONTH_B, model.KLINE_MONTH_NR, model.KLINE_MONTH_F:
		cycle = model.MONTH
	}
	switch klt {
	case model.KLINE_DAY_NR, model.KLINE_WEEK_NR, model.KLINE_MONTH_NR:
		rtype = model.None
	case model.KLINE_DAY_B, model.KLINE_WEEK_B, model.KLINE_MONTH_B:
		rtype = model.Backward
	}

	ldate := ""
	lklid := -1
	if incr {
		latest := getLatestTradeDataBasic(code, model.KlineMaster, cycle, rtype, 5+1) //plus one offset for pre-close, varate calculation
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
			ttm, e := time.Parse(global.DateFormat, stk.TimeToMarket.String)
			if e != nil {
				log.Printf("%s invalid date format for \"time to market\": %s\n%+v",
					code, stk.TimeToMarket.String, e)
			} else {
				ttd, e := time.Parse(global.DateFormat, ktoday.Date)
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
		body, e := util.HttpGetBytesUsingHeaders(url,
			map[string]string{
				"Referer": "http://stockpage.10jqka.com.cn/HQ_v4.html",
				"Cookie":  conf.Args.DataSource.THS.Cookie})
		//body, e := util.HttpGetBytes(url)
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
			tToday, e := time.Parse(global.DateFormat, ktoday.Date)
			if e != nil {
				log.Printf("%s %s [%d] invalid date format %+v", code, klt, rt+1, e)
				continue
			}
			yToday, wToday := tToday.ISOWeek()
			tHead, e := time.Parse(global.DateFormat, kls[0].Date)
			if e != nil {
				log.Printf("%s %s [%d] invalid date format %+v", code, klt, rt+1, e)
				continue
			}
			yLast, wLast := tHead.ISOWeek()
			if yToday == yLast && wToday == wLast {
				kls = kls[1:]
			}
			// if cytp is month, and ktoday and kls[0] in the same month, remove kls[0]
			if len(kls) > 0 && klt == model.KLINE_MONTH_F && kls[0].Date[:8] == ktoday.Date[:8] {
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
		supplementMisc(quotes, klt, lklid)
		if ldate != "" {
			// skip the first record which is for varate calculation
			quotes = quotes[1:]
		}
		binsert(quotes, string(klt), lklid)
	}
	return quotes, true
}

func strip(data []byte) []byte {
	s := bytes.IndexByte(data, 40)     // first occurrence of '('
	e := bytes.LastIndexByte(data, 41) // last occurrence of ')'
	if s >= 0 && e >= 0 {
		return data[s+1 : e]
	}
	return data
}

func getToday(code string, typ string) (q *model.Quote, ok, retry bool) {
	urlToday := fmt.Sprintf("http://d.10jqka.com.cn/v6/line/hs_%s/%s/today.js", code, typ)
	body, e := util.HttpGetBytesUsingHeaders(urlToday,
		map[string]string{
			"Referer": "http://stockpage.10jqka.com.cn/HQ_v4.html",
			"Cookie":  conf.Args.DataSource.THS.Cookie})
	//body, e := util.HttpGetBytes(url_today)
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
