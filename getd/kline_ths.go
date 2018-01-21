package getd

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
	"github.com/chromedp/chromedp/runner"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	pool  *chromedp.Pool
	dt2mc = map[model.DBTab]string{
		model.KLINE_DAY_NR:   "00",
		model.KLINE_DAY:      "01",
		model.KLINE_WEEK_NR:  "10",
		model.KLINE_WEEK:     "11",
		model.KLINE_MONTH_NR: "20",
		model.KLINE_MONTH:    "21",
	}
)

// This version trys to run fetch for multiple kline types in a single chrome instance,
// may improve performance to some degree
func getKlineThs(stk *model.Stock, kltype []model.DBTab) (qmap map[model.DBTab][]*model.Quote, suc bool) {
	RETRIES := 20
	var (
		code = stk.Code
	)

	for rt := 0; rt < RETRIES; rt++ {
		klsMap, suc, retry := klineThsCDPv2(stk, kltype)
		if suc {
			qmap = klsMap
			break
		} else {
			if retry && rt+1 < RETRIES {
				log.Printf("%s retrying [%d]", code, rt+1)
				time.Sleep(time.Millisecond * 2500)
				continue
			} else {
				log.Printf("%s failed", code)
				return qmap, false
			}
		}
	}
	return
}

func klineThs(stk *model.Stock, klt model.DBTab, incr bool) (quotes []*model.Quote, suc bool) {
	RETRIES := 20
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

	supplementMisc(quotes, lklid)
	if ldate != "" {
		//skip the first record which is for varate calculation
		quotes = quotes[1:]
	}
	binsert(quotes, string(klt), lklid)
	return quotes, true
}

func klineThsCDPv2(stk *model.Stock, kltype []model.DBTab) (qmap map[model.DBTab][]*model.Quote, suc, retry bool) {
	suc, retry, tdmap, hismap := runCdpV2(stk.Code, kltype)
	if !suc {
		return qmap, false, retry
	}
	xdxr := latestUFRXdxr(stk.Code)
	lkmap := make(map[model.DBTab]int)
	for t, tdat := range tdmap {
		quotes, lklid, suc, retry := byte2Quote(stk, t, tdat, hismap[t], xdxr)
		if !suc {
			return qmap, false, retry
		}
		qmap[t] = quotes
		lkmap[t] = lklid
	}
	for klt, quotes := range qmap {
		supplementMisc(quotes, lkmap[klt])
		if lkmap[klt] != -1 {
			//skip the first record which is for varate calculation
			quotes = quotes[1:]
		}
	}
	calcVarateRgl(stk, qmap)
	for klt, quotes := range qmap {
		binsert(quotes, string(klt), lkmap[klt])
	}
	return qmap, true, false
}

func calcVarateRgl(stk *model.Stock, qmap map[model.DBTab][]*model.Quote) {
	for t, qs := range qmap {
		switch t {
		case model.KLINE_DAY_NR, model.KLINE_WEEK_NR, model.KLINE_MONTH_NR:
			continue
		case model.KLINE_DAY:
			inferVarateRgl(stk, model.KLINE_DAY_NR, qmap[model.KLINE_DAY_NR], qs)
		case model.KLINE_WEEK:
			inferVarateRgl(stk, model.KLINE_WEEK_NR, qmap[model.KLINE_WEEK_NR], qs)
		case model.KLINE_MONTH:
			inferVarateRgl(stk, model.KLINE_MONTH_NR, qmap[model.KLINE_MONTH_NR], qs)
		default:
			log.Printf("%s can't calculate varate_rgl for unsupported type: %v", stk.Code, t)
		}
	}
}

func inferVarateRgl(stk *model.Stock, tab model.DBTab, nrqs []*model.Quote, tgqs []*model.Quote) {
	if tgqs == nil || len(tgqs) == 0 {
		log.Printf("%s unable to infer varate_rgl from %v. please provide valid target quotes parameter.",
			stk.Code, tab)
		return
	}
	sDate, eDate := tgqs[0].Date, tgqs[len(tgqs)-1].Date
	if nrqs == nil || len(nrqs) == 0 {
		//load nrqs from db
		qry := fmt.Sprintf(`select * from %s where code = ? and date between ? and ? order by date desc`, tab)
		_, e := dbmap.Select(&nrqs, qry, stk.Code, sDate, eDate)
		if e != nil {
			log.Printf("%s unable to infer varate_rgl from %v: %+v", stk.Code, tab, e)
			return
		}
	}
	if len(nrqs) != len(tgqs) {
		log.Printf("%s unable to infer varate_rgl from %v. len(nrqs)=%d, len(tgqs)=%d",
			stk.Code, tab, len(nrqs), len(tgqs))
		return
	}
	// handle xdxr events
	rows, e := dbmap.Query(`select code, xdxr_date, sum(divi), sum(shares_allot), sum(shares_cvt) `+
		`from xdxr where code = ? and xdxr_date between ? and ? group by xdxr_date`, stk.Code, sDate, eDate)
	if e != nil {
		if e != sql.ErrNoRows {
			log.Printf("%s unable to infer varate_rgl from %v: %+v", stk.Code, tab, e)
		}
		return
	}
	defer rows.Close()
	xemap := make(map[string]*model.Xdxr)
	var (
		code, xdate          string
		divi, shallot, shcvt sql.NullFloat64
	)
	for rows.Next() {
		e = rows.Scan(&code, &xdate, &divi, &shallot, &shcvt)
		if e != nil {
			log.Printf("%s failed to infer varate_rgl from %v: %+v", stk.Code, tab, e)
			return
		}
		xemap[xdate] = &model.Xdxr{
			Code:        code,
			XdxrDate:    sql.NullString{Valid: true, String: xdate},
			Divi:        divi,
			SharesAllot: shallot,
			SharesCvt:   shcvt,
		}
	}
	if e = rows.Err(); e != nil {
		log.Printf("%s failed to infer varate_rgl from %v: %+v", stk.Code, tab, e)
	}
	// begin varate transferation
	for i := 0; i < len(tgqs); i++ {
		nrq := nrqs[i]
		tgq := tgqs[i]
		if nrq.Code != tgq.Code || nrq.Date != tgq.Date || nrq.Klid != tgq.Klid {
			log.Printf("%s unable to infer varate_rgl from %v. unmatched nrq & tgq at %d: %+v : %+v",
				stk.Code, tab, i, nrq, tgq)
			return
		}
		tvar := nrq.Varate.Float64
		if len(xemap) > 0 {
			if xe, ok := xemap[tgq.Date]; ok && i-1 >= 0 {
				// adjust fore-day price for regulated varate calculation
				pcl := nrqs[i-1].Close
				d, sa, sc := 0., 0., 0.
				if xe.Divi.Valid {
					d = xe.Divi.Float64
				}
				if xe.SharesAllot.Valid {
					sa = xe.SharesAllot.Float64
				}
				if xe.SharesCvt.Valid {
					sc = xe.SharesCvt.Float64
				}
				pcl = (pcl*10.0 - d) / (10.0 + sa + sc)
				tvar = (nrq.Close - pcl) / pcl * 100.
			}
		}
		tgq.VarateRgl.Valid = true
		tgq.VarateRgl.Float64 = tvar
	}
	return
}

func byte2Quote(stk *model.Stock, klt model.DBTab, today, all []byte, xdxr *model.Xdxr) (
	quotes []*model.Quote, lklid int, suc, retry bool) {
	var (
		code   = stk.Code
		ktoday = model.Ktoday{}
		kall   = model.KlAll{}
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

	ttd, e := time.Parse("2006-01-02", ktoday.Date)
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
	case model.KLINE_WEEK, model.KLINE_WEEK_NR, model.KLINE_MONTH, model.KLINE_MONTH_NR:
		if stk.TimeToMarket.Valid && len(stk.TimeToMarket.String) == 10 {
			ttm, e := time.Parse("2006-01-02", stk.TimeToMarket.String)
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
	case model.KLINE_DAY, model.KLINE_WEEK, model.KLINE_MONTH:
		incr = xdxr == nil
	}
	ldate := ""
	if incr {
		ldy := getLatestKl(code, klt, 5+1) //plus one offset for pre-close, varate calculation
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
	case model.KLINE_DAY, model.KLINE_DAY_NR:
		// if ktoday and kls[0] in the same day, remove kls[0]
		if kls[0].Date == ktoday.Date {
			kls = kls[1:]
		}
	case model.KLINE_WEEK, model.KLINE_WEEK_NR:
		// if ktoday and kls[0] in the same week, remove kls[0]
		yToday, wToday := ttd.ISOWeek()
		tHead, e := time.Parse("2006-01-02", kls[0].Date)
		if e != nil {
			log.Printf("%s %s invalid date format: %+v \n %+v", code, klt, kls[0].Date, e)
			return quotes, -1, false, true
		}
		yLast, wLast := tHead.ISOWeek()
		if yToday == yLast && wToday == wLast {
			kls = kls[1:]
		}
	case model.KLINE_MONTH, model.KLINE_MONTH_NR:
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
	kall = model.KlAll{}
	e = json.Unmarshal(strip(all), &kall)
	if e != nil {
		log.Printf("%s error parsing json for %+v: %s\n%+v", code, klt, string(all), e)
		return quotes, false, true
	} else if kall.Price == "" {
		log.Printf("%s %+v empty price data in json response: %s", code, klt, string(all))
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

func runCdpV2(code string, tabs []model.DBTab) (ok, retry bool, tdmap, hismap map[model.DBTab][]byte) {
	// create context
	ctxt, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	// get chrome runner from the pool
	pr, err := getCdpPool().Allocate(ctxt,
		runner.Flag("headless", true),
		runner.Flag("no-default-browser-check", true),
		runner.Flag("no-first-run", true),
		runner.ExecPath(conf.Args.ChromeDP.Path),
	)
	if err != nil {
		log.Printf("%s failed to allocate chrome runner from the pool: %+v\n", code, err)
		return false, true, tdmap, hismap
	}
	defer pr.Release()
	chr := make(chan bool)
	go func(chr chan bool) {
		err = pr.Run(ctxt, buildBatchActions(code, tabs, tdmap, hismap))
		if err != nil {
			log.Printf("chrome runner reported error: %+v\n", err)
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
	// create context
	ctxt, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// get chrome runner from the pool
	pr, err := getCdpPool().Allocate(ctxt,
		runner.Flag("headless", true),
		runner.Flag("no-default-browser-check", true),
		runner.Flag("no-first-run", true),
		runner.ExecPath(conf.Args.ChromeDP.Path),
	)
	if err != nil {
		log.Printf("%s failed to allocate chrome runner from the pool: %+v\n", code, err)
		return false, true, today, all
	}
	defer pr.Release()
	chr := make(chan bool)
	go func(chr chan bool) {
		err = pr.Run(ctxt, buildActions(code, tab, &today, &all))
		if err != nil {
			log.Printf("chrome runner reported error: %+v\n", err)
			chr <- false
		} else {
			chr <- true
		}
	}(chr)
	select {
	case ok = <-chr:
		return ok, !ok, today, all
	case <-ctxt.Done():
		if ctxt.Err() != nil {
			log.Printf("%s timeout waiting for chromedp response", code)
			return false, true, today, all
		}
		return true, false, today, all
	}
}

func buildBatchActions(code string, tabs []model.DBTab, tdmap, hismap map[model.DBTab][]byte) chromedp.Tasks {
	fin := make(chan error)
	url := fmt.Sprintf(`http://stockpage.10jqka.com.cn/HQ_v4.html#hs_%s`, code)
	//.hxc3-hxc3KlinePricePane-hover visible indicates kline data loaded
	ltag := `.hxc3-hxc3KlinePricePane-hover`
	tasks := chromedp.Tasks{
		chromedp.Navigate(url),
		batchCaptureData(tdmap, hismap, tabs, fin),
	}
	for _, t := range tabs {
		sel := `a[hxc3-data-type^="hxc3Kline"][hxc3-data-type$="%s"]`
		switch t {
		case model.KLINE_DAY_NR, model.KLINE_DAY:
			sel = fmt.Sprintf(sel, "Day")
		case model.KLINE_WEEK_NR, model.KLINE_WEEK:
			sel = fmt.Sprintf(sel, "Week")
		case model.KLINE_MONTH_NR, model.KLINE_MONTH:
			sel = fmt.Sprintf(sel, "Month")
		default:
			log.Panicf("unsupported DBTab: %+v", t)
		}
		tasks = append(tasks,
			chromedp.WaitVisible(sel, chromedp.ByQuery),
			chromedp.Click(sel, chromedp.ByQuery),
		)
		fqSel := `a[data-type="%s"]`
		if t == model.KLINE_DAY_NR || t == model.KLINE_WEEK_NR || t == model.KLINE_MONTH_NR {
			fqSel = fmt.Sprintf(fqSel, "Bfq")
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
	return chromedp.ActionFunc(func(ctxt context.Context, h cdp.Executor) error {
		select {
		case <-ctxt.Done():
			return nil
		case e := <-fin:
			return e
		}
	})
}

func batchCaptureData(tdmap, hismap map[model.DBTab][]byte, tabs []model.DBTab, fin chan error) chromedp.Action {
	mcodes := make(map[string]model.DBTab)
	for _, t := range tabs {
		mcodes[dt2mc[t]] = t
	}
	return chromedp.ActionFunc(func(ctxt context.Context, h cdp.Executor) error {
		th, ok := h.(*chromedp.TargetHandler)
		if !ok {
			log.Fatal("invalid Executor type")
		}
		echan := th.Listen(cdproto.EventNetworkRequestWillBeSent, cdproto.EventNetworkLoadingFinished,
			cdproto.EventNetworkLoadingFailed)
		go func(echan <-chan interface{}, ctxt context.Context, fin chan error) {
			defer func() {
				th.Release(echan)
				close(fin)
			}()
			reqIDTd := make(map[network.RequestID]model.DBTab)
			reqIDAll := make(map[network.RequestID]model.DBTab)
			urlmap := make(map[network.RequestID]string)
			for {
				select {
				case d := <-echan:
					switch d.(type) {
					case *network.EventLoadingFailed:
						lfail := d.(*network.EventLoadingFailed)
						if _, ok := reqIDTd[lfail.RequestID]; ok {
							fin <- errors.Errorf("network data loading failed: %s, %+v", urlmap[lfail.RequestID], lfail)
							return
						}
						if _, ok := reqIDAll[lfail.RequestID]; ok {
							fin <- errors.Errorf("network data loading failed: %s, %+v", urlmap[lfail.RequestID], lfail)
							return
						}
					case *network.EventRequestWillBeSent:
						req := d.(*network.EventRequestWillBeSent)
						for mcode, t := range mcodes {
							if strings.HasSuffix(req.Request.URL, mcode+"/today.js") {
								urlmap[req.RequestID] = req.Request.URL
								reqIDTd[req.RequestID] = t
							} else if strings.HasSuffix(req.Request.URL, mcode+"/all.js") {
								urlmap[req.RequestID] = req.Request.URL
								reqIDAll[req.RequestID] = t
							}
						}
					case *network.EventLoadingFinished:
						res := d.(*network.EventLoadingFinished)
						if t, ok := reqIDTd[res.RequestID]; ok {
							data, e := network.GetResponseBody(res.RequestID).Do(ctxt, h)
							if e != nil {
								fin <- errors.Wrapf(e, "failed to get response body "+
									"from chrome, requestId: %+v, url: %s", res.RequestID, urlmap[res.RequestID])
								return
							}
							tdmap[t] = data
						}
						if t, ok := reqIDAll[res.RequestID]; ok {
							data, e := network.GetResponseBody(res.RequestID).Do(ctxt, h)
							if e != nil {
								fin <- errors.Wrapf(e, "failed to get response body "+
									"from chrome, requestId: %+v, url: %s", res.RequestID, urlmap[res.RequestID])
								return
							}
							hismap[t] = data
						}
					}
					if len(tdmap) == len(tabs) && len(hismap) == len(tabs) {
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

func captureData(today, all *[]byte, mcode string, fin chan error) chromedp.Action {
	return chromedp.ActionFunc(func(ctxt context.Context, h cdp.Executor) error {
		th, ok := h.(*chromedp.TargetHandler)
		if !ok {
			log.Fatal("invalid Executor type")
		}
		echan := th.Listen(cdproto.EventNetworkRequestWillBeSent, cdproto.EventNetworkLoadingFinished,
			cdproto.EventNetworkLoadingFailed)
		go func(echan <-chan interface{}, ctxt context.Context, fin chan error) {
			defer func() {
				th.Release(echan)
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
							*today = data
							finTd = true
						} else if reqIdAll == res.RequestID {
							data, e := network.GetResponseBody(reqIdAll).Do(ctxt, h)
							if e != nil {
								fin <- errors.Wrapf(e, "failed to get response body "+
									"from chrome, requestId: %+v, url: %s", reqIdAll, urlAll)
							}
							*all = data
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
	opt := make([]chromedp.PoolOption, 0, 4)
	if conf.Args.ChromeDP.Debug {
		opt = append(opt, chromedp.PoolLog(logrus.Infof, logrus.Debugf, logrus.Errorf))
	}
	pool, err = chromedp.NewPool(opt...)
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
			kl.Volume = sql.NullFloat64{util.Str2F64(vols[i]), true}
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
				kl.Volume = sql.NullFloat64{util.Str2F64(e), true}
			case 6:
				kl.Amount = sql.NullFloat64{util.Str2F64(e), true}
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
		body, e := util.HttpGetBytesUsingHeaders(url,
			map[string]string{
				"Referer": "http://stockpage.10jqka.com.cn/HQ_v4.html",
				"Cookie":  conf.Args.Datasource.ThsCookie})
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

func strip(data []byte) []byte {
	s := bytes.IndexByte(data, 40)     // first occurrence of '('
	e := bytes.LastIndexByte(data, 41) // last occurrence of ')'
	if s >= 0 && e >= 0 {
		return data[s+1 : e]
	} else {
		return data
	}
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
		} else {
			cc := klines[i].Close
			if preclose == 0 && cc == 0 {
				klines[i].Varate.Float64 = 0
			} else if preclose == 0 {
				klines[i].Varate.Float64 = cc / .01 * 100.
			} else if cc == 0 {
				klines[i].Varate.Float64 = (-0.01 - preclose) / math.Abs(preclose) * 100.
			} else {
				klines[i].Varate.Float64 = (cc - preclose) / math.Abs(preclose) * 100.
			}
		}
		preclose = klines[i].Close
	}
}

func getToday(code string, typ string) (q *model.Quote, ok, retry bool) {
	url_today := fmt.Sprintf("http://d.10jqka.com.cn/v6/line/hs_%s/%s/today.js", code, typ)
	body, e := util.HttpGetBytesUsingHeaders(url_today,
		map[string]string{
			"Referer": "http://stockpage.10jqka.com.cn/HQ_v4.html",
			"Cookie":  conf.Args.Datasource.ThsCookie})
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
