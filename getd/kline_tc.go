package getd

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
)

func klineTc(stk *model.Stock, tab model.DBTab, incr bool) (kldy []*model.Quote, suc bool) {
	RETRIES := 20
	var (
		kls   []*model.Quote
		retry bool
		lklid int
		code  = stk.Code
	)

	for rt := 0; rt < RETRIES; rt++ {
		kls, suc, retry, lklid = tryKlineTc(stk, tab, incr)
		if suc {
			kldy = kls
			break
		} else {
			if retry && rt+1 < RETRIES {
				log.Printf("%s retrying to get %s [%d]", code, tab, rt+1)
				time.Sleep(time.Millisecond * 500)
				continue
			} else {
				log.Printf("%s failed to get %s", code, tab)
				return kldy, false
			}
		}
	}

	if lklid >= 0 {
		//skip the first record which is for varate calculation
		kldy = kldy[1:]
	}
	binsert(kldy, string(tab), lklid)
	return kldy, true
}

func tryKlineTc(stock *model.Stock, tab model.DBTab, incr bool) (quotes []*model.Quote, ok, retry bool, sklid int) {
	var (
		fcode    = strings.ToLower(stock.Market.String) + stock.Code
		code     = stock.Code
		body     []byte
		e        error
		url, per string
		ldate    = ""
		nrec     = 800 // for non-index, reinstated data, at most 800 records at a time
	)

	sklid = -1
	if incr {
		ldy := getLatestKl(code, tab, nrec)
		if ldy != nil {
			ldate = ldy.Date
			sklid = ldy.Klid
		} else {
			log.Printf("%s latest %s data not found, will be fully refreshed", code, tab)
		}
	} else {
		log.Printf("%s %s data will be fully refreshed", code, tab)
	}

	if sklid < 0 {
		ldate = "1988-01-01"
		if tab == model.KLINE_DAY_NR || stock.IsIndex {
			nrec = 10000 + rand.Intn(10000)
		}
	}

	lklid := sklid
	if lklid < 0 {
		lklid = 0
	}
	// [1]: reinstatement-fqkline/get, no reinstatement-kline/kline
	// [2]: lower case market id + stock code, e.g. sz000001
	// [3]: cycle type: day, week, month, year
	// [4]: end date
	// [5]: number of records to return
	// [6]: for reinstatement, use 'qfq'
	urlt := `http://web.ifzq.gtimg.cn/appstock/app/%[1]s?param=%[2]s,%[3]s,%[4]s,%[5]s,%[6]d,%[7]s`
	for {
		switch tab {
		case model.KLINE_DAY:
			per = "day"
			url = fmt.Sprintf(urlt, "fqkline/get", fcode, per, ldate, "", nrec, "qfq")
		case model.KLINE_DAY_NR:
			per = "day"
			url = fmt.Sprintf(urlt, "kline/kline", fcode, per, ldate, "", nrec, "")
		case model.KLINE_WEEK:
			per = "week"
			url = fmt.Sprintf(urlt, "fqkline/get", fcode, per, ldate, "", nrec, "qfq")
		case model.KLINE_MONTH:
			per = "month"
			url = fmt.Sprintf(urlt, "fqkline/get", fcode, per, ldate, "", nrec, "qfq")
		default:
			log.Panicf("unhandled kltype: %s", tab)
		}

		//get kline data
		body, e = util.HttpGetBytes(url)
		if e != nil {
			log.Printf("%s error visiting %s: \n%+v", code, url, e)
			return quotes, false, true, sklid
		}

		qj := &model.QQJson{}
		qj.Code = code
		qj.Fcode = fcode
		qj.Period = per
		e = json.Unmarshal(body, qj)
		if e != nil {
			log.Printf("failed to parse json from %s\n%+v", url, e)
			return quotes, false, true, sklid
		}
		if len(qj.Quotes) > 0 && ldate != "" && qj.Quotes[0].Date != ldate {
			log.Printf("start date %s mismatched with database: %s", qj.Quotes[0], ldate)
			return quotes, false, true, sklid
		}
		if len(qj.Quotes) > 0 {
			quotes = append(quotes, qj.Quotes...)
		} else {
			break
		}
		if len(qj.Quotes) == nrec {
			// need to fetch more
			lq := qj.Quotes[len(qj.Quotes)-1]
			lt, e := time.Parse("2016-01-02", lq.Date)
			if e != nil {
				log.Printf("invalid date format in %+v", lq)
			}
			lt.AddDate(0, 0, 1)
			ldate = lt.Format("2016-01-02")
			lklid = lq.Klid + 1
		} else {
			break
		}
	}

	return quotes, true, false, sklid
}
