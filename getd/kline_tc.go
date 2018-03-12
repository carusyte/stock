package getd

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
)

func klineTc(stk *model.Stock, tab model.DBTab, incr bool) (quotes []*model.Quote, suc bool) {
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
			quotes = kls
			break
		} else {
			if retry && rt+1 < RETRIES {
				log.Printf("%s retrying to get %s [%d]", code, tab, rt+1)
				time.Sleep(time.Millisecond * time.Duration(500+rand.Intn(1500)))
				continue
			} else {
				log.Printf("%s failed to get %s", code, tab)
				return quotes, false
			}
		}
	}

	supplementMisc(quotes, tab, lklid)

	if lklid >= 0 {
		//skip the first record which is for varate calculation
		quotes = quotes[1:]
	}
	binsert(quotes, string(tab), lklid)
	return quotes, true
}

func tryKlineTc(stock *model.Stock, tab model.DBTab, incr bool) (quotes []*model.Quote, ok, retry bool, sklid int) {
	var (
		fcode    = strings.ToLower(stock.Market.String) + stock.Code
		code     = stock.Code
		body     []byte
		e        error
		url, per string
		sDate    = ""
		eDate    = ""
		nrec     = 800 // for non-index, reinstated data, at most 800 records at a time
	)

	sklid = -1
	if incr {
		ldy := getLatestKl(code, tab, 5+1) // plus one for varate calculation
		if ldy != nil {
			sDate = ldy.Date
			sklid = ldy.Klid
			sTime, e := time.Parse("2006-01-02", sDate)
			if e != nil {
				logrus.Errorf("failed to parse date: %+v", ldy)
				return
			}
			nrec = int(time.Since(sTime).Hours()/24) + 1
		} else {
			log.Printf("%s latest %s data not found, will be fully refreshed", code, tab)
		}
	} else {
		log.Printf("%s %s data will be fully refreshed", code, tab)
	}

	if tab == model.KLINE_DAY_NR || tab == model.KLINE_WEEK_NR || tab == model.KLINE_MONTH_NR || stock.IsIndex {
		nrec = 7000 + rand.Intn(2000)
	}

	// [1]: reinstatement-fqkline/get, no reinstatement-kline/kline
	// [2]: lower case market id + stock code, e.g. sz000001
	// [3]: cycle type: day, week, month, year
	// [4]: end date
	// [5]: number of records to return
	// [6]: for reinstatement, use 'qfq'
	urlt := `http://web.ifzq.gtimg.cn/appstock/app/%[1]s?param=%[2]s,%[3]s,%[4]s,%[5]s,%[6]d,%[7]s`
	eDate = time.Now().Format("2006-01-02")
	for {
		//fetch klines backward
		switch tab {
		case model.KLINE_DAY, model.KLINE_DAY_VLD:
			per = "day"
			url = fmt.Sprintf(urlt, "fqkline/get", fcode, per, "", eDate, nrec, "qfq")
		case model.KLINE_DAY_NR:
			per = "day"
			url = fmt.Sprintf(urlt, "kline/kline", fcode, per, "", eDate, nrec, "")
		case model.KLINE_WEEK, model.KLINE_WEEK_VLD:
			per = "week"
			url = fmt.Sprintf(urlt, "fqkline/get", fcode, per, "", eDate, nrec, "qfq")
		case model.KLINE_MONTH, model.KLINE_MONTH_VLD:
			per = "month"
			url = fmt.Sprintf(urlt, "fqkline/get", fcode, per, "", eDate, nrec, "qfq")
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
		fin := false
		if len(qj.Quotes) > 0 {
			//extract data backward till sDate (excluded)
			for i := len(qj.Quotes) - 1; i >= 0; i-- {
				q := qj.Quotes[i]
				if q.Date == sDate {
					fin = true
					break
				}
				quotes = append(quotes, q)
			}
		} else {
			break
		}
		if fin || len(qj.Quotes) < nrec {
			break
		}
		// need to fetch more
		first := qj.Quotes[0]
		iDate, e := time.Parse("2006-01-02", first.Date)
		if e != nil {
			log.Printf("invalid date format in %+v", first)
		}
		eDate = iDate.AddDate(0, 0, -1).Format("2006-01-02")
	}
	//reverse, into ascending order
	for i, j := 0, len(quotes)-1; i < j; i, j = i+1, j-1 {
		quotes[i], quotes[j] = quotes[j], quotes[i]
	}
	return quotes, true, false, sklid
}
