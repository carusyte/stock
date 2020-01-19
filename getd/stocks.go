package getd

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"github.com/ssgreg/repeat"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

var (
	xqCookies []*http.Cookie
)

//StocksDb loads all stocks from basics table.
func StocksDb() (allstk []*model.Stock) {
	dbmap.Select(&allstk, "select * from basics")
	return
}

//StocksDbByCode load stocks of specified codes from the basics table.
func StocksDbByCode(code ...string) (stocks []*model.Stock) {
	sql := fmt.Sprintf("select * from basics where code in (%s)", util.Join(code, ",", true))
	_, e := dbmap.Select(&stocks, sql)
	if e != nil {
		if "sql: no rows in result set" == e.Error() {
			return
		}
		log.Panicln("failed to run sql", e)
	}
	return
}

//StocksDbTo load all stock data from basics into specified interface{}
func StocksDbTo(target interface{}) {
	dbmap.Select(target, "select * from basics")
	return
}

//GetStockInfo fetches basic stocks info from remote servers.
func GetStockInfo() (allstk *model.Stocks) {
	//allstk = getFrom10jqk()
	//allstk = getFromQq()
	allstk = getFromExchanges()
	log.Printf("total stocks: %d", allstk.Size())

	getIndustry(allstk)
	getShares(allstk)

	overwrite(allstk.List)

	return
}

func getIndustry(stocks *model.Stocks) {
	log.Println("getting industry info...")
	var wg sync.WaitGroup
	chstk := make(chan *model.Stock, global.JOB_CAPACITY)
	chrstk := make(chan *model.Stock, global.JOB_CAPACITY)
	rstks := new(model.Stocks)
	wgr := collect(rstks, chrstk)
	pl := conf.Args.Concurrency
	if conf.Args.DataSource.Industry == conf.THS {
		pl = conf.Args.DataSource.ThsConcurrency
	}
	for i := 0; i < pl; i++ {
		wg.Add(1)
		go doGetIndustry(chstk, chrstk, &wg)
	}
	for _, s := range stocks.List {
		chstk <- s
	}
	close(chstk)
	wg.Wait()
	close(chrstk)
	wgr.Wait()
	log.Printf("%d industry info fetched", rstks.Size())
	if stocks.Size() != rstks.Size() {
		same, skp := stocks.Diff(rstks)
		if !same {
			log.Printf("Failed: %+v", skp)
		}
	}
	return
}

func getShares(stocks *model.Stocks) {
	log.Println("getting share info...")
	var wg sync.WaitGroup
	chstk := make(chan *model.Stock, global.JOB_CAPACITY)
	chrstk := make(chan *model.Stock, global.JOB_CAPACITY)
	rstks := new(model.Stocks)
	wgr := collect(rstks, chrstk)
	for i := 0; i < conf.Args.Concurrency; i++ {
		wg.Add(1)
		go doGetShares(chstk, chrstk, &wg)
	}
	for _, s := range stocks.List {
		chstk <- s
	}
	close(chstk)
	wg.Wait()
	close(chrstk)
	wgr.Wait()
	log.Printf("%d share info fetched", rstks.Size())
	if stocks.Size() != rstks.Size() {
		same, skp := stocks.Diff(rstks)
		if !same {
			log.Printf("Failed: %+v", skp)
		}
	}
	return
}

func doGetIndustry(chstk, chrstk chan *model.Stock, wg *sync.WaitGroup) {
	defer wg.Done()
	// target web server can't withstand heavy traffic
	for stock := range chstk {
		for rtCount := 0; rtCount <= conf.Args.DataSource.KlineFailureRetry; rtCount++ {
			var ok, r bool
			switch conf.Args.DataSource.Industry {
			case conf.TENCENT_TC, conf.TENCENT_CSRC:
				ok, r = tcIndustry(stock)
			case conf.THS:
				ok, r = thsIndustry(stock)
			default:
				panic("unable to get industry, unsupported source: " + conf.Args.DataSource.Industry)
			}
			if ok {
				chrstk <- stock
			} else if r {
				log.Infof("%s retrying %d...", stock.Code, rtCount+1)
				time.Sleep(time.Millisecond * time.Duration(1000+rand.Intn(1000)))
				continue
			} else {
				log.Warnf("%s retried %d, giving up. restart the program to recover", stock.Code, rtCount+1)
			}
			break
		}
	}
}

func doGetShares(chstk, chrstk chan *model.Stock, wg *sync.WaitGroup) {
	defer wg.Done()
	var e error
	RETRIES := conf.Args.DefaultRetry
	for stock := range chstk {
		var ok, r bool
		for rtCount := 0; rtCount <= RETRIES; rtCount++ {
			ok, r = thsShares(stock)
			if ok {
				chrstk <- stock
			} else if r {
				log.Infof("%s retrying %d...", stock.Code, rtCount+1)
				time.Sleep(time.Millisecond * time.Duration(500+rand.Intn(1000)))
				continue
			} else {
				log.Warnf("%s retried %d, giving up. ", stock.Code, rtCount+1)
			}
			break
		}
		if ok {
			continue
		}
		log.Printf("%s switching to secondary source xueqiu.com", stock.Code)
		for rtCount := 0; rtCount <= RETRIES; rtCount++ {
			if len(xqCookies) == 0 {
				xqCookies, e = getXqCookies()
				if e != nil {
					log.Printf("%s failed to get cookies: %+v, retrying %d...", stock.Code, e, rtCount+1)
					continue
				}
			}
			ok, r = xqShares(stock, xqCookies)
			if ok {
				chrstk <- stock
			} else if r {
				log.Printf("%s retrying %d...", stock.Code, rtCount+1)
				time.Sleep(time.Millisecond * time.Duration(500+rand.Intn(1000)))
				continue
			} else {
				log.Warnf("%s retried %d, giving up. restart the program to recover missing data", stock.Code, rtCount+1)
			}
			break
		}
	}
}

func getXqCookies() (cookies []*http.Cookie, e error) {
	res, e := util.HTTPGetResponse(`https://xueqiu.com/`, nil, false, true, true)
	if e != nil {
		return
	}
	return res.Cookies(), nil
}

func thsShares(stock *model.Stock) (ok, retry bool) {
	url := fmt.Sprintf(`http://basic.10jqka.com.cn/%s/equity.html`, stock.Code)
	res, e := util.HTTPGetResponse(url, nil, false, true, true)
	if e != nil {
		log.Printf("%s, http failed %s", stock.Code, url)
		return false, true
	}
	defer res.Body.Close()

	// Convert the designated charset HTML to utf-8 encoded HTML.
	utfBody := transform.NewReader(res.Body, simplifiedchinese.GBK.NewDecoder())

	// parse body using goquery
	doc, e := goquery.NewDocumentFromReader(utfBody)
	if e != nil {
		log.Printf("[%s,%s] failed to read from response body, retrying...", stock.Code,
			stock.Name)
		return false, true
	}

	//parse share structure table
	ok, retry, cont := false, true, true
	doc.Find("#stockcapit div.bd.pt5 table tbody tr").Each(
		func(i int, s *goquery.Selection) {
			typ := strings.TrimSpace(s.Find("th").Text())
			if "变动原因" == typ || !cont {
				return
			}
			sval := s.Find("td").First().Text()
			var (
				fval float64
				div  = 100000000.
			)
			if strings.Contains(sval, "亿") {
				fval, e = strconv.ParseFloat(strings.TrimSuffix(sval, "亿"), 64)
				if e != nil {
					log.Panicf("%s invalid share value format: %s, url: %s\n%+v", stock.Code, sval, url, e)
				}
				div = 1.
			} else if strings.Contains(sval, "万") {
				fval, e = strconv.ParseFloat(strings.TrimSuffix(sval, "万"), 64)
				if e != nil {
					log.Panicf("%s invalid share value format: %s, url: %s\n%+v", stock.Code, sval, url, e)
				}
				div = 10000.
			}
			fval /= div
			ok, retry = true, false
			switch typ {
			case "总股本(股)":
				stock.ShareSum.Valid = true
				stock.ShareSum.Float64 = fval
			case "A股总股本(股)":
				stock.AShareSum.Valid = true
				stock.AShareSum.Float64 = fval
			case "流通A股(股)":
				stock.AShareExch.Valid = true
				stock.AShareExch.Float64 = fval
			case "限售A股(股)":
				stock.AShareR.Valid = true
				stock.AShareR.Float64 = fval
			case "B股总股本(股)":
				stock.BShareSum.Valid = true
				stock.BShareSum.Float64 = fval
			case "流通B股(股)":
				stock.BShareExch.Valid = true
				stock.BShareExch.Float64 = fval
			case "限售B股(股)":
				stock.BShareR.Valid = true
				stock.BShareR.Float64 = fval
			case "H股总股本(股)":
				stock.HShareSum.Valid = true
				stock.HShareSum.Float64 = fval
			case "流通H股(股)":
				stock.HShareExch.Valid = true
				stock.HShareExch.Float64 = fval
			case "限售H股(股)":
				stock.HShareR.Valid = true
				stock.HShareR.Float64 = fval
			default:
				log.Printf("%s unrecognized type: %s, url: %s", stock.Code, typ, url)
				ok, retry, cont = false, true, false
				return
			}
		})
	return
}

func xqShares(stock *model.Stock, cookies []*http.Cookie) (ok, retry bool) {
	//TODO get share info from xueqiu.com
	// https://xueqiu.com/snowman/S/SH601598/detail#/GBJG
	// https://stock.xueqiu.com/v5/stock/f10/cn/shareschg.json?symbol=SH601598&count=100&extend=true
	url := fmt.Sprintf(`https://stock.xueqiu.com/v5/stock/f10/cn/shareschg.json?symbol=%s%s&count=1000&extend=true`, stock.Market.String, stock.Code)
	res, e := util.HTTPGetResponse(url, nil, false, true, true, cookies...)
	if e != nil {
		log.Printf("%s, http failed %s", stock.Code, url)
		return false, true
	}
	defer res.Body.Close()
	var xqshare model.XqSharesChg
	if body, e := ioutil.ReadAll(res.Body); e != nil {
		log.Printf("[%s,%s] failed to read from response body, retrying...", stock.Code,
			stock.Name)
		return false, true
	} else if strings.Contains(string(body), `"error_code": "400016"`) {
		// cookie timeout, refresh cookies
		xqCookies, e = getXqCookies()
		if e != nil {
			log.Errorf("%s failed to get cookies: %+v", stock.Code, e)
			return false, true
		}
	} else if e = json.Unmarshal(body, &xqshare); e != nil {
		log.Printf("[%s,%s] failed to parse json body, retrying...", stock.Code,
			stock.Name)
		return false, true
	}
	if xqshare.ErrorCode != 0 {
		log.Printf("[%s,%s] failed from xueqiu.com:[%d, %s] retrying...", stock.Code,
			stock.Name, xqshare.ErrorCode, xqshare.ErrorDesc)
		return false, true
	} else if len(xqshare.Data.Items) == 0 {
		log.Printf("[%s,%s] no share info from xueqiu.com", stock.Code, stock.Name)
		return true, false
	}
	mod := 0.00000001
	s := xqshare.Data.Items[0]
	if s.TotalShare != nil {
		stock.ShareSum.Valid = true
		stock.ShareSum.Float64 = *s.TotalShare * mod
	}
	if s.FloatAShare != nil {
		stock.AShareSum.Valid = true
		stock.AShareSum.Float64 += *s.FloatAShare
		stock.AShareExch.Valid = true
		stock.AShareExch.Float64 = *s.FloatAShare * mod
	}
	if s.LimitAShare != nil {
		stock.AShareSum.Valid = true
		stock.AShareSum.Float64 += *s.LimitAShare
		stock.AShareR.Valid = true
		stock.AShareR.Float64 = *s.LimitAShare * mod
	}
	stock.AShareSum.Float64 *= mod

	if s.FloatBShare != nil {
		stock.BShareSum.Valid = true
		stock.BShareSum.Float64 += *s.FloatBShare
		stock.BShareExch.Valid = true
		stock.BShareExch.Float64 = *s.FloatBShare * mod
	}
	if s.LimitBShare != nil {
		stock.BShareSum.Valid = true
		stock.BShareSum.Float64 += *s.LimitBShare
		stock.BShareR.Valid = true
		stock.BShareR.Float64 = *s.LimitBShare * mod
	}
	stock.BShareSum.Float64 *= mod

	if s.FloatHShare != nil {
		stock.HShareSum.Valid = true
		stock.HShareSum.Float64 += *s.FloatHShare
		stock.HShareExch.Valid = true
		stock.HShareExch.Float64 = *s.FloatHShare * mod
	}
	if s.LimitHShare != nil {
		stock.HShareSum.Valid = true
		stock.HShareSum.Float64 += *s.LimitHShare
		stock.HShareR.Valid = true
		stock.HShareR.Float64 = *s.LimitHShare * mod
	}
	stock.HShareSum.Float64 *= mod

	return true, false
}

func tcIndustry(stock *model.Stock) (ok, retry bool) {
	url := fmt.Sprintf(`http://stock.finance.qq.com/corp1/plate.php?zqdm=%s`, stock.Code)
	res, e := util.HttpGetResp(url)
	if e != nil {
		log.Printf("%s, http failed %s", stock.Code, url)
		return false, true
	}
	defer res.Body.Close()

	// Convert the designated charset HTML to utf-8 encoded HTML.
	utfBody := transform.NewReader(res.Body, simplifiedchinese.GBK.NewDecoder())

	// parse body using goquery
	doc, e := goquery.NewDocumentFromReader(utfBody)
	if e != nil {
		log.Printf("[%s,%s] failed to read from response body, retrying...", stock.Code,
			stock.Name)
		return false, true
	}

	//parse industry value
	var sel string
	switch conf.Args.DataSource.Industry {
	case conf.TENCENT_TC:
		sel = `body div.page div table.list tbody tr:nth-child(2) td:nth-child(2) a`
	case conf.TENCENT_CSRC:
		sel = `body div.page div table.list tbody tr.nobor td:nth-child(2) a`
	default:
		log.Panicf("unrecognized industry info source: %s", conf.Args.DataSource.Industry)
	}
	val := doc.Find(sel).Text()
	stock.Industry.Valid = true
	stock.Industry.String = val

	return true, false
}

func thsIndustry(stock *model.Stock) (ok, retry bool) {
	url := fmt.Sprintf(`http://basic.10jqka.com.cn/%s/field.html`, stock.Code)
	res, e := util.HTTPGetResponse(url, nil, false, true, true)
	if e != nil {
		log.Printf("%s, http failed %s", stock.Code, url)
		return false, true
	}
	defer res.Body.Close()

	// Convert the designated charset HTML to utf-8 encoded HTML.
	utfBody := transform.NewReader(res.Body, simplifiedchinese.GBK.NewDecoder())

	// parse body using goquery
	doc, e := goquery.NewDocumentFromReader(utfBody)
	if e != nil {
		log.Debugf("[%s,%s] failed to read from response body, retrying...", stock.Code,
			stock.Name)
		return false, true
	}

	log.Debugf("%s returns: \n %s", url, doc.Text())

	if len(doc.Find(`#fieldstatus div.bd.pr div.field_wraper p`).Text()) == 0 {
		text := doc.Find(`body div.wrapper div.header div.bd.clear div.code.fl div:nth-child(2) h1`).Text()
		text = strings.TrimSpace(text)
		if text == stock.Code {
			log.Warnf("[%s, %s] no industry info", stock.Code, stock.Name)
			return true, false
		}
		log.Debugf("[%s,%s] industry info not detected, retrying...", stock.Code, stock.Name)
		return false, true
	}

	//parse industry value
	sel := `#fieldstatus div.bd.pr div.field_wraper p span`
	val := doc.Find(sel).Text()
	if len(val) == 0 {
		return true, false
	}
	sep := " -- "
	idx := strings.Index(val, sep)
	if idx <= 0 {
		log.Warnf("%s no industry lv1 data. value: %s, source: %s", stock.Code, val, url)
		return true, false
	}
	stock.IndLv1.Valid = true
	stock.IndLv1.String = val[:idx]
	sval := val[idx+4:]
	idx = strings.Index(sval, sep)
	if idx <= 0 {
		log.Warnf("%s no industry lv2 data. value: %s, source: %s", stock.Code, val, url)
		return true, false
	}
	stock.IndLv2.Valid = true
	stock.IndLv2.String = sval[:idx]
	sval = sval[idx+4:]
	idx = strings.Index(sval, " ")
	if idx <= 0 {
		log.Warnf("%s no industry lv3 data. value: %s, source: %s", stock.Code, val, url)
		return true, false
	}
	stock.IndLv3.Valid = true
	stock.IndLv3.String = sval[:idx]
	return true, false
}

//get stock list from official exchange web sites
func getFromExchanges() (allstk *model.Stocks) {
	allstk = getSSE()
	for _, s := range getSZSE() {
		allstk.Add(s)
	}
	return
}

//get Shenzhen A-share list
func getSZSE() (list []*model.Stock) {
	log.Println("Fetching Shenzhen A-Share list...")
	url_sz := `http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1110x&TABKEY=tab1&random=%.16f`
	url_sz = fmt.Sprintf(url_sz, rand.Float64())
	d, e := util.HttpGetBytes(url_sz)
	util.CheckErr(e, "failed to get Shenzhen A-share list")
	x, e := zip.NewReader(bytes.NewReader(d), int64(len(d)))
	util.CheckErr(e, "failed to parse Shenzhen A-share xlsx file")
	var xd xlsxData
	for _, f := range x.File {
		if "xl/worksheets/sheet1.xml" == f.Name {
			rc, e := f.Open()
			util.CheckErr(e, "failed to open sheet1.xml")
			d, e := ioutil.ReadAll(rc)
			util.CheckErr(e, "failed to read sheet1.xml")
			xml.Unmarshal(d, &xd)
		}
	}
	for _, r := range xd.data[1:] {
		// skip those with empty A-share code
		if r[5] == "" {
			continue
		}
		s := &model.Stock{}
		s.Market.Valid = true
		s.Market.String = "SZ"
		for i, c := range r {
			switch i {
			case 5:
				s.Code = c
			case 6:
				s.Name = c
			case 7:
				s.TimeToMarket.String = c
				s.TimeToMarket.Valid = true
			case 8:
				v, e := strconv.ParseFloat(strings.Replace(c, ",", "", -1), 64)
				util.CheckErr(e, "failed to parse total share in Shenzhen security list")
				s.Totals.Float64 = v / 100000000.0
				s.Totals.Valid = true
			case 9:
				v, e := strconv.ParseFloat(strings.Replace(c, ",", "", -1), 64)
				util.CheckErr(e, "failed to parse outstanding share in Shenzhen security list")
				s.Outstanding.Float64 = v / 100000000.0
				s.Outstanding.Valid = true
			}
		}
		d, t := util.TimeStr()
		s.UDate.Valid = true
		s.UTime.Valid = true
		s.UDate.String = d
		s.UTime.String = t
		list = append(list, s)
	}
	return
}

//get Shanghai A-share list
func getSSE() *model.Stocks {
	log.Println("Fetching Shanghai A-Share list...")

	url := `http://query.sse.com.cn/security/stock/getStockListData2.do` +
		`?&isPagination=false&stockType=1&pageHelp.pageSize=9999`
	ref := `http://www.sse.com.cn/assortment/stock/list/share/`
	d, e := util.HttpGetBytesUsingHeaders(url, map[string]string{"Referer": ref})
	util.CheckErr(e, "failed to get Shanghai A-share list")
	list := &model.Stocks{}
	e = json.Unmarshal(d, list)
	if e != nil {
		log.Panicf("failed to parse json from %s\n%+v", url, e)
	}
	// var wg, wgr sync.WaitGroup
	// // supplement shares info from the following
	// // http://query.sse.com.cn/commonQuery.do?jsonCallBack=jsonpCallback5040&isPagination=false&sqlId=COMMON_SSE_CP_GPLB_GPGK_GBJG_C&companyCode=600000&_=1578668181485
	// urlt := `http://query.sse.com.cn/commonQuery.do?isPagination=false&sqlId=COMMON_SSE_CP_GPLB_GPGK_GBJG_C&companyCode=%s`
	// ichan := make(chan string, global.JOB_CAPACITY)
	// ochan := make(chan *model.SseShareJson, global.JOB_CAPACITY)
	// wgr.Add(1)
	// go func() {
	// 	defer wgr.Done()
	// 	mod := 0.0001
	// 	for s := range ochan {
	// 		stk := list.Map[s.Code]
	// 		stk.Outstanding = util.Str2FBilMod(s.UnlimitedShares, mod)
	// 		stk.Totals = util.Str2FBilMod(s.DomesticShares, mod)
	// 	}
	// }()
	// for i := 0; i < conf.Args.Concurrency; i++ {
	// 	wg.Add(1)
	// 	go getSseShareInfo(&wg, ichan, ochan, urlt, ref)
	// }
	// close(ichan)
	// wg.Wait()
	// close(ochan)
	// wgr.Wait()

	list.SetMarket("SH")
	return list
}

func getSseShareInfo(wg *sync.WaitGroup, cin chan string, cout chan *model.SseShareJson, urlt, ref string) {
	defer wg.Done()
	for code := range cin {
		url := fmt.Sprintf(urlt, code)
		op := func(c int) (e error) {
			res, e := util.HTTPGetResponse(url, map[string]string{"Referer": ref}, false, true, true)
			if e != nil {
				log.Debugf("failed to get share info from %s\n%+v", url, e)
				return
			}
			defer res.Body.Close()
			payload, e := ioutil.ReadAll(res.Body)
			if e != nil {
				log.Debugf("failed to http body from %s\n%+v", url, e)
				return
			}
			sseJson := &model.SseShareJson{Code: code}
			e = json.Unmarshal(payload, sseJson)
			if e != nil {
				log.Debugf("failed to parse json from %s\n%+v", url, e)
				return
			}
			cout <- sseJson
			return
		}
		e := repeat.Repeat(
			repeat.FnWithCounter(op),
			repeat.StopOnSuccess(),
			repeat.LimitMaxTries(conf.Args.DefaultRetry),
			repeat.WithDelay(
				repeat.FullJitterBackoff(500*time.Millisecond).WithMaxDelay(10*time.Second).Set(),
			),
		)
		if e != nil {
			log.Warnf("failed to get industry info for issuer %s, "+
				"may rerun the program and try to recover data: %+v", code, e)
		}
	}
}

//overwrite to database
func overwrite(allstk []*model.Stock) {
	if len(allstk) > 0 {
		tran, e := dbmap.Begin()
		util.CheckErr(e, "failed to begin new transaction")
		numFields := 31
		holders := make([]string, numFields)
		for i := range holders {
			holders[i] = "?"
		}
		holderString := fmt.Sprintf("(%s)", strings.Join(holders, ","))
		codes := make([]string, len(allstk))
		type batch struct {
			placeHolders []string
			values       []interface{}
		}
		batchSize := 500
		batches := make([]*batch, 0, 16)
		for i, stk := range allstk {
			q, r := i/batchSize, i%batchSize
			if r == 0 {
				batches = append(batches, &batch{})
			}
			batches[q].placeHolders = append(batches[q].placeHolders, holderString)
			batches[q].values = append(batches[q].values, stk.Code)
			batches[q].values = append(batches[q].values, stk.Name)
			batches[q].values = append(batches[q].values, stk.Market)
			batches[q].values = append(batches[q].values, stk.Industry)
			batches[q].values = append(batches[q].values, stk.IndLv1)
			batches[q].values = append(batches[q].values, stk.IndLv2)
			batches[q].values = append(batches[q].values, stk.IndLv3)
			batches[q].values = append(batches[q].values, stk.Price)
			batches[q].values = append(batches[q].values, stk.Varate)
			batches[q].values = append(batches[q].values, stk.Var)
			batches[q].values = append(batches[q].values, stk.Accer)
			batches[q].values = append(batches[q].values, stk.Xrate)
			batches[q].values = append(batches[q].values, stk.Volratio)
			batches[q].values = append(batches[q].values, stk.Ampl)
			batches[q].values = append(batches[q].values, stk.Turnover)
			batches[q].values = append(batches[q].values, stk.Outstanding)
			batches[q].values = append(batches[q].values, stk.Totals)
			batches[q].values = append(batches[q].values, stk.CircMarVal)
			batches[q].values = append(batches[q].values, stk.TimeToMarket)
			batches[q].values = append(batches[q].values, stk.ShareSum)
			batches[q].values = append(batches[q].values, stk.AShareSum)
			batches[q].values = append(batches[q].values, stk.AShareExch)
			batches[q].values = append(batches[q].values, stk.AShareR)
			batches[q].values = append(batches[q].values, stk.BShareSum)
			batches[q].values = append(batches[q].values, stk.BShareExch)
			batches[q].values = append(batches[q].values, stk.BShareR)
			batches[q].values = append(batches[q].values, stk.HShareSum)
			batches[q].values = append(batches[q].values, stk.HShareExch)
			batches[q].values = append(batches[q].values, stk.HShareR)
			batches[q].values = append(batches[q].values, stk.UDate)
			batches[q].values = append(batches[q].values, stk.UTime)
			codes[i] = stk.Code
		}

		rs, e := tran.Exec(fmt.Sprintf("delete from basics where code not in (%s)", util.Join(codes, ",", true)))
		if e != nil {
			tran.Rollback()
			log.Panicf("failed to clean basics %d\n%+v", len(allstk), e)
		}
		ra, e := rs.RowsAffected()
		if e != nil {
			tran.Rollback()
			log.Panicf("failed to get delete sql rows affected\n%+v", e)
		}
		log.Printf("%d stale stock record deleted from basics", ra)
		for _, b := range batches {
			stmt := fmt.Sprintf("INSERT INTO basics (code,name,market,industry,ind_lv1,ind_lv2,ind_lv3,price,"+
				"varate,var,accer,xrate,volratio,ampl,turnover,outstanding,totals,circmarval,timeToMarket,"+
				"share_sum,a_share_sum,a_share_exch,a_share_r,b_share_sum,b_share_exch,b_share_r,"+
				"h_share_sum,h_share_exch,h_share_r,udate,utime) VALUES %s on duplicate key update "+
				"name=values(name),market=values(market),industry=values(industry),ind_lv1=values(ind_lv1),ind_lv2=values(ind_lv2),"+
				"ind_lv3=values(ind_lv3),price=values(price),varate=values(varate),var=values(var),accer=values(accer),"+
				"xrate=values(xrate),volratio=values(volratio),ampl=values(ampl),turnover=values(turnover),"+
				"outstanding=values(outstanding),totals=values(totals),circmarval=values(circmarval),timeToMarket=values"+
				"(timeToMarket),share_sum=values(share_sum),a_share_sum=values(a_share_sum),a_share_exch=values(a_share_exch),"+
				"a_share_r=values(a_share_r),b_share_sum=values(b_share_sum),b_share_exch=values(b_share_exch),"+
				"b_share_r=values(b_share_r),h_share_sum=values(h_share_sum),h_share_exch=values(h_share_exch),"+
				"h_share_r=values(h_share_r),udate=values(udate),utime=values(utime)",
				strings.Join(b.placeHolders, ","))
			_, e = tran.Exec(stmt, b.values...)
			if e != nil {
				tran.Rollback()
				log.Panicf("failed to bulk update basics %d\n%+v", len(allstk), e)
			}
		}
		tran.Commit()
		log.Printf("%d stocks info overwrite to basics", len(allstk))
	}
}

func getFrom10jqk() (allstk []*model.Stock) {
	var (
		wg    sync.WaitGroup
		wgget sync.WaitGroup
	)
	chstk := make(chan []*model.Stock, 100)
	wg.Add(1)
	tp := parse10jqk(chstk, 1, true, &wg)
	log.Printf("total page: %d", tp)
	wgget.Add(1)
	go func() {
		defer wgget.Done()
		c := 1
		for stks := range chstk {
			allstk = append(allstk, stks...)
			log.Printf("%d/%d, %d", c, tp, len(allstk))
			c++
		}
	}()
	for p := 2; p <= tp; p++ {
		wg.Add(1)
		go parse10jqk(chstk, p, false, &wg)
	}
	wg.Wait()
	close(chstk)
	wgget.Wait()

	return
}

func parse10jqk(chstk chan []*model.Stock, page int, parsePage bool, wg *sync.WaitGroup) (totalPage int) {
	var stocks []*model.Stock
	defer wg.Done()
	urlt := `http://q.10jqka.com.cn/index/index/board/all/field/zdf/order/desc/page/%d/ajax/1/`

	// Load the URL
	url := fmt.Sprintf(urlt, page)
	res, e := util.HTTPGetResponse(url, nil, false, true, true)
	if e != nil {
		panic(e)
	}
	defer res.Body.Close()

	// Convert the designated charset HTML to utf-8 encoded HTML.
	utfBody := transform.NewReader(res.Body, simplifiedchinese.GBK.NewDecoder())

	// parse utfBody using goquery
	doc, err := goquery.NewDocumentFromReader(utfBody)
	if err != nil {
		log.Printf("%+v", utfBody)
		log.Panic(err)
	}

	doc.Find("tbody tr").Each(func(i int, s *goquery.Selection) {
		stk := &model.Stock{}
		stocks = append(stocks, stk)
		s.Find("td").Each(func(j int, s2 *goquery.Selection) {
			v := s2.Text()
			switch j {
			case 1:
				stk.Code = strings.TrimSpace(v)
			case 2:
				stk.Name = strings.TrimSpace(v)
			case 3:
				stk.Price = util.Str2Fnull(v)
			case 4:
				stk.Varate = util.Str2Fnull(v)
			case 5:
				stk.Var = util.Str2Fnull(v)
			case 6:
				stk.Accer = util.Str2Fnull(v)
			case 7:
				stk.Xrate = util.Str2Fnull(v)
			case 8:
				stk.Volratio = util.Str2Fnull(v)
			case 9:
				stk.Ampl = util.Str2Fnull(v)
			case 10:
				stk.Turnover = util.Str2FBil(v)
			case 11:
				stk.Outstanding = util.Str2FBil(v)
			case 12:
				stk.CircMarVal = util.Str2FBil(v)
			case 13:
				stk.Pe = util.Str2Fnull(v)
			default:
				// skip
			}
		})
		d, t := util.TimeStr()
		stk.UDate.Valid = true
		stk.UTime.Valid = true
		stk.UDate.String = d
		stk.UTime.String = t
	})

	chstk <- stocks

	if parsePage {
		//*[@id="m-page"]/span
		doc.Find("#m-page span").Each(func(i int, s *goquery.Selection) {
			t := s.Text()
			ps := strings.Split(t, `/`)
			if len(ps) == 2 {
				cp, e := strconv.ParseInt(ps[1], 10, 32)
				if e != nil {
					log.Printf("can't parse total page: %+v, error: %+v", t, e)
				} else {
					totalPage = int(cp)
				}
			}
		})
	}

	return
}

func getFromQq() (allstk []*model.Stock) {
	allstk = append(allstk, getQqMarket("Shanghai A-Share market", `http://stock.finance.qq`+
		`.com/hqing/hqst/paiminglist1.htm?page=%d`)...)
	allstk = append(allstk, getQqMarket("Shenzhen A-Share market", `http://stock.finance.qq`+
		`.com/hqing/hqst/paiminglistsa1.htm?page=%d`)...)
	return
}

func getQqMarket(name, urlt string) (allstk []*model.Stock) {
	var (
		wg    sync.WaitGroup
		wgget sync.WaitGroup
	)
	chstk := make(chan []*model.Stock, 100)
	wg.Add(1)
	tp := parseQq(chstk, 1, true, urlt, &wg)
	log.Printf("%s total page: %d", name, tp)
	wgget.Add(1)
	go func() {
		defer wgget.Done()
		c := 1
		for stks := range chstk {
			allstk = append(allstk, stks...)
			log.Printf("%d/%d, %d", c, tp, len(allstk))
			c++
		}
	}()
	for p := 2; p <= tp; p++ {
		wg.Add(1)
		go parseQq(chstk, p, false, urlt, &wg)
	}
	wg.Wait()
	close(chstk)
	wgget.Wait()

	return
}

func parseQq(chstk chan []*model.Stock, page int, parsePage bool, urlt string, wg *sync.WaitGroup) (totalPage int) {
	var stocks []*model.Stock
	defer wg.Done()

	// Load the URL
	res, e := util.HttpGetResp(fmt.Sprintf(urlt, page))
	if e != nil {
		panic(e)
	}
	defer res.Body.Close()

	// Convert the designated charset HTML to utf-8 encoded HTML.
	utfBody := transform.NewReader(res.Body, simplifiedchinese.GBK.NewDecoder())

	// parse utfBody using goquery
	doc, err := goquery.NewDocumentFromReader(utfBody)
	if err != nil {
		log.Printf("%+v", utfBody)
		log.Panic(err)
	}

	//> tr:nth - child(1)
	doc.Find("body table:nth-child(12) tbody tr td table:nth-child(4) tbody tr").Each(
		func(i int, s *goquery.Selection) {
			if i == 0 {
				//skip header
				return
			}
			stk := &model.Stock{}
			stocks = append(stocks, stk)
			s.Find("td").Each(func(j int, s2 *goquery.Selection) {
				v := s2.Text()
				switch j {
				case 0:
					stk.Code = strings.TrimSpace(v)
				case 1:
					stk.Name = strings.TrimSpace(v)
				case 2:
					stk.Price = util.Str2Fnull(v)
				case 3:
					//pre close
				case 4:
					//open
				case 5:
					stk.Accer = util.Str2Fnull(v)
				case 6:
					stk.Xrate = util.Str2Fnull(v)
				case 7:
				case 8:
					stk.Volratio = util.Str2Fnull(v)
				case 9:
					stk.Ampl = util.Str2Fnull(v)
				case 10:
					stk.Turnover = util.Str2FBil(v)
				case 11:
					stk.Outstanding = util.Str2FBil(v)
				case 12:
					stk.CircMarVal = util.Str2FBil(v)
				default:
					// skip
				}
			})
			d, t := util.TimeStr()
			stk.UDate.Valid = true
			stk.UTime.Valid = true
			stk.UDate.String = d
			stk.UTime.String = t
		})

	chstk <- stocks

	if parsePage {
		//*[@id="m-page"]/span
		doc.Find("#m-page span").Each(func(i int, s *goquery.Selection) {
			t := s.Text()
			ps := strings.Split(t, `/`)
			if len(ps) == 2 {
				cp, e := strconv.ParseInt(ps[1], 10, 32)
				if e != nil {
					log.Printf("can't parse total page: %+v, error: %+v", t, e)
				} else {
					totalPage = int(cp)
				}
			}
		})
	}

	return
}

type xlsxData struct {
	data [][]string
}

func (x *xlsxData) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	ws := struct {
		SheetData struct {
			Row []struct {
				C []struct {
					Is struct {
						T string `xml:"t"`
					} `xml:"is"`
				} `xml:"c"`
			} `xml:"row"`
		} `xml:"sheetData"`
	}{}
	d.DecodeElement(&ws, &start)
	for _, r := range ws.SheetData.Row {
		nr := [][]string{{}}
		for _, c := range r.C {
			nr[0] = append(nr[0], c.Is.T)
		}
		x.data = append(x.data, nr...)
	}
	return nil
}

// Update basic info such as P/E, P/UDPPS, P/OCFPS
func updBasics(stocks *model.Stocks) *model.Stocks {
	sql, e := dot.Raw("UPD_BASICS")
	util.CheckErr(e, "failed to get UPD_BASICS sql")
	sql = fmt.Sprintf(sql, util.Join(stocks.Codes, ",", true))
	_, e = dbmap.Exec(sql)
	util.CheckErr(e, "failed to update basics, sql:\n"+sql)
	log.Printf("%d basics info updated", stocks.Size())
	return stocks
}

func collect(stocks *model.Stocks, rstks chan *model.Stock) (wgr *sync.WaitGroup) {
	wgr = new(sync.WaitGroup)
	wgr.Add(1)
	go func() {
		defer wgr.Done()
		for stk := range rstks {
			stocks.Add(stk)
		}
	}()
	return wgr
}
