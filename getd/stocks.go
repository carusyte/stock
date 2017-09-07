package getd

import (
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
	"log"
	"strconv"
	"strings"
	"sync"
	"encoding/json"
	"bytes"
	"archive/zip"
	"encoding/xml"
	"io/ioutil"
)

func StocksDb() (allstk []*model.Stock) {
	dbmap.Select(&allstk, "select * from basics")
	return
}

func StocksDbByCode(code ... string) (stocks []*model.Stock) {
	sql := fmt.Sprintf("select * from basics where code in (%s)", util.Join(code, ",", true))
	_, e := dbmap.Select(&stocks, sql)
	if e != nil {
		if "sql: no rows in result set" == e.Error() {
			return
		} else {
			log.Panicln("failed to run sql", e)
		}
	}
	return
}

func StocksDbTo(target interface{}) {
	dbmap.Select(target, "select * from basics")
	return
}

func GetStockInfo() (allstk *model.Stocks) {
	//allstk = getFrom10jqk()
	//allstk = getFromQq()
	//TODO need to get industry or area info
	allstk = getFromExchanges()
	log.Printf("total stocks: %d", allstk.Size())

	overwrite(allstk.List)

	return
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
	url_sz := `http://www.szse.cn/szseWeb/ShowReport.szse?SHOWTYPE=xlsx&CATALOGID=1110&tab1PAGENO=1&ENCODE=1&TABKEY=tab1`
	d, e := util.HttpGetBytesUsingHeaders(url_sz, map[string]string{
		"Referer": `http://www.szse.cn/main/marketdata/jypz/colist/`,
	})
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

	url_sh := `http://query.sse.com.cn/security/stock/getStockListData2.do` +
		`?&isPagination=false&stockType=1&pageHelp.pageSize=9999`
	d, e := util.HttpGetBytesUsingHeaders(url_sh, map[string]string{"Referer": "http://www.sse.com" +
		".cn/assortment/stock/list/share/"})
	util.CheckErr(e, "failed to get Shanghai A-share list")
	list := &model.Stocks{}
	e = json.Unmarshal(d, list)
	if e != nil {
		log.Panicf("failed to parse json from %s\n%+v", url_sh, e)
	}
	list.SetMarket("SH")
	return list
}

//overwrite to database
func overwrite(allstk []*model.Stock) {
	if len(allstk) > 0 {
		tran, e := dbmap.Begin()
		util.CheckErr(e, "failed to begin new transaction")

		codes := make([]string, len(allstk))
		valueStrings := make([]string, 0, len(allstk))
		valueArgs := make([]interface{}, 0, len(allstk)*17)
		for i, stk := range allstk {
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, stk.Code)
			valueArgs = append(valueArgs, stk.Name)
			valueArgs = append(valueArgs, stk.Market)
			valueArgs = append(valueArgs, stk.Price)
			valueArgs = append(valueArgs, stk.Varate)
			valueArgs = append(valueArgs, stk.Var)
			valueArgs = append(valueArgs, stk.Accer)
			valueArgs = append(valueArgs, stk.Xrate)
			valueArgs = append(valueArgs, stk.Volratio)
			valueArgs = append(valueArgs, stk.Ampl)
			valueArgs = append(valueArgs, stk.Turnover)
			valueArgs = append(valueArgs, stk.Outstanding)
			valueArgs = append(valueArgs, stk.Totals)
			valueArgs = append(valueArgs, stk.CircMarVal)
			valueArgs = append(valueArgs, stk.TimeToMarket)
			valueArgs = append(valueArgs, stk.UDate)
			valueArgs = append(valueArgs, stk.UTime)
			codes[i] = stk.Code
		}

		_, e = tran.Exec(fmt.Sprintf("delete from basics where code not in (%s)", util.Join(codes, ",", true)))
		if e != nil {
			tran.Rollback()
			log.Panicf("failed to clean basics %d\n%+v", len(allstk), e)
		}

		stmt := fmt.Sprintf("INSERT INTO basics (code,name,market,price,varate,var,accer,xrate,volratio,ampl,"+
			"turnover,outstanding,totals,circmarval,timeToMarket,udate,utime) VALUES %s on duplicate key update "+
			"name=values(name),market=values(market),"+
			"price=values(price),varate=values(varate),var=values(var),accer=values(accer),"+
			"xrate=values(xrate),volratio=values(volratio),ampl=values(ampl),turnover=values(turnover),"+
			"outstanding=values(outstanding),totals=values(totals),circmarval=values(circmarval),timeToMarket=values"+
			"(timeToMarket),udate=values(udate),utime=values(utime)",
			strings.Join(valueStrings, ","))
		_, e = tran.Exec(stmt, valueArgs...)
		if e != nil {
			tran.Rollback()
			log.Panicf("failed to bulk update basics %d\n%+v", len(allstk), e)
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
