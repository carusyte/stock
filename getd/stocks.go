package main

import (
	"database/sql"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

func GetStockInfo() (allstk []*model.Stock) {
	var (
		wg    sync.WaitGroup
		wgget sync.WaitGroup
	)
	chstk := make(chan []*model.Stock, 100)
	wg.Add(1)
	tp := parseStockPage(chstk, 1, true, &wg)
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
		go parseStockPage(chstk, p, false, &wg)
	}
	wg.Wait()
	close(chstk)
	wgget.Wait()

	log.Printf("total stocks: %d", len(allstk))

	//update to database
	if len(allstk) > 0 {
		valueStrings := make([]string, 0, len(allstk))
		valueArgs := make([]interface{}, 0, len(allstk)*13)
		for _, stk := range allstk {
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, stk.Code)
			valueArgs = append(valueArgs, stk.Name)
			valueArgs = append(valueArgs, stk.Price)
			valueArgs = append(valueArgs, stk.Varate)
			valueArgs = append(valueArgs, stk.Var)
			valueArgs = append(valueArgs, stk.Accer)
			valueArgs = append(valueArgs, stk.Xrate)
			valueArgs = append(valueArgs, stk.Volratio)
			valueArgs = append(valueArgs, stk.Ampl)
			valueArgs = append(valueArgs, stk.Turnover)
			valueArgs = append(valueArgs, stk.Outstanding)
			valueArgs = append(valueArgs, stk.CircMarVal)
			valueArgs = append(valueArgs, stk.Pe)
		}
		stmt := fmt.Sprintf("INSERT INTO basics (code,name,price,varate,var,accer,xrate,volratio,ampl,"+
			"turnover,outstanding,circmarval,pe) VALUES %s on duplicate key update name=values(name),"+
			"price=values(price),varate=values(varate),var=values(var),accer=values(accer),"+
			"xrate=values(xrate),volratio=values(volratio),ampl=values(ampl),turnover=values(turnover),"+
			"outstanding=values(outstanding),circmarval=values(circmarval),pe=values(pe)",
			strings.Join(valueStrings, ","))
		_, err := dbmap.Exec(stmt, valueArgs...)
		util.CheckErr(err, "failed to bulk update basics")
		log.Printf("%d stocks info updated to basics", len(allstk))
	}

	getXDXRs(allstk)

	return
}

//TODO get xdxr info
func getXDXRs(stocks []*model.Stock) {
	log.Println("getting XDXR info...")
	var (
		wg    sync.WaitGroup
		wgget sync.WaitGroup
		xdxrs []*model.Xdxr
	)
	chxdxr := make(chan []*model.Xdxr, 16)
	wgget.Add(1)
	go func() {
		defer wgget.Done()
		c := 1
		for xdxr := range chxdxr {
			xdxrs = append(xdxrs, xdxr...)
			if len(xdxr) > 0 {
				log.Printf("%d/%d : %s[%s] : %d", c, len(stocks), xdxr[0].Code, xdxr[0].Name, len(xdxr))
			} else {
				log.Printf("%d/%d : %d", c, len(stocks), len(xdxr))
			}
			c++
		}
	}()
	for _, s := range stocks {
		wg.Add(1)
		go parseBonusPage(chxdxr, s, &wg)
	}
	wg.Wait()
	close(chxdxr)
	wgget.Wait()

	log.Printf("total xdxr records: %d", len(xdxrs))

	//update to database
	if len(xdxrs) > 0 {
		valueStrings := make([]string, 0, len(xdxrs))
		valueArgs := make([]interface{}, 0, len(xdxrs)*15)
		for _, e := range xdxrs {
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, e.Code)
			valueArgs = append(valueArgs, e.Name)
			valueArgs = append(valueArgs, e.Index)
			valueArgs = append(valueArgs, e.ReportYear)
			valueArgs = append(valueArgs, e.BoardDate)
			valueArgs = append(valueArgs, e.GmsDate)
			valueArgs = append(valueArgs, e.ImplDate)
			valueArgs = append(valueArgs, e.Plan)
			valueArgs = append(valueArgs, e.Divi)
			valueArgs = append(valueArgs, e.Shares)
			valueArgs = append(valueArgs, e.RecordDate)
			valueArgs = append(valueArgs, e.XdxrDate)
			valueArgs = append(valueArgs, e.Progress)
			valueArgs = append(valueArgs, e.PayoutRatio)
			valueArgs = append(valueArgs, e.DivRate)
		}
		stmt := fmt.Sprintf("INSERT INTO `div` (code,name,`index`,report_year,board_date,gms_date,impl_date,"+
			"plan,divi,shares,record_date,xdxr_date,progress,payout_ratio,div_rate) VALUES %s on "+
			"duplicate key update name=values(name),report_year=values(report_year),board_date=values"+
			"(board_date),gms_date=values(gms_date),impl_date=values(impl_date),plan=values(plan),"+
			"divi=values(divi),shares=values(shares),record_date=values(record_date),xdxr_date=values"+
			"(xdxr_date),progress=values(progress),payout_ratio=values(payout_ratio),div_rate=values"+
			"(div_rate)",
			strings.Join(valueStrings, ","))
		_, err := dbmap.Exec(stmt, valueArgs...)
		util.CheckErr(err, "failed to bulk update div")
		log.Printf("%d xdxr info updated to div", len(xdxrs))
	}
}

func parseBonusPage(chxdxr chan []*model.Xdxr, stock *model.Stock, wg *sync.WaitGroup) {
	defer wg.Done()
	var xdxrs []*model.Xdxr
	urlt := `http://stockpage.10jqka.com.cn/%s/bonus/`

	// Load the URL
	res, e := HttpGetResp(fmt.Sprintf(urlt, stock.Code))
	if e != nil {
		panic(e)
	}
	defer res.Body.Close()

	// parse body using goquery
	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		log.Printf("[%s,%s] body:\n%+v", stock.Code, stock.Name, res.Body)
		log.Panic(err)
	}

	doc.Find("#bonus_table tbody tr").Each(func(i int, s *goquery.Selection) {
		xdxr := newXdxr()
		xdxrs = append(xdxrs, xdxr)
		s.Find("td").Each(func(j int, s2 *goquery.Selection) {
			v := s2.Text()
			switch j {
			case 1:
				xdxr.ReportYear = strings.TrimSpace(v)
			case 2:
				xdxr.BoardDate = strings.TrimSpace(v)
			case 3:
				xdxr.GmsDate = strings.TrimSpace(v)
			case 4:
				xdxr.ImplDate = strings.TrimSpace(v)
			case 5:
				xdxr.Plan = strings.TrimSpace(v)
			case 6:
				xdxr.RecordDate = strings.TrimSpace(v)
			case 7:
				xdxr.XdxrDate = strings.TrimSpace(v)
			case 8:
				xdxr.Progress = strings.TrimSpace(v)
			case 9:
				xdxr.PayoutRatio = util.Str2fnull(strings.TrimSpace(v[:len(v)-1]))
			case 10:
				xdxr.DivRate = util.Str2fnull(strings.TrimSpace(v[:len(v)-1]))
			default:
				// skip
			}
		})

		//FIXME the column order is undetermined
		if len(xdxr.BoardDate) > 10{
			log.Printf("%s %s board date: %s", xdxr.Code, xdxr.Name, xdxr.BoardDate)
		}
		if len(xdxr.GmsDate) > 10{
			log.Printf("%s %s gms date: %s", xdxr.Code, xdxr.Name, xdxr.GmsDate)
		}
		if len(xdxr.ImplDate) > 10{
			log.Printf("%s %s impl date: %s", xdxr.Code, xdxr.Name, xdxr.ImplDate)
		}
		xdxr.Code = stock.Code
		xdxr.Name = stock.Name
		xdxr.Index = i
		parseXdxrPlan(xdxr)
	})

	chxdxr <- xdxrs
}
func newXdxr() *model.Xdxr {
	xdxr := &model.Xdxr{}
	xdxr.Shares = sql.NullFloat64{0, false}
	xdxr.DivRate = sql.NullFloat64{0, false}
	xdxr.Divi = sql.NullFloat64{0, false}
	xdxr.PayoutRatio = sql.NullFloat64{0, false}
	return xdxr
}

func parseXdxrPlan(xdxr *model.Xdxr) {
	allot := regexp.MustCompile(`送(\d*)股`).FindStringSubmatch(xdxr.Plan)
	cvt := regexp.MustCompile(`转(\d*)股`).FindStringSubmatch(xdxr.Plan)
	div := regexp.MustCompile(`派(\d*\.?\d*)元`).FindStringSubmatch(xdxr.Plan)

	if allot != nil {
		for i, a := range allot {
			if i > 0 {
				xdxr.Shares.Float64 += util.Str2f64(a)
				xdxr.Shares.Valid = true
			}
		}
	}
	if cvt != nil {
		for i, a := range cvt {
			if i > 0 {
				xdxr.Shares.Float64 += util.Str2f64(a)
				xdxr.Shares.Valid = true
			}
		}
	}
	if div != nil {
		for i, a := range div {
			if i > 0 {
				xdxr.Divi.Float64 += util.Str2f64(a)
				xdxr.Divi.Valid = true
			}
		}
	}
}

func parseStockPage(chstk chan []*model.Stock, page int, parsePage bool, wg *sync.WaitGroup) (totalPage int) {
	var stocks []*model.Stock
	defer wg.Done()
	urlt := `http://q.10jqka.com.cn/index/index/board/all/field/zdf/order/desc/page/%d/ajax/1/`

	// Load the URL
	res, e := HttpGetResp(fmt.Sprintf(urlt, page))
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
				stk.Price = s2billion(v)
			case 4:
				stk.Varate = s2billion(v)
			case 5:
				stk.Var = s2billion(v)
			case 6:
				stk.Accer = s2billion(v)
			case 7:
				stk.Xrate = s2billion(v)
			case 8:
				stk.Volratio = s2billion(v)
			case 9:
				stk.Ampl = s2billion(v)
			case 10:
				stk.Turnover = s2billion(v)
			case 11:
				stk.Outstanding = s2billion(v)
			case 12:
				stk.CircMarVal = s2billion(v)
			case 13:
				stk.Pe = s2billion(v)
			default:
				// skip
			}
		})
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

func s2billion(s string) (f sql.NullFloat64) {
	mod := 1.0
	s = strings.TrimSpace(s)
	if strings.HasSuffix(s, `万`) {
		s = strings.TrimSuffix(s, `万`)
		mod = 0.0001
	} else if strings.HasSuffix(s, `亿`) {
		s = strings.TrimSuffix(s, `亿`)
	}
	f64, e := strconv.ParseFloat(s, 64)
	if e == nil {
		f.Float64 = f64 * mod
		f.Valid = true
	} else {
		f.Valid = false
	}
	return
}
