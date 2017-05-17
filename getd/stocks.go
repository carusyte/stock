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

	getXDXR(allstk)

	return
}

// get xdxr info
func getXDXR(stocks []*model.Stock) {

}

func parseStockPage(chstk chan []*model.Stock, page int, parsePage bool, wg *sync.WaitGroup) (totalPage int) {
	var stocks []*model.Stock
	defer wg.Done()
	urlt := `http://q.10jqka.com.cn/index/index/board/all/field/zdf/order/desc/page/%d/ajax/1/`

	// Load the URL
	res, e := HttpGetResp(fmt.Sprintf(urlt, page))
	if e != nil{
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