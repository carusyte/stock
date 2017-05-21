package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"log"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
)

func GetXDXRs(stocks []*model.Stock) {
	log.Println("getting XDXR info...")
	var wg sync.WaitGroup
	chstk := make(chan *model.Stock, JOB_CAPACITY)
	for i := 0; i < MAX_CONCURRENCY; i++ {
		wg.Add(1)
		go parseBonusPage(chstk, &wg)
	}
	for _, s := range stocks {
		chstk <- s
	}
	close(chstk)
	wg.Wait()
}

func parseBonusPage(chstk chan *model.Stock, wg *sync.WaitGroup) {
	defer wg.Done()
	urlt := `http://stockpage.10jqka.com.cn/%s/bonus/`
	// target web server can't withstand heavy traffic
	RETRIES := 5
	for stock := range chstk {
		url := fmt.Sprintf(urlt, stock.Code)
		for rtCount := 0; rtCount <= RETRIES; rtCount++ {
			ok, r := doParseBonusPage(url, stock)
			if !ok && r {
				log.Printf("%s retrying %d...", stock.Code, rtCount+1)
				time.Sleep(time.Second * 1)
				continue
			} else if !ok && !r {
				log.Printf("%s retried %d, giving up. restart the program to recover", stock.Code, rtCount+1)
			}
			break
		}
	}
}

func doParseBonusPage(url string, stock *model.Stock) (ok, retry bool) {
	var xdxrs []*model.Xdxr
	// Load the URL
	res, e := util.HttpGetResp(url)
	if e != nil {
		log.Printf("%s, http failed, giving up %s", stock.Code, url)
		return false, false
	}
	defer res.Body.Close()

	// parse body using goquery
	doc, e := goquery.NewDocumentFromReader(res.Body)
	if e != nil {
		log.Printf("[%s,%s] failed to read from response body, retrying...", stock.Code,
			stock.Name)
		return false, true
	}

	//parse column index
	iReportYear, iBoardDate, iGmsDate, iImplDate, iPlan, iRecordDate, iXdxrDate, iProgress, iPayoutRatio,
	iDivRate, iPayoutDate := -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1
	doc.Find("#bonus_table thead tr").Each(func(i int, s *goquery.Selection) {
		s.Find("th").Each(func(j int, s2 *goquery.Selection) {
			v := s2.Text()
			switch v {
			case "报告期":
				iReportYear = j
			case "董事会日期":
				iBoardDate = j
			case "股东大会日期":
				iGmsDate = j
			case "实施日期":
				iImplDate = j
			case "分红方案说明":
				iPlan = j
			case "A股股权登记日":
				iRecordDate = j
			case "A股除权除息日":
				iXdxrDate = j
			case "A股派息日":
				iPayoutDate = j
			case "方案进度":
				iProgress = j
			case "股利支付率(%)":
				fallthrough
			case "股利支付率":
				iPayoutRatio = j
			case "分红率(%)":
				fallthrough
			case "分红率":
				iDivRate = j
			default:
				log.Printf("unidentified column header in bonus page %s : %s", url, v)
			}
		})
	})

	doc.Find("#bonus_table tbody tr").Each(func(i int, s *goquery.Selection) {
		xdxr := newXdxr()
		xdxrs = append(xdxrs, xdxr)
		s.Find("td").Each(func(j int, s2 *goquery.Selection) {
			v := s2.Text()
			if "--" != v {
				switch j {
				case iReportYear:
					xdxr.ReportYear = util.Str2Snull(v)
				case iBoardDate:
					xdxr.BoardDate = util.Str2Snull(v)
				case iGmsDate:
					xdxr.GmsDate = util.Str2Snull(v)
				case iImplDate:
					xdxr.ImplDate = util.Str2Snull(v)
				case iPlan:
					xdxr.Plan = util.Str2Snull(v)
				case iRecordDate:
					xdxr.RecordDate = util.Str2Snull(v)
				case iXdxrDate:
					xdxr.XdxrDate = util.Str2Snull(v)
				case iPayoutDate:
					xdxr.PayoutDate = util.Str2Snull(v)
				case iProgress:
					xdxr.Progress = util.Str2Snull(v)
				case iPayoutRatio:
					xdxr.PayoutRatio = util.Str2Fnull(strings.TrimSpace(strings.TrimSuffix(v,
						"%")))
				case iDivRate:
					xdxr.DivRate = util.Str2Fnull(strings.TrimSpace(strings.TrimSuffix(v,
						"%")))
				default:
					log.Printf("unidentified column value in bonus page %s : %s", url, v)
				}
			}
		})

		xdxr.Code = stock.Code
		xdxr.Name = stock.Name
		parseXdxrPlan(xdxr)
	})

	// no records found, return normally
	if len(xdxrs) == 0 {
		return true, false
	}

	for i, j := len(xdxrs)-1, 0; i >= 0; i, j = i-1, j+1 {
		xdxrs[i].Index = j
	}

	//update to database
	if len(xdxrs) > 0 {
		valueStrings := make([]string, 0, len(xdxrs))
		valueArgs := make([]interface{}, 0, len(xdxrs)*16)
		for _, e := range xdxrs {
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
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
			valueArgs = append(valueArgs, e.PayoutDate)
			valueArgs = append(valueArgs, e.Progress)
			valueArgs = append(valueArgs, e.PayoutRatio)
			valueArgs = append(valueArgs, e.DivRate)
		}
		stmt := fmt.Sprintf("INSERT INTO `div` (code,name,`index`,report_year,board_date,gms_date,impl_date,"+
			"plan,divi,shares,record_date,xdxr_date,payout_date,progress,payout_ratio,div_rate) VALUES %s"+
			" on duplicate key update name=values(name),report_year=values(report_year),board_date=values"+
			"(board_date),gms_date=values(gms_date),impl_date=values(impl_date),plan=values(plan),"+
			"divi=values(divi),shares=values(shares),record_date=values(record_date),xdxr_date=values"+
			"(xdxr_date),payout_date=values(payout_date),progress=values(progress),payout_ratio=values"+
			"(payout_ratio),div_rate=values"+
			"(div_rate)",
			strings.Join(valueStrings, ","))
		_, err := dbmap.Exec(stmt, valueArgs...)
		util.CheckErr(err, stock.Code+": failed to bulk update div")
	}

	return true, false
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
	if !xdxr.Plan.Valid || "不分配不转增" == xdxr.Plan.String || "董事会预案未通过" == xdxr.Plan.String {
		return
	}

	allot := regexp.MustCompile(`送(\d*\.?\d*)股?`).FindStringSubmatch(xdxr.Plan.String)
	cvt := regexp.MustCompile(`转增?(\d*\.?\d*)股?`).FindStringSubmatch(xdxr.Plan.String)
	div := regexp.MustCompile(`派(发现金红利)?(\d*\.?\d*)元?`).FindStringSubmatch(xdxr.Plan.String)

	if allot != nil {
		if len(allot) > 0 {
			xdxr.Shares.Float64 += util.Str2F64(allot[len(allot)-1])
			xdxr.Shares.Valid = true
		}
	}
	if cvt != nil {
		if len(cvt) > 0 {
			xdxr.Shares.Float64 += util.Str2F64(cvt[len(cvt)-1])
			xdxr.Shares.Valid = true
		}
	}
	if div != nil {
		if len(div) > 0 {
			xdxr.Divi.Float64 += util.Str2F64(div[len(div)-1])
			xdxr.Divi.Valid = true
		}
	}

	if allot == nil && cvt == nil && div == nil {
		log.Printf("%s, no value parsed from plan: %s", xdxr.Code, xdxr.Plan.String)
	}
}

//get finance info
func GetFinance(stocks []*model.Stock) {
	log.Println("getting Finance info...")
	var wg sync.WaitGroup
	chstk := make(chan *model.Stock, JOB_CAPACITY)
	for i := 0; i < MAX_CONCURRENCY; i++ {
		wg.Add(1)
		go parseFinancePage(chstk, &wg)
	}
	for _, s := range stocks {
		chstk <- s
	}
	close(chstk)
	wg.Wait()
}

func parseFinancePage(chstk chan *model.Stock, wg *sync.WaitGroup) {
	defer wg.Done()
	urlt := `http://stockpage.10jqka.com.cn/%s/finance`
	RETRIES := 5
	for stock := range chstk {
		url := fmt.Sprintf(urlt, stock.Code)
		for rtCount := 0; rtCount <= RETRIES; rtCount++ {
			ok, r := doParseFinPage(url, stock.Code)
			if !ok && r {
				log.Printf("%s retrying %d...", stock.Code, rtCount+1)
				time.Sleep(time.Second * 1)
				continue
			} else if !ok && !r {
				log.Printf("%s retried %d, giving up. restart the program to recover", stock.Code, rtCount+1)
			}
			break
		}
	}
}

func doParseFinPage(url string, code string) (ok, retry bool) {
	var (
		res *http.Response
		doc *goquery.Document
		e   error
	)
	// Load the URL
	res, e = util.HttpGetResp(url)
	if e != nil {
		log.Printf("%s, http failed, giving up %s", code, url)
		return false, false
	}
	defer res.Body.Close()
	// parse body using goquery
	doc, e = goquery.NewDocumentFromReader(res.Body)
	if e != nil {
		log.Printf("%s failed to read from response body, retrying...", code)
		return false, true
	}
	fr := &model.FinReport{}
	e = json.Unmarshal([]byte(doc.Find("#main").Text()), fr)
	if e != nil {
		log.Printf("%s failed to parse json, retrying...", code)
		return false, true
	}
	fr.SetCode(code)
	fins := fr.Items
	//update to database
	if len(fins) > 0 {
		valueStrings := make([]string, 0, len(fins))
		valueArgs := make([]interface{}, 0, len(fins)*20)
		for _, e := range fins {
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "+
				"?, ?)")
			valueArgs = append(valueArgs, e.Code)
			valueArgs = append(valueArgs, e.Alr)
			valueArgs = append(valueArgs, e.Crps)
			valueArgs = append(valueArgs, e.Eps)
			valueArgs = append(valueArgs, e.Gpm)
			valueArgs = append(valueArgs, e.Gr)
			valueArgs = append(valueArgs, e.GrYoy)
			valueArgs = append(valueArgs, e.Itr)
			valueArgs = append(valueArgs, e.Navps)
			valueArgs = append(valueArgs, e.Np)
			valueArgs = append(valueArgs, e.NpAdn)
			valueArgs = append(valueArgs, e.NpAdnYoy)
			valueArgs = append(valueArgs, e.Npm)
			valueArgs = append(valueArgs, e.NpRg)
			valueArgs = append(valueArgs, e.NpYoy)
			valueArgs = append(valueArgs, e.Ocfps)
			valueArgs = append(valueArgs, e.Roe)
			valueArgs = append(valueArgs, e.RoeDlt)
			valueArgs = append(valueArgs, e.Udpps)
			valueArgs = append(valueArgs, e.Year)
		}
		stmt := fmt.Sprintf("INSERT INTO finance (code,alr,crps,eps,gpm,gr,gr_yoy,itr,navps,np,np_adn,"+
			"np_adn_yoy,npm,np_rg,np_yoy,ocfps,roe,roe_dlt,udpps,year) VALUES %s"+
			" on duplicate key update alr=values(alr),crps=values(crps),eps=values(eps),gpm=values(gpm),"+
			"gr=values(gr),gr_yoy=values(gr_yoy),itr=values(itr),navps=values(navps),np=values(np),"+
			"np_adn=values(np_adn),np_adn_yoy=values(np_adn_yoy),npm=values(npm),np_rg=values(np_rg),"+
			"np_yoy=values(np_yoy),ocfps=values(ocfps),roe=values(roe),roe_dlt=values(roe_dlt),"+
			"udpps=values(udpps)",
			strings.Join(valueStrings, ","))
		_, err := dbmap.Exec(stmt, valueArgs...)
		util.CheckErr(err, code + ": failed to bulk update finance")
	}
	return true, false
}
