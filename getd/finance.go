package getd

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
	"log"
	"math"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
)

func GetXDXRs(stocks []*model.Stock) {
	log.Println("getting XDXR info...")
	var wg sync.WaitGroup
	chstk := make(chan *model.Stock, global.JOB_CAPACITY)
	for i := 0; i < global.MAX_CONCURRENCY; i++ {
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
	// target web server can't withstand heavy traffic
	RETRIES := 5
	for stock := range chstk {
		for rtCount := 0; rtCount <= RETRIES; rtCount++ {
			//FIXME use http://basic.10jqka.com.cn/600383/bonus.html to extract info instead
			ok, r := parse10jqkBonus(stock)
			//ok, r := ParseIfengBonus(stock)
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

func parse10jqkBonus(stock *model.Stock) (ok, retry bool) {
	//urlt := `http://stockpage.10jqka.com.cn/%s/bonus/`
	urlt := `http://basic.10jqka.com.cn/%s/bonus.html`
	url := fmt.Sprintf(urlt, stock.Code)

	var xdxrs []*model.Xdxr
	// Load the URL
	res, e := util.HttpGetResp(url)
	if e != nil {
		log.Printf("%s, http failed, giving up %s", stock.Code, url)
		return false, false
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

	//parse column index
	iReportYear, iBoardDate, iGmsDate, iImplDate, iPlan, iRegDate, iXdxrDate, iProgress, iPayoutRatio,
	iDivRate, iPayoutDate := -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1
	doc.Find(`#bonus_table thead tr`).Each(func(i int, s *goquery.Selection) {
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
				iRegDate = j
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
				case iRegDate:
					xdxr.RegDate = util.Str2Snull(v)
				case iXdxrDate:
					xdxr.XdxrDate = util.Str2Snull(v)
				case iPayoutDate:
					xdxr.PayoutDate = util.Str2Snull(v)
				case iProgress:
					xdxr.Progress = util.Str2Snull(v)
				case iPayoutRatio:
					// skip dyr and dpr from the web and calculate later
					//xdxr.Dpr = util.Str2Fnull(strings.TrimSpace(strings.TrimSuffix(v,
					//	"%")))
				case iDivRate:
					// skip dyr and dpr from the web and calculate later
					//xdxr.Dyr = util.Str2Fnull(strings.TrimSpace(strings.TrimSuffix(v,
					//	"%")))
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
		xdxrs[i].Idx = j
	}

	calcDyrDpr(xdxrs)
	saveXdxrs(xdxrs)

	return true, false
}

// calculates dyr and dpr dynamically
func calcDyrDpr(xdxrs []*model.Xdxr) {
	for _, x := range xdxrs {
		if x.Divi.Valid && x.Divi.Float64 > 0 {
			var price float64 = math.NaN()
			var date string = time.Now().Format("2006-01-02")
			// use normal price at reg_date or impl_date, if not found, use the day before that day
			if x.RegDate.Valid {
				date = x.RegDate.String
			} else if x.ImplDate.Valid {
				date = x.ImplDate.String
			}
			c, e := dbmap.SelectNullFloat("select close from kline_d_n where code = ? "+
				"and date = ?", x.Code, date)
			util.CheckErrNop(e, x.Code+" failed to query close from kline_d_n at "+date)

			if e == nil {
				if c.Valid {
					price = c.Float64
				} else {
					c, e = dbmap.SelectNullFloat("select close from kline_d_n "+
						"where code = ? and date < ? order by klid desc limit "+
						"1", x.Code, date)
					util.CheckErrNop(e, x.Code + " failed to query close from "+
						"kline_d_n the day before "+ date)
					if e == nil {
						price = c.Float64
					}
				}
			}

			if math.IsNaN(price) {
				// use latest price
				c, e := dbmap.SelectNullFloat("select close from kline_d_n where code = ? "+
					"order by date desc limit 1", x.Code)
				util.CheckErrNop(e, x.Code+" failed to query lastest close from kline_d_n")
				if e == nil && c.Valid {
					price = c.Float64
				}
			}

			if math.IsNaN(price) {
				log.Printf("failed to calculate dyr for %s at %s", x.Code, x.ReportYear)
			} else if price != 0 {
				x.Dyr.Float64 = x.Divi.Float64 / price / 10.0
				x.Dyr.Valid = true
			}

			// calculates dpr
			eps, e := dbmap.SelectNullFloat("select eps from finance where code = ? "+
				"and year < ? and year like '%-12-31' order by year desc limit 1", x.Code, date)
			if e != nil{
				log.Printf("failed to query eps for %s before %s", x.Code, date)
			}else{
				if eps.Valid && eps.Float64 != 0 {
					x.Dpr.Float64 = x.Divi.Float64 / eps.Float64 / 10.0
					x.Dpr.Valid = true
				}
			}
		}
	}
}

//update to database
func saveXdxrs(xdxrs []*model.Xdxr) {
	if len(xdxrs) > 0 {
		code := xdxrs[0].Code
		valueStrings := make([]string, 0, len(xdxrs))
		valueArgs := make([]interface{}, 0, len(xdxrs)*25)
		for _, e := range xdxrs {
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "+
				"?, ?, ?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, e.Code)
			valueArgs = append(valueArgs, e.Name)
			valueArgs = append(valueArgs, e.Idx)
			valueArgs = append(valueArgs, e.NoticeDate)
			valueArgs = append(valueArgs, e.ReportYear)
			valueArgs = append(valueArgs, e.BoardDate)
			valueArgs = append(valueArgs, e.GmsDate)
			valueArgs = append(valueArgs, e.ImplDate)
			valueArgs = append(valueArgs, e.Plan)
			valueArgs = append(valueArgs, e.Divi)
			valueArgs = append(valueArgs, e.DiviAtx)
			valueArgs = append(valueArgs, e.DiviEndDate)
			valueArgs = append(valueArgs, e.SharesAllot)
			valueArgs = append(valueArgs, e.SharesAllotDate)
			valueArgs = append(valueArgs, e.SharesCvt)
			valueArgs = append(valueArgs, e.SharesCvtDate)
			valueArgs = append(valueArgs, e.RegDate)
			valueArgs = append(valueArgs, e.XdxrDate)
			valueArgs = append(valueArgs, e.PayoutDate)
			valueArgs = append(valueArgs, e.Progress)
			valueArgs = append(valueArgs, e.Dpr)
			valueArgs = append(valueArgs, e.Dyr)
			valueArgs = append(valueArgs, e.DiviTarget)
			valueArgs = append(valueArgs, e.SharesBase)
			valueArgs = append(valueArgs, e.EndTrdDate)
		}
		stmt := fmt.Sprintf("INSERT INTO xdxr (code,name,idx,notice_date,report_year,board_date,"+
			"gms_date,impl_date,plan,divi,divi_atx,divi_end_date,shares_allot,shares_allot_date,shares_cvt,"+
			"shares_cvt_date,reg_date,xdxr_date,payout_date,progress,dpr,"+
			"dyr,divi_target,shares_base,end_trddate) VALUES %s "+
			"on duplicate key update name=values(name),notice_date=values(notice_date),report_year=values"+
			"(report_year),board_date=values"+
			"(board_date),gms_date=values(gms_date),impl_date=values(impl_date),plan=values(plan),"+
			"divi=values(divi),divi_atx=values(divi_atx),divi_end_date=values"+
			"(divi_end_date),shares_allot=values(shares_allot),shares_allot_date=values"+
			"(shares_allot_date),shares_cvt=values"+
			"(shares_cvt),shares_cvt_date=values(shares_cvt_date),reg_date=values(reg_date),"+
			"xdxr_date=values"+
			"(xdxr_date),payout_date=values(payout_date),progress=values(progress),dpr=values"+
			"(dpr),dyr=values(dyr),divi_target=values(divi_target),"+
			"shares_base=values(shares_base),end_trddate=values(end_trddate)",
			strings.Join(valueStrings, ","))
		_, err := global.Dbmap.Exec(stmt, valueArgs...)
		util.CheckErr(err, code+": failed to bulk update xdxr")
	}
}

func newXdxr() *model.Xdxr {
	xdxr := &model.Xdxr{}
	xdxr.SharesAllot = sql.NullFloat64{0, false}
	xdxr.SharesCvt = sql.NullFloat64{0, false}
	xdxr.Dyr = sql.NullFloat64{0, false}
	xdxr.Divi = sql.NullFloat64{0, false}
	xdxr.Dpr = sql.NullFloat64{0, false}
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
		for i := len(allot) - 1; i > 0; i-- {
			xdxr.SharesAllot.Float64 += util.Str2F64(allot[i])
			xdxr.SharesAllot.Valid = true
		}
	}
	if cvt != nil {
		for i := len(cvt) - 1; i > 0; i-- {
			xdxr.SharesCvt.Float64 += util.Str2F64(cvt[i])
			xdxr.SharesCvt.Valid = true
		}
	}
	if div != nil {
		for i := len(div) - 1; i > 0; i-- {
			xdxr.Divi.Float64 += util.Str2F64(div[i])
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
	chstk := make(chan *model.Stock, global.JOB_CAPACITY)
	for i := 0; i < global.MAX_CONCURRENCY; i++ {
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
	urlt := `http://basic.10jqka.com.cn/%s/finance.html`
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
	// Convert the designated charset HTML to utf-8 encoded HTML.
	utfBody := transform.NewReader(res.Body, simplifiedchinese.GBK.NewDecoder())
	// parse body using goquery
	doc, e = goquery.NewDocumentFromReader(utfBody)
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
		_, err := global.Dbmap.Exec(stmt, valueArgs...)
		util.CheckErr(err, code+": failed to bulk update finance")
	}
	return true, false
}
