package proxy

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"github.com/carusyte/stock/util"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

var agentPool []string

//PickUserAgent picks a user agent string from the pool randomly.
//if the pool is not populated, it will trigger the initialization process
//to fetch user agent lists from remote server.
func PickUserAgent() (ua string, e error) {
	if len(agentPool) > 0 {
		return agentPool[rand.Intn(len(agentPool))], nil
	}
	log.Println("fetching user agent list from remote server...")
	url := fmt.Sprintf(`http://basic.10jqka.com.cn/%s/equity.html`, stock.Code)
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

	//parse share structure table
	doc.Find("#stockcapit div.bd.pt5 table tbody tr").Each(
		func(i int, s *goquery.Selection) {
			typ := strings.TrimSpace(s.Find("th").Text())
			if "变动原因" == typ {
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
				log.Panicf("%s unrecognized type: %s, url: %s", stock.Code, typ, url)
				return
			}
		})

	return agentPool[rand.Intn(len(agentPool))], nil
}
