package getd

import (
	"fmt"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
)

func ParseIfengBonus(stock *model.Stock) (ok, retry bool) {
	urlt := `http://app.finance.ifeng.com/data/stock/fhpxjl.php?symbol=%s`
	url := fmt.Sprintf(urlt, stock.Code)

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

	//parse tables
	var field string
	doc.Find("body div.main div div.contentR div.block02 div table").Each(func(it int, st *goquery.Selection) {
		xdxr := newXdxr()
		xdxrs = append(xdxrs, xdxr)
		st.Find("tbody tr").Each(func(itr int, str *goquery.Selection) {
			str.Find("td").Each(func(itd int, std *goquery.Selection) {
				td := strings.TrimSpace(std.Text())
				if itd%2 == 0 {
					field = td
				} else {
					if "--" == td || "" == td {
						return
					}
					switch field {
					case `公告日期`:
						xdxr.NoticeDate = util.Str2Snull(td)
					case `分红截止日期`:
						xdxr.DiviEndDate = util.Str2Snull(td)
					case `分红对象`:
						xdxr.DiviTarget = util.Str2Snull(td)
					case `派息股本基数`:
						xdxr.SharesBase = util.Str2Inull(strings.TrimSuffix(td, "股"))
					case `每10股现金(含税)`:
						xdxr.Divi = util.Str2Fnull(strings.TrimSuffix(td, "元"))
					case `每10股现金(税后)`:
						xdxr.DiviAtx = util.Str2Fnull(strings.TrimSuffix(td, "元"))
					case `每10股送红股`:
						xdxr.SharesAllot = util.Str2Fnull(strings.TrimSuffix(td, "股"))
					case `每10股转增股本`:
						xdxr.SharesCvt = util.Str2Fnull(strings.TrimSuffix(td, "股"))
					case `股权登记日`:
						xdxr.RegDate = util.Str2Snull(td)
					case `除权除息日`:
						xdxr.XdxrDate = util.Str2Snull(td)
					case `最后交易日`:
						xdxr.EndTrdDate = util.Str2Snull(td)
					case `股息到帐日`:
						xdxr.PayoutDate = util.Str2Snull(td)
					case `红股上市日`:
						xdxr.SharesAllotDate = util.Str2Snull(td)
					case `转增股本上市日`:
						xdxr.SharesCvtDate = util.Str2Snull(td)
					default:
						log.Printf("%s unidentified field: %s", stock.Code, field)
					}
				}

			})
		})
		xdxr.Code = stock.Code
		xdxr.Name = stock.Name
	})

	// no records found, return normally
	if len(xdxrs) == 0 {
		return true, false
	}

	for i, j := len(xdxrs)-1, 0; i >= 0; i, j = i-1, j+1 {
		xdxrs[i].Idx = j
	}

	//calcDprDyr(xdxrs)

	saveXdxrs(xdxrs)

	return true, false
}

//func calcDprDyr(xdxrs []*model.Xdxr) {
//	for _, x := range xdxrs {
//
//	}
//}
