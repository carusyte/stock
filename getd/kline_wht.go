package getd

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
)

func getKlineWht(stk *model.Stock, kltype []model.DBTab) (qmap map[model.DBTab][]*model.Quote, suc bool) {
	qmap = make(map[model.DBTab][]*model.Quote)
	for _, klt := range kltype {
		quotes, suc := whtKline(stk, klt)
		if !suc {
			return qmap, false
		}
		qmap[klt] = quotes
	}
	return qmap, true
}

func whtKline(stk *model.Stock, tab model.DBTab) (quotes []*model.Quote, suc bool) {
	url := conf.Args.DataSource.WhtURL + "/hq/hiskline"
	lklid := -1
	klt := ""
	xdrType := "none"
	switch tab {
	case model.KLINE_DAY_NR:
		klt = "day"
	case model.KLINE_WEEK_NR:
		klt = "week"
	case model.KLINE_MONTH_NR:
		klt = "month"
	}
	mkt := strings.ToLower(stk.Market.String)
	ldate := ""
	ldy := getLatestKl(stk.Code, tab, 5+1) //plus one offset for pre-close, varate calculation
	if ldy != nil {
		ldate = ldy.Date
		lklid = ldy.Klid
	} else {
		log.Printf("%s latest %s data not found, will be fully refreshed", stk.Code, tab)
	}
	num := "0"
	if lklid != -1 {
		ltime, e := time.Parse("2006-01-02", ldate)
		if e != nil {
			log.Printf("%s %+v failed to parse date value '%s': %+v", stk.Code, tab, ldate, e)
			return nil, false
		}
		num = fmt.Sprintf("%d", int(time.Since(ltime).Hours()/24)+1)
	}
	form := map[string]string{
		"stkCode": mkt + stk.Code,
		// "market":    mkt,
		"xdrType":   xdrType,
		"kLineType": klt,
		"num":       num, // 0: fetch all
	}
	body, e := util.HTTPPostJSON(url, nil, form)
	if e != nil {
		log.Printf("%s failed to get %v from %s: %+v", stk.Code, tab, url, e)
		return nil, false
	}
	data := make([]map[string]interface{}, 0, 16)
	e = json.Unmarshal(body, &data)
	if e != nil {
		log.Printf("%s failed to get %v from %s: %+v\return value:%+v", stk.Code, tab, url, e, string(body))
		return nil, false
	}
	//extract quotes
	quotes = make([]*model.Quote, 0, 16)
	for _, m := range data {
		date := m["date"].(string)[:8]
		date = date[:4] + "-" + date[4:6] + "-" + date[6:]
		if date <= ldate {
			continue
		}
		q := new(model.Quote)
		q.Code = stk.Code
		q.Date = date
		q.Open = m["open"].(float64)
		q.Close = m["close"].(float64)
		q.High = m["high"].(float64)
		q.Low = m["low"].(float64)
		q.Volume = sql.NullFloat64{Float64: m["vol"].(float64), Valid: true}
		q.Amount = sql.NullFloat64{Float64: m["amt"].(float64), Valid: true}
		q.Ma5 = sql.NullFloat64{Float64: m["avg5"].(float64), Valid: true}
		q.Ma10 = sql.NullFloat64{Float64: m["avg10"].(float64), Valid: true}
		q.Ma20 = sql.NullFloat64{Float64: m["avg20"].(float64), Valid: true}
		q.Ma30 = sql.NullFloat64{Float64: m["avg30"].(float64), Valid: true}
		q.Ma60 = sql.NullFloat64{Float64: m["avg60"].(float64), Valid: true}
		q.Ma120 = sql.NullFloat64{Float64: m["avg120"].(float64), Valid: true}
		// special case treated as non-trading date and should be skipped
		preClose := m["preClose"].(float64)
		if preClose == q.Close &&
			q.Close == q.Open &&
			q.Close == q.High &&
			q.Close == q.Low &&
			q.Volume.Float64 == 0 &&
			q.Amount.Float64 == 0 {
			log.Printf("%s %v skipping dummy data:%+v", q.Code, tab, m)
			continue
		}
		quotes = append(quotes, q)
	}
	supplementMisc(quotes, lklid)
	quotes = quotes[1:]
	binsert(quotes, string(tab), lklid)
	return quotes, true
}
