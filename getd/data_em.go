package getd

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"strings"
	"time"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"github.com/pkg/errors"
	"github.com/ssgreg/repeat"
)

func getKlineEM(stk *model.Stock, kltype []model.DBTab) (
	tdmap map[model.DBTab]*model.TradeData, lkmap map[model.DBTab]int, suc bool) {

	tdmap = make(map[model.DBTab]*model.TradeData)
	lkmap = make(map[model.DBTab]int)
	code := stk.Code
	xdxr := latestUFRXdxr(stk.Code)

	genop := func(tab model.DBTab) (op func(c int) (e error)) {
		return func(c int) (e error) {
			trdat, lklid, suc, retry := emKline(stk, tab, xdxr)
			if suc {
				log.Infof("%s %v fetched: %d", code, tab, trdat.MaxLen())
				tdmap[tab] = trdat
				lkmap[tab] = lklid
				return nil
			}
			e = fmt.Errorf("failed to get kline for %s", code)
			if retry {
				log.Printf("%s retrying [%d]", code, c+1)
				return repeat.HintTemporary(e)
			}
			return repeat.HintStop(e)
		}
	}

	suc = true
	for _, klt := range kltype {
		e := repeat.Repeat(
			repeat.FnWithCounter(genop(klt)),
			repeat.StopOnSuccess(),
			repeat.LimitMaxTries(conf.Args.DataSource.KlineFailureRetry-1),
			repeat.WithDelay(
				repeat.FullJitterBackoff(500*time.Millisecond).WithMaxDelay(5*time.Second).Set(),
			),
		)
		if e != nil {
			suc = false
		}
	}

	return tdmap, lkmap, suc
}

func emKline(stk *model.Stock, tab model.DBTab, xdxr *model.Xdxr) (
	trdat *model.TradeData, lklid int, suc, retry bool) {

	period, authorityType := "", ""
	var cycle model.CYTP
	rtype := model.None
	switch tab {
	case model.KLINE_DAY_F, model.KLINE_DAY_NR, model.KLINE_DAY_B, model.KLINE_DAY_VLD:
		period = "k"
		cycle = model.DAY
	case model.KLINE_WEEK_F, model.KLINE_WEEK_NR, model.KLINE_WEEK_B, model.KLINE_WEEK_VLD:
		period = "wk"
		cycle = model.WEEK
	case model.KLINE_MONTH_F, model.KLINE_MONTH_NR, model.KLINE_MONTH_B, model.KLINE_MONTH_VLD:
		period = "mk"
		cycle = model.MONTH
	default:
		log.Panicf("unsupported type: %+v", tab)
	}
	switch tab {
	case model.KLINE_DAY_F, model.KLINE_WEEK_F, model.KLINE_MONTH_F:
		rtype = model.Forward
	case model.KLINE_DAY_B, model.KLINE_WEEK_B, model.KLINE_MONTH_B:
		rtype = model.Backward
	case model.KLINE_DAY_VLD, model.KLINE_WEEK_VLD, model.KLINE_MONTH_VLD:
		rtype = model.Rtype(conf.Args.DataSource.KlineValidateType)
	}
	switch rtype {
	case model.Forward:
		authorityType = "fa"
	case model.Backward:
		authorityType = "ba"
	}
	mkt := ""
	switch stk.Market.String {
	case model.MarketSH:
		mkt = "1"
	case model.MarketSZ:
		mkt = "2"
	default:
		log.Panicf("unsupported market type: %s", stk.Market.String)
	}
	var symbol string
	code := stk.Code
	if isIndex(symbol) {
		symbol = code[2:] + mkt
	} else {
		symbol = code + mkt
	}
	incr := true
	if rtype == model.Forward {
		incr = xdxr == nil
	}
	lklid = -1
	if incr {
		ldy := getLatestTradeDataBasic(code, cycle, rtype, 5+1) //plus one offset for pre-close, varate calculation
		if ldy != nil {
			lklid = ldy.Klid
		} else {
			log.Printf("%s latest %s data not found, will be fully refreshed", code, tab)
		}
	} else {
		log.Printf("%s %s data will be fully refreshed", code, tab)
	}

	emk, e := tryEMKline(code, symbol, period, authorityType)
	if e != nil {
		return nil, lklid, false, true
	}

	switch tab {
	//for validate kline, we need to supplement kline data with counterparts
	case model.KLINE_DAY_VLD, model.KLINE_WEEK_VLD, model.KLINE_MONTH_VLD:
		var emk2 *model.EMKline
		switch authorityType {
		case "", "fa":
			if emk2, e = tryEMKline(code, symbol, period, "ba"); e == nil {
				for _, k1 := range emk.Data {
					if k2, ok := emk2.DataMap[k1.Date]; ok {
						k1.Xrate = k2.Xrate
					}
				}
			}
		case "ba":
			if emk2, e = tryEMKline(code, symbol, period, ""); e == nil {
				for _, k1 := range emk.Data {
					if k2, ok := emk2.DataMap[k1.Date]; ok {
						k1.Xrate = k2.Amount
					}
				}
			}
		}
		if e != nil {
			log.Warnf("%s failed to supplement EM kline data: %+v", code, e)
			return nil, lklid, false, true
		}
	}

	//construct trade data
	trdat = &model.TradeData{
		Code:          code,
		Cycle:         cycle,
		Reinstatement: rtype,
		Base:          emk.Data,
	}

	return trdat, lklid, true, false
}

//get data from eastmoney.com and convert json to TradeDataBasic
func tryEMKline(code, symbol, period, xdrType string) (emk *model.EMKline, e error) {
	emk = &model.EMKline{Code: code}
	//id = 6008981, 0000022
	//type = k/wk/mk
	//authorityType = /fa/ba
	urlt := `http://pdfm.eastmoney.com/EM_UBG_PDTI_Fast/api/js?&rtntype=5&id=%[1]s&type=%[2]s&authorityType=%[3]s`
	url := fmt.Sprintf(urlt, symbol, period, xdrType)

	var uagent string
	uagent, e = util.PickUserAgent()
	if e != nil {
		e = errors.Wrap(e, "failed to get user agent")
		return
	}
	headers := map[string]string{
		"User-Agent": uagent,
	}
	var px *util.Proxy
	wgt := conf.Args.DataSource.EM.DirectProxyWeight
	sum := wgt[0] + wgt[1] + wgt[2]
	dw := wgt[0] / sum
	mw := (wgt[0] + wgt[1]) / sum
	dice := rand.Float64()
	if dice <= dw {
		//direct connection
		log.Debug("accessing EM using direct connection")
	} else if dice <= mw {
		//master proxy
		log.Debugf("accessing EM using master proxy: %s", conf.Args.Network.MasterProxyAddr)
		ss := strings.Split(conf.Args.Network.MasterProxyAddr, ":")
		px = &util.Proxy{
			Host: ss[0],
			Port: ss[1],
			Type: "socks5",
		}
	} else {
		//rotate proxy
		px, e = util.PickProxy()
		if e != nil {
			e = errors.Wrap(e, "failed to get rotate proxy")
			return
		}
		log.Debugf("accessing EM using rotate proxy: %s://%s:%s", px.Type, px.Host, px.Port)
	}
	res, e := util.HTTPGet(url, headers, px)
	if e != nil {
		e = errors.Wrap(e, "failed to get http response")
		return
	}
	defer res.Body.Close()
	data, e := ioutil.ReadAll(res.Body)
	if e != nil {
		log.Warnf("%s failed to read http response body from %s: %+v", code, url, e)
		return
	}
	//strip parentheses
	e = json.Unmarshal(data[1:len(data)-1], emk)
	if e != nil {
		log.Warnf("%s failed to parse json from %s: %+v\return value:%+v", code, url, e, string(data))
		return
	}
	log.Debugf("return from EM: %+v", string(data))
	return
}
