package getd

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"github.com/pkg/errors"
	"github.com/ssgreg/repeat"
)

//EmKlineFetcher is capable of fetching kline data from eastmoney.
type EmKlineFetcher struct {
	//key(code_cycle_rtype) -> []*model.TradeDataBasic
	klineData map[string][]*model.TradeDataBasic
	lock      sync.RWMutex
}

//Cleanup resources
func (f *EmKlineFetcher) Cleanup() {
	f.klineData = nil
}

func (f *EmKlineFetcher) cache(td *model.TradeData) {
	lock.Lock()
	defer lock.Unlock()
	if f.klineData == nil {
		f.klineData = make(map[string][]*model.TradeDataBasic)
	}
	f.klineData[f.cacheKey(td.Code, td.Cycle, td.Reinstatement)] = td.Base
}

func (f *EmKlineFetcher) cacheKey(code string, c model.CYTP, r model.Rtype) string {
	return fmt.Sprintf("%v_%v_%v", code, c, r)
}

func (f *EmKlineFetcher) cachedValue(code string, c model.CYTP, r model.Rtype) (cv []*model.TradeDataBasic) {
	if f.klineData == nil {
		return
	}
	key := f.cacheKey(code, c, r)
	return f.klineData[key]
}

//FetchKline from eastmoney for the given stock.
func (f *EmKlineFetcher) FetchKline(stk *model.Stock, fr FetchRequest, incr bool) (
	trdat *model.TradeData, lklid int, suc, retry bool) {

	code := stk.Code
	lklid = -1
	cycle := fr.Cycle
	rtype := fr.Reinstate

	//otherwise, need to fetch from remote
	period, authorityType := "", ""
	switch cycle {
	case model.DAY:
		period = "k"
	case model.WEEK:
		period = "wk"
	case model.MONTH:
		period = "mk"
	default:
		log.Panicf("unsupported cycle: %+v", cycle)
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
	if isIndex(symbol) {
		symbol = code[2:] + mkt
	} else {
		symbol = code + mkt
	}

	tabs := resolveTableNames(fr)

	//if target data has been cached previously, fetch from cache
	var emk *model.EMKline
	var e error
	cv := f.cachedValue(code, cycle, rtype)
	if len(cv) > 0 {
		log.Printf("%s %+v data has been fetched from cache", code, tabs)
		emk = newEMKline(code, symbol, period, authorityType, cv)
	} else {
		log.Printf("%s %+v data will be fully refreshed", code, tabs)
		emk, e = tryEMKline(code, symbol, period, authorityType)
		if e != nil {
			return nil, lklid, false, true
		}
	}

	e = fixEMKline(f, emk, fr)
	if e != nil {
		log.Warn(e)
		return nil, lklid, false, true
	}

	//construct trade data
	trdat = &model.TradeData{
		Source:        fr.LocalSource,
		Code:          code,
		Cycle:         cycle,
		Reinstatement: rtype,
		Base:          emk.Data,
	}

	if rtype == model.Backward || rtype == model.None {
		f.cache(trdat)
	}

	return trdat, lklid, true, false
}

func newEMKline(code, symbol, period, authorityType string, data []*model.TradeDataBasic) (emk *model.EMKline) {
	emk = &model.EMKline{
		Code:     code,
		Symbol:   symbol,
		Period:   period,
		AuthType: authorityType,
		Data:     data,
		DataMap:  make(map[string]*model.TradeDataBasic),
	}
	for _, d := range data {
		emk.Dates = append(emk.Dates, d.Date)
		emk.DataMap[d.Date] = d
	}
	return
}

//fix kline data with counterparts
func fixEMKline(f *EmKlineFetcher, k *model.EMKline, fr FetchRequest) (e error) {
	ctr := model.Backward
	if fr.Reinstate == model.Backward {
		ctr = model.None
	}

	if fixEMKfromCache(f, k, fr, ctr) {
		return
	} else if fixEMKfromDB(k, fr, ctr) {
		return
	}

	return fixEMKfromRemote(f, k, fr, ctr)
}

func fixEMKfromDB(k *model.EMKline, fr FetchRequest, ctr model.Rtype) bool {
	ltb := getLatestTradeDataBasic(k.Code, fr.LocalSource, fr.Cycle, ctr, 0)
	if ltb == nil {
		return false
	}
	dbDate, e := time.Parse(global.DateFormat, ltb.Date)
	if e != nil {
		log.Panicf("%s invalid time format from db: %s, %+v", k.Code, ltb.Date, e)
	}
	kd := k.Dates[len(k.Dates)-1]
	kDate, e := time.Parse(global.DateFormat, kd)
	if e != nil {
		log.Panicf("%s invalid time format from remote: %s, %+v", k.Code, kd, e)
	}
	if dbDate.Sub(kDate) < 0 {
		return false
	}
	trdat := GetTrDataAt(
		k.Code,
		TrDataQry{
			LocalSource: fr.LocalSource,
			Cycle:       fr.Cycle,
			Reinstate:   ctr,
			Basic:       true,
		},
		Date,
		false,
		util.Str2IntfSlice(k.Dates)...,
	)
	bmap := trdat.BaseMap()
	switch ctr {
	case model.Backward:
		for _, k1 := range k.Data {
			if k2, ok := bmap[k1.Date]; ok {
				k1.Xrate = k2.Xrate
			}
		}
	default:
		for _, k1 := range k.Data {
			if k2, ok := bmap[k1.Date]; ok {
				k1.Amount = k2.Amount
			}
		}
	}
	return true
}

func fixEMKfromCache(f *EmKlineFetcher, k *model.EMKline, fr FetchRequest, ctr model.Rtype) bool {
	cv := f.cachedValue(k.Code, fr.Cycle, ctr)
	if len(cv) == 0 {
		return false
	}
	cmap := make(map[string]*model.TradeDataBasic)
	for _, b := range cv {
		cmap[b.Date] = b
	}
	switch ctr {
	case model.Backward:
		for _, k1 := range k.Data {
			if k2, ok := cmap[k1.Date]; ok {
				k1.Xrate = k2.Xrate
			}
		}
	default:
		for _, k1 := range k.Data {
			if k2, ok := cmap[k1.Date]; ok {
				k1.Amount = k2.Amount
			}
		}
	}
	return true
}

func fixEMKfromRemote(f *EmKlineFetcher, k *model.EMKline, fr FetchRequest, ctr model.Rtype) (e error) {
	xdr := ""
	if ctr == model.Backward {
		xdr = "ba"
	}
	var emk2 *model.EMKline
	op := func(error) error {
		emk2, e = tryEMKline(k.Code, k.Symbol, k.Period, xdr)
		return e
	}
	if e = repeat.Repeat(
		repeat.FnHintTemporary(op),
		repeat.StopOnSuccess(),
		repeat.LimitMaxTries(conf.Args.DefaultRetry),
	); e != nil {
		e = errors.Wrapf(e, "failed to supplement EM kline data for %s, %s, %s", k.Symbol, k.Period, k.AuthType)
		return
	}
	f.cache(&model.TradeData{
		Source:        fr.LocalSource,
		Code:          k.Code,
		Cycle:         fr.Cycle,
		Reinstatement: ctr,
		Base:          emk2.Data,
	})
	switch ctr {
	case model.Backward:
		for _, k1 := range k.Data {
			if k2, ok := emk2.DataMap[k1.Date]; ok {
				k1.Xrate = k2.Xrate
			}
		}
	default:
		for _, k1 := range k.Data {
			if k2, ok := emk2.DataMap[k1.Date]; ok {
				k1.Amount = k2.Amount
			}
		}
	}
	return nil
}

//get data from eastmoney.com and convert json to TradeDataBasic
func tryEMKline(code, symbol, period, xdrType string) (emk *model.EMKline, e error) {
	emk = &model.EMKline{
		Code:     code,
		Symbol:   symbol,
		Period:   period,
		AuthType: xdrType,
	}
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
		util.UpdateProxyScore(px, false)
		return
	}
	util.UpdateProxyScore(px, true)
	if len(data) == 0 {
		e = errors.Errorf("no data returned from %s", url)
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
