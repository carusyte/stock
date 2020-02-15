package getd

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"github.com/pkg/errors"
	"github.com/ssgreg/repeat"
)

//XqKlineFetcher is capable of fetching kline data from Xueqiu.
type XqKlineFetcher struct{}

func (f *XqKlineFetcher) extraRequest(fr FetchRequest) FetchRequest {
	return FetchRequest{
		RemoteSource: model.DataSource(conf.Args.DataSource.Validate.Source),
		LocalSource:  model.DataSource(conf.Args.DataSource.Validate.Source),
		Cycle:        fr.Cycle,
		Reinstate:    fr.Reinstate,
	}
}

func (f *XqKlineFetcher) getExtraRequests(frIn []FetchRequest) (frOut []FetchRequest) {
	for _, fr := range frIn {
		frOut = append(frOut, f.extraRequest(fr))
	}
	return
}

//fetchKline from Xueqiu for the given stock.
func (f *XqKlineFetcher) fetchKline(stk *model.Stock, fr FetchRequest, incr bool) (
	tdmap map[FetchRequest]*model.TradeData, lkmap map[FetchRequest]int, suc, retry bool) {

	tdmap = make(map[FetchRequest]*model.TradeData)
	lkmap = make(map[FetchRequest]int)

	period := ""
	xdrType := "normal"
	rtype := fr.Reinstate
	cycle := fr.Cycle
	switch cycle {
	case model.DAY:
		period = "day"
	case model.WEEK:
		period = "week"
	case model.MONTH:
		period = "month"
	default:
		log.Panicf("unsupported cycle: %+v", fr.Cycle)
	}
	switch rtype {
	case model.Forward:
		xdrType = "before"
	case model.Backward:
		xdrType = "after"
	}

	code := stk.Code
	symbol := ""
	if fr.LocalSource == model.Index {
		symbol = strings.ToUpper(code)
	} else {
		mkt := strings.ToUpper(stk.Market.String)
		symbol = mkt + code
	}
	
	tabs := resolveTableNames(fr)
	lkmap[fr] = -1
	ldate := ""
	if incr {
		ldy := getLatestTradeDataBasic(code, fr.LocalSource, cycle, rtype, 5+1) //plus one offset for pre-close, varate calculation
		if ldy != nil {
			ldate = ldy.Date
			lkmap[fr] = ldy.Klid
		} else {
			log.Printf("%s latest %+v data not found, will be fully refreshed", code, tabs)
		}
	} else {
		log.Printf("%s %+v data will be fully refreshed", code, tabs)
	}

	startDateStr := "1990-12-19"
	startDate, e := time.Parse(global.DateFormat, startDateStr)
	if e != nil {
		log.Panicf("failed to parse date %s: %+v", startDateStr, e)
	}
	count := -142
	multiGet := false
	if lkmap[fr] == -1 {
		count = int(math.Round(-.75*(time.Since(startDate).Hours()/24.) - float64(rand.Intn(1000))))
		multiGet = true
	} else {
		ltime, e := time.Parse(global.DateFormat, ldate)
		if e != nil {
			log.Warnf("%s %+v failed to parse date value '%s': %+v", stk.Code, tabs, ldate, e)
			return tdmap, lkmap, false, false
		}
		count = -1 * (int(time.Since(ltime).Hours()/24) + 2)
	}

	xqk, e := tryXQKline(code, symbol, period, xdrType, count, multiGet)
	if e != nil {
		return tdmap, lkmap, false, true
	}

	if extd, exlk, e := fixXqData(stk, xqk, fr); e != nil {
		return tdmap, lkmap, false, false
	} else if len(extd) > 0 && len(exlk) > 0 {
		for k, v := range extd {
			tdmap[k] = v
		}
		for k, v := range exlk {
			lkmap[k] = v
		}
	} else {
		//construct empty signal entry
		exfr := f.extraRequest(fr)
		tdmap[exfr] = nil
		lkmap[exfr] = -1
	}

	//construct trade data
	tdmap[fr] = &model.TradeData{
		Code:          code,
		Source:        fr.LocalSource,
		Cycle:         cycle,
		Reinstatement: rtype,
		Base:          xqk.GetData(false),
	}

	return tdmap, lkmap, true, false
}

//supplement kline data from validate table if any
func fixXqData(stk *model.Stock, k *model.XQKline, fr FetchRequest) (
	extd map[FetchRequest]*model.TradeData, exlk map[FetchRequest]int, e error) {

	if len(k.MissingData) == 0 && len(k.MissingAmount) == 0 {
		return
	}
	vsrc := model.DataSource(conf.Args.DataSource.Validate.Source)
	//check whether local validate kline has the latest data
	dates := make([]string, len(k.Dates))
	copy(dates, k.Dates)
	sort.Strings(dates)
	trdat := GetTrDataAt(
		k.Code,
		TrDataQry{
			LocalSource: vsrc,
			Cycle:       fr.Cycle,
			Reinstate:   fr.Reinstate,
			Basic:       true,
		},
		Date,
		false,
		dates[len(dates)-1],
	)
	dates = append(k.MissingAmount, k.MissingData...)
	bmap := make(map[string]*model.TradeDataBasic)
	tabs := resolveTableNames(fr)
	var unmatched []string
	if len(trdat.Base) > 0 {
		//fetch from db directly
		log.Infof("%s %+v data for the following dates will be updated from local validate kline: %+v",
			k.Code, tabs, dates)
		trdat := GetTrDataAt(
			k.Code,
			TrDataQry{
				LocalSource: vsrc,
				Cycle:       fr.Cycle,
				Reinstate:   fr.Reinstate,
				Basic:       true,
			},
			Date,
			false,
			util.Str2IntfSlice(dates)...,
		)
		bmap = trdat.BaseMap()
	} else {
		//fetch from remote
		log.Infof("%s %+v data for the following dates will be updated from remote validate source: %+v",
			k.Code, tabs, dates)
		kf := kfmap[vsrc]
		suc := false
		exfr := FetchRequest{
			RemoteSource: vsrc,
			LocalSource:  vsrc,
			Cycle:        fr.Cycle,
			Reinstate:    fr.Reinstate,
		}
		extd, exlk, suc = getKlineFromSource(stk, kf, exfr)
		if !suc {
			msg := fmt.Sprintf("%s %+v failed to fix data for the following dates: %+v", k.Code, tabs, dates)
			e = errors.New(msg)
			log.Warnln(msg)
			return
		}
		bmap = extd[exfr].BaseMap()
	}

	for _, d := range k.MissingData {
		if b, ok := bmap[d]; ok {
			kd := k.Data[d]
			kd.Open = b.Open
			kd.Open = b.Open
			kd.High = b.High
			kd.Close = b.Close
			kd.Low = b.Low
			if !kd.Amount.Valid || kd.Amount.Float64 == 0 {
				kd.Amount = b.Amount
			}
			if !kd.Volume.Valid || kd.Volume.Float64 == 0 {
				kd.Volume = b.Volume
			}
			if !kd.Xrate.Valid || kd.Xrate.Float64 == 0 {
				kd.Xrate = b.Xrate
			}
		} else {
			unmatched = append(unmatched, b.Date)
		}
	}

	for _, d := range k.MissingAmount {
		if b, ok := bmap[d]; ok && b.Amount.Valid {
			k.Data[d].Amount = b.Amount
		} else {
			unmatched = append(unmatched, d)
		}
	}

	if len(unmatched) > 0 {
		log.Warnf("%s unable to fix missing %+v data from validate kline for the following dates: %+v",
			k.Code, tabs, unmatched)
		if conf.Args.DataSource.XQ.DropInconsistent {
			log.Warnf("%s dropping inconsistent %+v data for the following dates: %+v",
				k.Code, tabs, unmatched)
			for _, u := range unmatched {
				delete(k.Data, u)
				for i, d := range k.Dates {
					if u == d {
						k.Dates = append(k.Dates[:i], k.Dates[i+1:]...)
						break
					}
				}
			}
		}
	}

	return
}

func tryXQCookie() (cookies []*http.Cookie, px *util.Proxy, headers map[string]string, e error) {
	op := func() error {
		cookies, px, headers, e = xqCookie()
		if e != nil {
			return repeat.HintTemporary(e)
		}
		return nil
	}
	e = repeat.Repeat(
		repeat.Fn(op),
		repeat.StopOnSuccess(),
		repeat.LimitMaxTries(conf.Args.DefaultRetry),
	)
	if e != nil {
		log.Warnf("failed to get cookies from XQ: %+v", e)
	}
	return
}

func tryXQKline(code, symbol, period, xdrType string, count int, multiGet bool) (xqk *model.XQKline, e error) {
	xqk = &model.XQKline{Code: code}
	//symbol = SH600104
	//begin = 1579589390096
	//period = day/week/month/60m/120m...
	//type = normal/after(forward)/before(backward)
	//count = -1000
	urlt := `https://stock.xueqiu.com/v5/stock/chart/kline.json?` +
		`symbol=%[1]s&begin=%[2]d&period=%[3]s&type=%[4]s&count=%[5]d&indicator=kline`
	begin := util.UnixMilliseconds(time.Now().AddDate(0, 0, 1))
	url := fmt.Sprintf(urlt, symbol, begin, period, xdrType, count)
	RETRY := 5
	genop := func(url string, hd map[string]string,
		px *util.Proxy, ck []*http.Cookie) (op func(c int) error) {
		return func(c int) error {
			res, e := util.HTTPGet(url, hd, px, ck...)
			if e != nil {
				log.Warnf("%s HTTP failed from %s: %+v", code, url, e)
				return repeat.HintTemporary(e)
			}
			defer res.Body.Close()
			data, e := ioutil.ReadAll(res.Body)
			if e != nil {
				util.UpdateProxyScore(px, false)
				log.Warnf("%s failed to read http response body from %s: %+v", code, url, e)
				return repeat.HintTemporary(e)
			}
			util.UpdateProxyScore(px, true)
			e = json.Unmarshal(data, xqk)
			if e != nil {
				if strings.Contains(e.Error(), "400016") { //cookie timeout
					log.Warnf("%s cookie timeout for %s: %+v", code, url, e)
					return repeat.HintStop(e)
				}
				log.Warnf("%s failed to parse json from %s: %+v\return value:%+v", code, url, e, string(data))
				return repeat.HintTemporary(e)
			}
			log.Debugf("return from XQ: %+v", string(data))
			return nil
		}
	}
	var (
		ck []*http.Cookie
		px *util.Proxy
		hd map[string]string
	)
	//first get the cookies from home page
	if ck, px, hd, e = tryXQCookie(); e != nil {
		return
	}
	//get kline using same header, proxy and cookies
	if e = repeat.Repeat(
		repeat.FnWithCounter(genop(url, hd, px, ck)),
		repeat.StopOnSuccess(),
		repeat.LimitMaxTries(RETRY),
		repeat.WithDelay(repeat.FullJitterBackoff(200*time.Millisecond).WithMaxDelay(2*time.Second).Set()),
	); e != nil {
		return
	}
	//check if more data is required
	ckTimeout := 0
	for multiGet && xqk.NumAdded == count && ckTimeout < 2 {
		data := xqk.GetData(false)
		var startDate time.Time
		startDate, e = time.Parse(global.DateFormat, data[0].Date)
		if e != nil {
			log.Warnf("%s failed to parse date %s: %+v", code, data[0].Date, e)
			return
		}
		begin = util.UnixMilliseconds(startDate.AddDate(0, 0, -1))
		url = fmt.Sprintf(urlt, symbol, begin, period, xdrType, count)
		if e = repeat.Repeat(
			repeat.FnWithCounter(genop(url, hd, px, ck)),
			repeat.StopOnSuccess(),
			repeat.LimitMaxTries(RETRY),
			repeat.WithDelay(repeat.FullJitterBackoff(200*time.Millisecond).WithMaxDelay(2*time.Second).Set()),
		); e != nil {
			if strings.Contains(e.Error(), "400016") { //cookie timeout, refresh cookie then retry
				if ck, px, hd, e = tryXQCookie(); e != nil {
					return
				}
				ckTimeout++
				continue
			}
			return
		}
		ckTimeout = 0
	}
	return
}

func xqCookie() (cookies []*http.Cookie, px *util.Proxy, headers map[string]string, e error) {
	homePage := `https://xueqiu.com/`
	var uagent string
	uagent, e = util.PickUserAgent()
	if e != nil {
		e = errors.Wrap(e, "failed to get user agent")
		return
	}
	headers = map[string]string{
		"User-Agent": uagent,
	}
	wgt := conf.Args.DataSource.XQ.DirectProxyWeight
	sum := wgt[0] + wgt[1] + wgt[2]
	dw := wgt[0] / sum
	mw := (wgt[0] + wgt[1]) / sum
	dice := rand.Float64()
	if dice <= dw {
		//direct connection
		log.Debug("accessing XQ using direct connection")
	} else if dice <= mw {
		//master proxy
		log.Debugf("accessing XQ using master proxy: %s", conf.Args.Network.MasterProxyAddr)
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
		log.Debugf("accessing XQ using rotate proxy: %s://%s:%s", px.Type, px.Host, px.Port)
	}
	res, e := util.HTTPGet(homePage, headers, px)
	if e != nil {
		e = errors.Wrap(e, "failed to get http response")
		return
	}
	defer res.Body.Close()
	util.UpdateProxyScore(px, true)
	cookies = res.Cookies()
	return
}

func xqShares(stock *model.Stock, px *util.Proxy, headers map[string]string, cookies []*http.Cookie) (ok, retry bool) {
	// get share info from xueqiu.com
	// https://xueqiu.com/snowman/S/SH601598/detail#/GBJG
	// https://stock.xueqiu.com/v5/stock/f10/cn/shareschg.json?symbol=SH601598&count=100&extend=true
	url := fmt.Sprintf(`https://stock.xueqiu.com/v5/stock/f10/cn/shareschg.json?symbol=%s%s&count=1000&extend=true`, stock.Market.String, stock.Code)
	res, e := util.HTTPGet(url, headers, px, cookies...)
	if e != nil {
		log.Printf("%s, http failed %s", stock.Code, url)
		return false, true
	}
	defer res.Body.Close()
	var xqshare model.XqSharesChg
	var body []byte
	if body, e = ioutil.ReadAll(res.Body); e != nil {
		log.Printf("[%s,%s] failed to read from response body, retrying...", stock.Code,
			stock.Name)
		util.UpdateProxyScore(px, false)
		return false, true
	}
	util.UpdateProxyScore(px, true)
	if strings.Contains(string(body), `"error_code": "400016"`) {
		log.Warnf("%s cookie timeout: %+v", stock.Code, string(body))
		return false, true
	} else if e = json.Unmarshal(body, &xqshare); e != nil {
		log.Printf("[%s,%s] failed to parse json body, retrying...", stock.Code,
			stock.Name)
		return false, true
	}
	if xqshare.ErrorCode != 0 {
		log.Printf("[%s,%s] failed from xueqiu.com:[%d, %s] retrying...", stock.Code,
			stock.Name, xqshare.ErrorCode, xqshare.ErrorDesc)
		return false, true
	} else if len(xqshare.Data.Items) == 0 {
		log.Printf("[%s,%s] no share info from xueqiu.com", stock.Code, stock.Name)
		return true, false
	}
	mod := 0.00000001
	s := xqshare.Data.Items[0]
	if s.TotalShare != nil {
		stock.ShareSum.Valid = true
		stock.ShareSum.Float64 = *s.TotalShare * mod
	}
	if s.FloatAShare != nil {
		stock.AShareSum.Valid = true
		stock.AShareSum.Float64 += *s.FloatAShare
		stock.AShareExch.Valid = true
		stock.AShareExch.Float64 = *s.FloatAShare * mod
	}
	if s.LimitAShare != nil {
		stock.AShareSum.Valid = true
		stock.AShareSum.Float64 += *s.LimitAShare
		stock.AShareR.Valid = true
		stock.AShareR.Float64 = *s.LimitAShare * mod
	}
	stock.AShareSum.Float64 *= mod

	if s.FloatBShare != nil {
		stock.BShareSum.Valid = true
		stock.BShareSum.Float64 += *s.FloatBShare
		stock.BShareExch.Valid = true
		stock.BShareExch.Float64 = *s.FloatBShare * mod
	}
	if s.LimitBShare != nil {
		stock.BShareSum.Valid = true
		stock.BShareSum.Float64 += *s.LimitBShare
		stock.BShareR.Valid = true
		stock.BShareR.Float64 = *s.LimitBShare * mod
	}
	stock.BShareSum.Float64 *= mod

	if s.FloatHShare != nil {
		stock.HShareSum.Valid = true
		stock.HShareSum.Float64 += *s.FloatHShare
		stock.HShareExch.Valid = true
		stock.HShareExch.Float64 = *s.FloatHShare * mod
	}
	if s.LimitHShare != nil {
		stock.HShareSum.Valid = true
		stock.HShareSum.Float64 += *s.LimitHShare
		stock.HShareR.Valid = true
		stock.HShareR.Float64 = *s.LimitHShare * mod
	}
	stock.HShareSum.Float64 *= mod

	return true, false
}
