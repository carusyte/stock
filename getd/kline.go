package getd

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type dbJob struct {
	stock  *model.Stock
	quotes []*model.Quote
	table  model.DBTab
	klid   int
}

var (
	chDbjob map[model.DBTab]chan *dbJob
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

//GetKlines Get various types of kline data for the given stocks. Returns the stocks that have been successfully processed.
func GetKlines(stks *model.Stocks, kltype ...model.DBTab) (rstks *model.Stocks) {
	//TODO find a way to get minute level klines
	defer Cleanup()
	log.Printf("begin to fetch kline data: %+v", kltype)
	var wg sync.WaitGroup
	parallel := conf.Args.ChromeDP.PoolSize
	switch conf.Args.DataSource.Kline {
	case conf.TENCENT:
		parallel = conf.Args.Concurrency
	}
	wf := make(chan int, parallel)
	outstks := make(chan *model.Stock, JOB_CAPACITY)
	rstks = new(model.Stocks)
	wgr := collect(rstks, outstks)
	chDbjob = createDbJobQueues(kltype...)
	wgdb := saveQuotes(outstks)
	for _, stk := range stks.List {
		wg.Add(1)
		wf <- 1
		go getKline(stk, kltype, &wg, &wf)
	}
	wg.Wait()
	close(wf)
	waitDbjob(wgdb)
	close(outstks)
	wgr.Wait()
	log.Printf("%d stocks %s data updated.", rstks.Size(), strings.Join(kt2strs(kltype), ", "))
	if stks.Size() != rstks.Size() {
		same, skp := stks.Diff(rstks)
		if !same {
			log.Printf("Failed: %+v", skp)
		}
	}
	return
}

func waitDbjob(wgs []*sync.WaitGroup) {
	for _, ch := range chDbjob {
		close(ch)
	}
	for _, wg := range wgs {
		wg.Wait()
	}
}

func createDbJobQueues(kltype ...model.DBTab) (qmap map[model.DBTab]chan *dbJob) {
	qmap = make(map[model.DBTab]chan *dbJob)
	for _, t := range kltype {
		qmap[t] = make(chan *dbJob, conf.Args.DBQueueCapacity)
	}
	return
}

func saveQuotes(outstks chan *model.Stock) (wgs []*sync.WaitGroup) {
	snmap := new(sync.Map)
	total := len(chDbjob)
	lock := new(sync.RWMutex)
	for tab, ch := range chDbjob {
		wg := new(sync.WaitGroup)
		wgs = append(wgs, wg)
		wg.Add(1)
		go func(wg *sync.WaitGroup, ch chan *dbJob, outstks chan *model.Stock,
			snmap *sync.Map, lock *sync.RWMutex, tab model.DBTab) {
			defer wg.Done()
			for j := range ch {
				c := binsert(j.quotes, string(j.table), j.klid)
				if c == len(j.quotes) {
					lock.Lock()
					var cnt interface{}
					if cnt, _ = snmap.LoadOrStore(j.stock.Code, 0); cnt.(int) == total-1 {
						snmap.Delete(j.stock.Code)
						outstks <- j.stock
						log.Printf("%s all requested klines fetched", j.stock.Code)
					} else {
						snmap.Store(j.stock.Code, cnt.(int)+1)
					}
					lock.Unlock()
				}
			}
		}(wg, ch, outstks, snmap, lock, tab)
	}
	return
}

//KlinePostProcess manipulates kline data stored in database
//after all newly data are fetched from remote source.
func KlinePostProcess(stks *model.Stocks) (rstks *model.Stocks) {
	switch conf.Args.DataSource.Kline {
	case conf.WHT:
		rstks = whtPostProcessKline(stks)
	default:
		rstks = stks
	}
	return
}

//GetKlineDb get specified type of kline data from database.
func GetKlineDb(code string, tab model.DBTab, limit int, desc bool) (hist []*model.Quote) {
	if limit <= 0 {
		sql := fmt.Sprintf("select * from %s where code = ? order by klid", tab)
		if desc {
			sql += " desc"
		}
		_, e := dbmap.Select(&hist, sql, code)
		util.CheckErr(e, "failed to query "+string(tab)+" for "+code)
	} else {
		d := ""
		if desc {
			d = "desc"
		}
		sql := fmt.Sprintf("select * from (select * from %s where code = ? order by klid desc limit ?) t "+
			"order by t.klid %s", tab, d)
		_, e := dbmap.Select(&hist, sql, code, limit)
		util.CheckErr(e, "failed to query "+string(tab)+" for "+code)
	}
	return
}

//GetKlBtwnKlid fetches kline data between specified klids.
func GetKlBtwnKlid(code string, tab model.DBTab, sklid, eklid string, desc bool) (hist []*model.Quote) {
	var (
		k1cond, k2cond string
	)
	if sklid != "" {
		op := ">"
		if strings.HasPrefix(sklid, "[") {
			op += "="
			sklid = sklid[1:]
		}
		k1cond = fmt.Sprintf("and klid %s %s", op, sklid)
	}
	if eklid != "" {
		op := "<"
		if strings.HasSuffix(eklid, "]") {
			op += "="
			eklid = eklid[:len(eklid)-1]
		}
		k2cond = fmt.Sprintf("and klid %s %s", op, eklid)
	}
	d := ""
	if desc {
		d = "desc"
	}
	sql := fmt.Sprintf("select * from %s where code = ? %s %s order by klid %s", tab, k1cond, k2cond, d)
	_, e := dbmap.Select(&hist, sql, code)
	util.CheckErr(e, "failed to query "+string(tab)+" for "+code+", sql: "+sql)
	for _, q := range hist {
		q.Type = tab
	}
	return
}

//GetKlBtwn fetches kline data between dates.
func GetKlBtwn(code string, tab model.DBTab, dt1, dt2 string, desc bool) (hist []*model.Quote) {
	var (
		dt1cond, dt2cond string
	)
	if dt1 != "" {
		op := ">"
		if strings.HasPrefix(dt1, "[") {
			op += "="
			dt1 = dt1[1:]
		}
		dt1cond = fmt.Sprintf("and date %s '%s'", op, dt1)
	}
	if dt2 != "" {
		op := "<"
		if strings.HasSuffix(dt2, "]") {
			op += "="
			dt2 = dt2[:len(dt2)-1]
		}
		dt2cond = fmt.Sprintf("and date %s '%s'", op, dt2)
	}
	d := ""
	if desc {
		d = "desc"
	}
	sql := fmt.Sprintf("select * from %s where code = ? %s %s order by klid %s",
		tab, dt1cond, dt2cond, d)
	_, e := dbmap.Select(&hist, sql, code)
	util.CheckErr(e, "failed to query "+string(tab)+" for "+code)
	for _, q := range hist {
		q.Type = tab
	}
	return
}

//FixVarate fixes stock varate inaccurate issue caused by 0 close price introduced in reinstate process.
func FixVarate() {
	tabs := []model.DBTab{model.KLINE_DAY, model.KLINE_WEEK, model.KLINE_MONTH}
	for _, t := range tabs {
		qry := fmt.Sprintf(`select * from %v where close = 0 order by code, klid`, t)
		var qs []*model.Quote
		_, e := dbmap.Select(&qs, qry)
		if e != nil {
			if e == sql.ErrNoRows {
				logrus.Infof("%v has no 0 close price records.", t)
				continue
			} else {
				logrus.Panicf("failed to query %v for 0 close price records. %+v", t, e)
			}
		}
		if len(qs) == 0 {
			logrus.Infof("%v has no 0 close price records.", t)
			continue
		}
		qmap := make(map[string]*model.Quote)
		logrus.Infof("%v found %d 0 close price records", t, len(qs))
		for i, q := range qs {
			tbu := make([]*model.Quote, 0, 3)
			qry = fmt.Sprintf("select * from %v where code = ? and klid between ? and ? order by klid", t)
			_, e := dbmap.Select(&tbu, qry, q.Code, q.Klid-1, q.Klid+1)
			if e != nil {
				logrus.Panicf("failed to query %v for 0 close price records. %+v", t, e)
			}
			if len(tbu) == 1 {
				continue
			}
			for j := 1; j < len(tbu); j++ {
				k := fmt.Sprintf("%s_%d", tbu[j].Code, tbu[j].Klid)
				if _, ok := qmap[k]; ok {
					continue
				}
				pc := tbu[j-1].Close
				cc := tbu[j].Close
				if pc == 0 && cc == 0 {
					tbu[j].Varate.Float64 = 0
				} else if pc == 0 {
					tbu[j].Varate.Float64 = cc / .01 * 100.
				} else if cc == 0 {
					tbu[j].Varate.Float64 = (-0.01 - pc) / math.Abs(pc) * 100.
				} else {
					tbu[j].Varate.Float64 = (cc - pc) / math.Abs(pc) * 100.
				}
				qmap[k] = tbu[j]
			}
			prgs := float64(i+1) / float64(len(qs)) * 100.
			logrus.Infof("%d/%d\t%.2f%%\t%s %d %s varate recalculated",
				i+1, len(qs), prgs, q.Code, q.Klid, q.Date)
		}
		updateVarate(qmap, t)
	}
}

//CalVarate calculates variation rate based on previous value and current value.
// 0 previous value is adjusted by a bias of 0.01. Returns variation rate at a
// specified scale(e.g 100 as percentage value).
func CalVarate(prev, cur, scale float64) float64 {
	if prev == 0 && cur == 0 {
		return 0
	} else if prev == 0 {
		return cur / .01 * scale
	} else if cur == 0 {
		return (-0.01 - prev) / math.Abs(prev) * scale
	}
	return (cur - prev) / math.Abs(prev) * scale
}

func calLogReturnsFor(qmap map[model.DBTab][]*model.Quote) (e error) {
	for _, qs := range qmap {
		CalLogReturns(qs)
	}
	return nil
}

//CalLogReturns calculates log return for high, open, close, low, and volume
// variation rates, or regulated variation rates if available.
func CalLogReturns(qs []*model.Quote) {
	for i, q := range qs {
		vcl := q.VarateRgl.Float64
		vhg := q.VarateRglHigh.Float64
		vop := q.VarateRglOpen.Float64
		vlw := q.VarateRglLow.Float64
		if !q.VarateRgl.Valid {
			vcl = q.Varate.Float64
		}
		if !q.VarateRglHigh.Valid {
			vhg = q.VarateHigh.Float64
		}
		if !q.VarateRglOpen.Valid {
			vop = q.VarateOpen.Float64
		}
		if !q.VarateRglLow.Valid {
			vlw = q.VarateLow.Float64
		}
		q.Lr = sql.NullFloat64{Float64: math.Log(1. + vcl/100.), Valid: true}
		q.LrHigh = sql.NullFloat64{Float64: math.Log(1. + vhg/100.), Valid: true}
		q.LrOpen = sql.NullFloat64{Float64: math.Log(1. + vop/100.), Valid: true}
		q.LrLow = sql.NullFloat64{Float64: math.Log(1. + vlw/100.), Valid: true}

		bias := .01
		if q.Xrate.Valid {
			q.LrXr.Valid = true
			if i > 0 && qs[i-1].Xrate.Valid {
				q.LrXr.Float64 = logReturn(qs[i-1].Xrate.Float64, q.Xrate.Float64, bias)
			}
		}
		//calculates LR for MA
		if q.Ma5.Valid {
			q.LrMa5.Valid = true
			if i > 0 && qs[i-1].Ma5.Valid {
				q.LrMa5.Float64 = logReturn(qs[i-1].Ma5.Float64, q.Ma5.Float64, bias)
			}
		}
		if q.Ma10.Valid {
			q.LrMa10.Valid = true
			if i > 0 && qs[i-1].Ma10.Valid {
				q.LrMa10.Float64 = logReturn(qs[i-1].Ma10.Float64, q.Ma10.Float64, bias)
			}
		}
		if q.Ma20.Valid {
			q.LrMa20.Valid = true
			if i > 0 && qs[i-1].Ma20.Valid {
				q.LrMa20.Float64 = logReturn(qs[i-1].Ma20.Float64, q.Ma20.Float64, bias)
			}
		}
		if q.Ma30.Valid {
			q.LrMa30.Valid = true
			if i > 0 && qs[i-1].Ma30.Valid {
				q.LrMa30.Float64 = logReturn(qs[i-1].Ma30.Float64, q.Ma30.Float64, bias)
			}
		}
		if q.Ma60.Valid {
			q.LrMa60.Valid = true
			if i > 0 && qs[i-1].Ma60.Valid {
				q.LrMa60.Float64 = logReturn(qs[i-1].Ma60.Float64, q.Ma60.Float64, bias)
			}
		}
		if q.Ma120.Valid {
			q.LrMa120.Valid = true
			if i > 0 && qs[i-1].Ma120.Valid {
				q.LrMa120.Float64 = logReturn(qs[i-1].Ma120.Float64, q.Ma120.Float64, bias)
			}
		}
		if q.Ma200.Valid {
			q.LrMa200.Valid = true
			if i > 0 && qs[i-1].Ma200.Valid {
				q.LrMa200.Float64 = logReturn(qs[i-1].Ma200.Float64, q.Ma200.Float64, bias)
			}
		}
		if q.Ma250.Valid {
			q.LrMa250.Valid = true
			if i > 0 && qs[i-1].Ma250.Valid {
				q.LrMa250.Float64 = logReturn(qs[i-1].Ma250.Float64, q.Ma250.Float64, bias)
			}
		}
		bias = 10
		if q.Volume.Valid {
			q.LrVol.Valid = true
			if i > 0 && qs[i-1].Volume.Valid {
				q.LrVol.Float64 = logReturn(qs[i-1].Volume.Float64, q.Volume.Float64, bias)
			}
		}
		if q.Amount.Valid {
			q.LrAmt.Valid = true
			if i > 0 && qs[i-1].Amount.Valid {
				q.LrAmt.Float64 = logReturn(qs[i-1].Amount.Float64, q.Amount.Float64, bias)
			}
		}
		//calculates LR for vol MA
		if q.Vol5.Valid {
			q.LrVol5.Valid = true
			if i > 0 && qs[i-1].Vol5.Valid {
				q.LrVol5.Float64 = logReturn(qs[i-1].Vol5.Float64, q.Vol5.Float64, bias)
			}
		}
		if q.Vol10.Valid {
			q.LrVol10.Valid = true
			if i > 0 && qs[i-1].Vol10.Valid {
				q.LrVol10.Float64 = logReturn(qs[i-1].Vol10.Float64, q.Vol10.Float64, bias)
			}
		}
		if q.Vol20.Valid {
			q.LrVol20.Valid = true
			if i > 0 && qs[i-1].Vol20.Valid {
				q.LrVol20.Float64 = logReturn(qs[i-1].Vol20.Float64, q.Vol20.Float64, bias)
			}
		}
		if q.Vol30.Valid {
			q.LrVol30.Valid = true
			if i > 0 && qs[i-1].Vol30.Valid {
				q.LrVol30.Float64 = logReturn(qs[i-1].Vol30.Float64, q.Vol30.Float64, bias)
			}
		}
		if q.Vol60.Valid {
			q.LrVol60.Valid = true
			if i > 0 && qs[i-1].Vol60.Valid {
				q.LrVol60.Float64 = logReturn(qs[i-1].Vol60.Float64, q.Vol60.Float64, bias)
			}
		}
		if q.Vol120.Valid {
			q.LrVol120.Valid = true
			if i > 0 && qs[i-1].Vol120.Valid {
				q.LrVol120.Float64 = logReturn(qs[i-1].Vol120.Float64, q.Vol120.Float64, bias)
			}
		}
		if q.Vol200.Valid {
			q.LrVol200.Valid = true
			if i > 0 && qs[i-1].Vol200.Valid {
				q.LrVol200.Float64 = logReturn(qs[i-1].Vol200.Float64, q.Vol200.Float64, bias)
			}
		}
		if q.Vol250.Valid {
			q.LrVol250.Valid = true
			if i > 0 && qs[i-1].Vol250.Valid {
				q.LrVol250.Float64 = logReturn(qs[i-1].Vol250.Float64, q.Vol250.Float64, bias)
			}
		}
	}
}

//logReturn calculates log return based on previous value, current value and bias.
// bias is only used either previous or current value is not greater than 0.
func logReturn(prev, cur, bias float64) float64 {
	if bias <= 0 {
		log.Panicf("bias %f must be greater than 0.", bias)
	}
	if prev == 0 {
		return 0
		// if prev == 0 && cur == 0 {
		// 	return 0
		// } else if prev == 0 {
		// 	if cur > 0 {
		// 		return math.Log((cur + bias) / bias)
		// 	}
		// 	return math.Log(bias / (math.Abs(cur) + bias))
	} else if cur == 0 {
		if prev > 0 {
			return math.Log(bias / (prev + bias))
		}
		return math.Log((math.Abs(prev) + bias) / bias)
	} else if prev < 0 && cur < 0 {
		return math.Log(math.Abs(prev) / math.Abs(cur))
	} else if prev < 0 {
		return math.Log((cur + math.Abs(prev) + bias) / bias)
	} else if cur < 0 {
		return math.Log(bias / (prev + math.Abs(cur) + bias))
	}
	return math.Log(cur / prev)
}

func updateVarate(qmap map[string]*model.Quote, tab model.DBTab) {
	d, t := util.TimeStr()
	s := fmt.Sprintf("update %v set varate = ?, udate = ?, utime = ? where code = ? and klid = ?", tab)
	stm, e := dbmap.Prepare(s)
	defer stm.Close()
	if e != nil {
		logrus.Panicf("failed to prepare varate update statement: %+v", e)
	}
	for _, q := range qmap {
		_, e = stm.Exec(q.Varate, d, t, q.Code, q.Klid)
		if e != nil {
			logrus.Panicf("failed to update varate for %s %d %s: %+v", q.Code, q.Klid, q.Date, e)
		}
	}
}

// Reinstate adjusts price considering given XDXR data.
// if x is nil, return p as is.
func Reinstate(p float64, x *model.Xdxr) float64 {
	if x == nil {
		return p
	}
	d, sa, sc := 0., 0., 0.
	if x.Divi.Valid {
		d = x.Divi.Float64
	}
	if x.SharesAllot.Valid {
		sa = x.SharesAllot.Float64
	}
	if x.SharesCvt.Valid {
		sc = x.SharesCvt.Float64
	}
	return (p*10.0 - d) / (10.0 + sa + sc)
}

// ToOne merges qs into one quote, such as merging daily quotes into weekly quote or month quote
func ToOne(qs []*model.Quote, preClose float64, preKlid int) *model.Quote {
	oq := new(model.Quote)
	if len(qs) == 0 {
		return nil
	} else if len(qs) == 1 {
		return qs[0]
	} else {
		oq.Low = math.Inf(0)
		oq.High = math.Inf(-1)
		oq.Code = qs[0].Code
		oq.Klid = preKlid + 1
		oq.Open = qs[0].Open
		oq.Close = qs[len(qs)-1].Close
		oq.Date = qs[len(qs)-1].Date
		oq.Varate.Valid = true
		cc := oq.Close
		if preClose == 0 && cc == 0 {
			oq.Varate.Float64 = 0
		} else if preClose == 0 {
			oq.Varate.Float64 = cc / .01 * 100.
		} else if cc == 0 {
			oq.Varate.Float64 = (-0.01 - preClose) / math.Abs(preClose) * 100.
		} else {
			oq.Varate.Float64 = (cc - preClose) / math.Abs(preClose) * 100.
		}
		d, t := util.TimeStr()
		oq.Udate.Valid = true
		oq.Utime.Valid = true
		oq.Udate.String = d
		oq.Utime.String = t
		for _, q := range qs {
			if q.Low < oq.Low {
				oq.Low = q.Low
			}
			if q.High > oq.High {
				oq.High = q.High
			}
			if q.Volume.Valid {

			}
			if q.Volume.Valid {
				oq.Volume.Valid = true
				oq.Volume.Float64 += q.Volume.Float64
			}
			if q.Xrate.Valid {
				oq.Xrate.Valid = true
				oq.Xrate.Float64 += q.Xrate.Float64
			}
			if q.Amount.Valid {
				oq.Amount.Valid = true
				oq.Amount.Float64 += q.Amount.Float64
			}
		}
		// no handling of oq.Time yet
	}
	return oq
}

//convert slice of KLType to slice of string
func kt2strs(kltype []model.DBTab) (s []string) {
	s = make([]string, len(kltype))
	for i, e := range kltype {
		s[i] = string(e)
	}
	return
}

func getKline(stk *model.Stock, kltype []model.DBTab, wg *sync.WaitGroup, wf *chan int) {
	defer func() {
		wg.Done()
		<-*wf
	}()
	fetchRemoteKline(stk, kltype)
}

func fetchRemoteKline(stk *model.Stock, kltype []model.DBTab) (ok bool) {
	suc := false
	fail := false
	var kltnv []model.DBTab
	var qmap map[model.DBTab][]*model.Quote
	var lkmap map[model.DBTab]int
	//process validate request first
	for _, klt := range kltype {
		switch klt {
		case model.KLINE_DAY_VLD, model.KLINE_WEEK_VLD, model.KLINE_MONTH_VLD:
			panic("validation data is not supported yet.")
			switch conf.Args.DataSource.KlineValidateSource {
			case conf.TENCENT:
				// _, suc = klineTc(stk, klt, true)
			default:
				logrus.Warnf("not supported validate source: %s", conf.Args.DataSource.KlineValidateSource)
			}
			if !suc {
				fail = true
			}
		default:
			kltnv = append(kltnv, klt)
		}
	}
	if fail {
		return false
	}
	if len(kltnv) > 0 {
		src := conf.Args.DataSource.Kline
		if stk.Source != "" {
			src = stk.Source
		}
		switch src {
		case conf.WHT:
			qmap, lkmap, suc = getKlineWht(stk, kltnv, true)
		case conf.THS:
			qmap, lkmap, suc = getKlineThs(stk, kltnv)
		case conf.TENCENT:
			qmap, lkmap, suc = getKlineTc(stk, kltnv)
		}
	}
	if !suc {
		return false
	}
	for klt, quotes := range qmap {
		supplementMisc(quotes, klt, lkmap[klt])
	}
	// insert non-reinstated quotes first for regulated varate calculation
	for klt, quotes := range qmap {
		switch klt {
		case model.KLINE_DAY_NR, model.KLINE_WEEK_NR, model.KLINE_MONTH_NR:
			CalLogReturns(quotes)
			if lkmap[klt] != -1 {
				//skip the first record which is for varate calculation
				quotes = quotes[1:]
			}
			binsert(quotes, string(klt), lkmap[klt])
		}
	}
	if !isIndex(stk.Code) {
		e := calcVarateRgl(stk, qmap)
		if e != nil {
			logrus.Errorf("%s failed to calculate varate_rgl: %+v", stk.Code, e)
			return false
		}
	}
	for klt, quotes := range qmap {
		switch klt {
		case model.KLINE_DAY_NR, model.KLINE_WEEK_NR, model.KLINE_MONTH_NR:
			//already inserted, publish empty quote to db job channel
			chDbjob[klt] <- &dbJob{
				stock:  stk,
				quotes: nil,
				table:  klt,
				klid:   lkmap[klt],
			}
		default:
			CalLogReturns(quotes)
			if lkmap[klt] != -1 {
				//skip the first record which is for varate calculation
				quotes = quotes[1:]
				qmap[klt] = quotes
			}
			chDbjob[klt] <- &dbJob{
				stock:  stk,
				quotes: quotes,
				table:  klt,
				klid:   lkmap[klt],
			}
		}
	}
	return true
}

func getMinuteKlines(code string, tab model.DBTab) (klmin []*model.Quote, suc bool) {
	RETRIES := 5
	for rt := 0; rt < RETRIES; rt++ {
		kls, suc, retry := tryMinuteKlines(code, tab)
		if suc {
			return kls, true
		}
		if retry && rt+1 < RETRIES {
			log.Printf("%s retrying to get %s [%d]", code, tab, rt+1)
			continue
		} else {
			log.Printf("%s failed getting %s", code, tab)
			return klmin, false
		}
	}
	return klmin, false
}

func tryMinuteKlines(code string, tab model.DBTab) (klmin []*model.Quote, suc, retry bool) {
	//TODO implement minute klines
	//urlt := `https://xueqiu.com/stock/forchartk/stocklist.json?symbol=%s&period=60m&type=before`
	panic("implement me ")
}

func binsert(quotes []*model.Quote, table string, lklid int) (c int) {
	if len(quotes) == 0 {
		return 0
	}
	numFields := 59
	retry := conf.Args.DeadlockRetry
	rt := 0
	lklid++
	code := ""
	holders := make([]string, numFields)
	for i := range holders {
		holders[i] = "?"
	}
	holderString := fmt.Sprintf("(%s)", strings.Join(holders, ","))
	var e error
	valueStrings := make([]string, 0, len(quotes))
	valueArgs := make([]interface{}, 0, len(quotes)*numFields)
	for _, q := range quotes {
		valueStrings = append(valueStrings, holderString)
		valueArgs = append(valueArgs, q.Code)
		valueArgs = append(valueArgs, q.Date)
		valueArgs = append(valueArgs, q.Klid)
		valueArgs = append(valueArgs, q.Open)
		valueArgs = append(valueArgs, q.High)
		valueArgs = append(valueArgs, q.Close)
		valueArgs = append(valueArgs, q.Low)
		valueArgs = append(valueArgs, q.Volume)
		valueArgs = append(valueArgs, q.Amount)
		valueArgs = append(valueArgs, q.LrAmt)
		valueArgs = append(valueArgs, q.Xrate)
		valueArgs = append(valueArgs, q.LrXr)
		valueArgs = append(valueArgs, q.Varate)
		valueArgs = append(valueArgs, q.VarateHigh)
		valueArgs = append(valueArgs, q.VarateOpen)
		valueArgs = append(valueArgs, q.VarateLow)
		valueArgs = append(valueArgs, q.VarateRgl)
		valueArgs = append(valueArgs, q.VarateRglHigh)
		valueArgs = append(valueArgs, q.VarateRglOpen)
		valueArgs = append(valueArgs, q.VarateRglLow)
		valueArgs = append(valueArgs, q.Lr)
		valueArgs = append(valueArgs, q.LrHigh)
		valueArgs = append(valueArgs, q.LrOpen)
		valueArgs = append(valueArgs, q.LrLow)
		valueArgs = append(valueArgs, q.LrVol)
		valueArgs = append(valueArgs, q.Ma5)
		valueArgs = append(valueArgs, q.Ma10)
		valueArgs = append(valueArgs, q.Ma20)
		valueArgs = append(valueArgs, q.Ma30)
		valueArgs = append(valueArgs, q.Ma60)
		valueArgs = append(valueArgs, q.Ma120)
		valueArgs = append(valueArgs, q.Ma200)
		valueArgs = append(valueArgs, q.Ma250)
		valueArgs = append(valueArgs, q.LrMa5)
		valueArgs = append(valueArgs, q.LrMa10)
		valueArgs = append(valueArgs, q.LrMa20)
		valueArgs = append(valueArgs, q.LrMa30)
		valueArgs = append(valueArgs, q.LrMa60)
		valueArgs = append(valueArgs, q.LrMa120)
		valueArgs = append(valueArgs, q.LrMa200)
		valueArgs = append(valueArgs, q.LrMa250)
		valueArgs = append(valueArgs, q.Vol5)
		valueArgs = append(valueArgs, q.Vol10)
		valueArgs = append(valueArgs, q.Vol20)
		valueArgs = append(valueArgs, q.Vol30)
		valueArgs = append(valueArgs, q.Vol60)
		valueArgs = append(valueArgs, q.Vol120)
		valueArgs = append(valueArgs, q.Vol200)
		valueArgs = append(valueArgs, q.Vol250)
		valueArgs = append(valueArgs, q.LrVol5)
		valueArgs = append(valueArgs, q.LrVol10)
		valueArgs = append(valueArgs, q.LrVol20)
		valueArgs = append(valueArgs, q.LrVol30)
		valueArgs = append(valueArgs, q.LrVol60)
		valueArgs = append(valueArgs, q.LrVol120)
		valueArgs = append(valueArgs, q.LrVol200)
		valueArgs = append(valueArgs, q.LrVol250)
		valueArgs = append(valueArgs, q.Udate)
		valueArgs = append(valueArgs, q.Utime)
		code = q.Code
	}
	// delete stale records first
	for ; rt < retry; rt++ {
		_, e = dbmap.Exec(fmt.Sprintf("delete from %s where code = ? and klid > ?", table), code, lklid)
		if e != nil {
			fmt.Println(e)
			if strings.Contains(e.Error(), "Deadlock") {
				continue
			} else {
				log.Panicf("%s failed to bulk insert %s: %+v", code, table, e)
			}
		}
		break
	}
	if rt >= retry {
		log.Panicf("%s failed to delete %s where klid > %d", code, table, lklid)
	}
	rt = 0
	stmt := fmt.Sprintf("INSERT INTO %s (code,date,klid,open,high,close,low,"+
		"volume,amount,lr_amt,xrate,lr_xr,varate,varate_h,varate_o,varate_l,varate_rgl,varate_rgl_h,varate_rgl_o,"+
		"varate_rgl_l,lr,lr_h,lr_o,lr_l,lr_vol,ma5,ma10,ma20,ma30,ma60,ma120,ma200,ma250,"+
		"lr_ma5,lr_ma10,lr_ma20,lr_ma30,lr_ma60,lr_ma120,lr_ma200,lr_ma250,"+
		"vol5,vol10,vol20,vol30,vol60,vol120,vol200,vol250,"+
		"lr_vol5,lr_vol10,lr_vol20,lr_vol30,lr_vol60,lr_vol120,lr_vol200,lr_vol250,"+
		"udate,utime) "+
		"VALUES %s on duplicate key update date=values(date),"+
		"open=values(open),high=values(high),close=values(close),low=values(low),"+
		"volume=values(volume),amount=values(amount),lr_amt=values(lr_amt),xrate=values(xrate),"+
		"lr_xr=values(lr_xr),varate=values(varate),"+
		"varate_h=values(varate_h),varate_o=values(varate_o),varate_l=values(varate_l),"+
		"varate_rgl=values(varate_rgl),varate_rgl_h=values(varate_rgl_h),"+
		"varate_rgl_o=values(varate_rgl_o),varate_rgl_l=values(varate_rgl_l),"+
		"lr=values(lr),lr_h=values(lr_h),lr_o=values(lr_o),lr_l=values(lr_l),"+
		"lr_vol=values(lr_vol),ma5=values(ma5),ma10=values(ma10),ma20=values(ma20),"+
		"ma30=values(ma30),ma60=values(ma60),ma120=values(ma120),ma200=values(ma200),"+
		"ma250=values(ma250),lr_ma5=values(lr_ma5),lr_ma10=values(lr_ma10),lr_ma20=values(lr_ma20),"+
		"lr_ma30=values(lr_ma30),lr_ma60=values(lr_ma60),lr_ma120=values(lr_ma120),"+
		"lr_ma200=values(lr_ma200),lr_ma250=values(lr_ma250),"+
		"vol5=values(vol5),vol10=values(vol10),vol20=values(vol20),"+
		"vol30=values(vol30),vol60=values(vol60),vol120=values(vol120),vol200=values(vol200),"+
		"vol250=values(vol250),lr_vol5=values(lr_vol5),lr_vol10=values(lr_vol10),lr_vol20=values(lr_vol20),"+
		"lr_vol30=values(lr_vol30),lr_vol60=values(lr_vol60),lr_vol120=values(lr_vol120),"+
		"lr_vol200=values(lr_vol200),lr_vol250=values(lr_vol250),"+
		"udate=values(udate),utime=values(utime)",
		table, strings.Join(valueStrings, ","))
	for ; rt < retry; rt++ {
		_, e = dbmap.Exec(stmt, valueArgs...)
		if e != nil {
			fmt.Println(e)
			if strings.Contains(e.Error(), "Deadlock") {
				continue
			} else {
				log.Panicf("%s failed to bulk insert %s: %+v", code, table, e)
			}
		}
		c = len(quotes)
		break
	}
	if rt >= retry {
		log.Panicf("%s failed to bulk insert %s: %+v", code, table, e)
	}
	return
}

//validateKline validates quotes against corresponding validation table, checking dates between two samples.
func validateKline(stk *model.Stock, t model.DBTab, quotes []*model.Quote, lklid int) bool {
	var vtab model.DBTab
	switch t {
	case model.KLINE_DAY, model.KLINE_DAY_B, model.KLINE_DAY_NR:
		vtab = model.KLINE_DAY_VLD
	case model.KLINE_WEEK, model.KLINE_WEEK_B, model.KLINE_WEEK_NR:
		vtab = model.KLINE_WEEK_VLD
	case model.KLINE_MONTH, model.KLINE_MONTH_B, model.KLINE_MONTH_NR:
		vtab = model.KLINE_MONTH_VLD
	default:
		logrus.Warnf("validation not supported for %v", t)
		return true
	}
	ex := make([]string, 0, 16)
	vquotes := GetKlBtwnKlid(stk.Code, vtab, "["+string(lklid), "", false)
	for i := 0; i < len(vquotes); i++ {
		vq := vquotes[i]
		if i >= len(quotes) {
			ex = append(ex, vq.Date)
		} else {
			q := quotes[i]
			if vq.Date != q.Date {
				ex = append(ex, vq.Date)
			}
		}
	}
	if len(ex) > 0 {
		logrus.Warnf("%s %v kline validation exception: %+v", stk.Code, t, ex)
	}
	return len(ex) == 0
}

//Assign KLID, calculate Varate, add update datetime
func supplementMisc(klines []*model.Quote, kltype model.DBTab, start int) {
	if len(klines) == 0 {
		return
	}
	q := klines[0]
	d, t := util.TimeStr()
	scale := 100.
	preclose, prehigh, preopen, prelow := math.NaN(), math.NaN(), math.NaN(), math.NaN()
	mas := []int{5, 10, 20, 30, 60, 120, 200, 250}
	maSrc := make([]*model.Quote, len(klines))
	for i := range maSrc {
		maSrc[i] = klines[len(maSrc)-1-i]
	}
	//expand maSrc for ma calculation
	sklid := strconv.Itoa(start + 1 - mas[len(mas)-1])
	eklid := strconv.Itoa(start + 1)
	//maSrc is in descending order, contrary to klines
	maSrc = append(maSrc, GetKlBtwnKlid(q.Code, kltype, sklid, eklid, true)...)
	for i := 0; i < len(klines); i++ {
		start++
		k := klines[i]
		k.Type = kltype
		k.Klid = start
		k.Udate.Valid = true
		k.Utime.Valid = true
		k.Udate.String = d
		k.Utime.String = t
		k.Varate.Valid = true
		k.VarateHigh.Valid = true
		k.VarateOpen.Valid = true
		k.VarateLow.Valid = true
		if math.IsNaN(preclose) {
			k.Varate.Float64 = 0
			k.VarateHigh.Float64 = 0
			k.VarateOpen.Float64 = 0
			k.VarateLow.Float64 = 0
		} else {
			k.Varate.Float64 = CalVarate(preclose, k.Close, scale)
			k.VarateHigh.Float64 = CalVarate(prehigh, k.High, scale)
			k.VarateOpen.Float64 = CalVarate(preopen, k.Open, scale)
			k.VarateLow.Float64 = CalVarate(prelow, k.Low, scale)
		}
		preclose = k.Close
		prehigh = k.High
		preopen = k.Open
		prelow = k.Low
		//calculate various ma if nil
		start := len(klines) - 1 - i
		for _, m := range mas {
			ma := 0.
			mavol := 0.
			if start+m-1 < len(maSrc) {
				for j := 0; j < m; j++ {
					idx := start + j
					ma += maSrc[idx].Close
					mavol += maSrc[idx].Volume.Float64
				}
				ma /= float64(m)
				mavol /= float64(m)
			}
			switch m {
			case 5:
				if !k.Ma5.Valid {
					k.Ma5.Valid = true
					k.Ma5.Float64 = ma
				}
				if !k.Vol5.Valid {
					k.Vol5.Valid = true
					k.Vol5.Float64 = mavol
				}
			case 10:
				if !k.Ma10.Valid {
					k.Ma10.Valid = true
					k.Ma10.Float64 = ma
				}
				if !k.Vol10.Valid {
					k.Vol10.Valid = true
					k.Vol10.Float64 = mavol
				}
			case 20:
				if !k.Ma20.Valid {
					k.Ma20.Valid = true
					k.Ma20.Float64 = ma
				}
				if !k.Vol20.Valid {
					k.Vol20.Valid = true
					k.Vol20.Float64 = mavol
				}
			case 30:
				if !k.Ma30.Valid {
					k.Ma30.Valid = true
					k.Ma30.Float64 = ma
				}
				if !k.Vol30.Valid {
					k.Vol30.Valid = true
					k.Vol30.Float64 = mavol
				}
			case 60:
				if !k.Ma60.Valid {
					k.Ma60.Valid = true
					k.Ma60.Float64 = ma
				}
				if !k.Vol60.Valid {
					k.Vol60.Valid = true
					k.Vol60.Float64 = mavol
				}
			case 120:
				if !k.Ma120.Valid {
					k.Ma120.Valid = true
					k.Ma120.Float64 = ma
				}
				if !k.Vol120.Valid {
					k.Vol120.Valid = true
					k.Vol120.Float64 = mavol
				}
			case 200:
				if !k.Ma200.Valid {
					k.Ma200.Valid = true
					k.Ma200.Float64 = ma
				}
				if !k.Vol200.Valid {
					k.Vol200.Valid = true
					k.Vol200.Float64 = mavol
				}
			case 250:
				if !k.Ma250.Valid {
					k.Ma250.Valid = true
					k.Ma250.Float64 = ma
				}
				if !k.Vol250.Valid {
					k.Vol250.Valid = true
					k.Vol250.Float64 = mavol
				}
			default:
				log.Panicf("unsupported MA value: %d", m)
			}
		}
	}
}

func getLatestKl(code string, klt model.DBTab, offset int) (q *model.Quote) {
	e := dbmap.SelectOne(&q, fmt.Sprintf("select code, date, klid from %s where code = ? order by klid desc "+
		"limit 1 offset ?", klt), code, offset)
	if e != nil {
		if "sql: no rows in result set" == e.Error() {
			return nil
		}
		log.Panicln("failed to run sql", e)
	}
	return
}

func calcVarateRgl(stk *model.Stock, qmap map[model.DBTab][]*model.Quote) (e error) {
	for t, qs := range qmap {
		var retTgqs []*model.Quote
		switch t {
		case model.KLINE_DAY:
			retTgqs, e = inferVarateRgl(stk, model.KLINE_DAY_NR, qmap[model.KLINE_DAY_NR], qs)
		case model.KLINE_WEEK:
			retTgqs, e = inferVarateRgl(stk, model.KLINE_WEEK_NR, qmap[model.KLINE_WEEK_NR], qs)
		case model.KLINE_MONTH:
			retTgqs, e = inferVarateRgl(stk, model.KLINE_MONTH_NR, qmap[model.KLINE_MONTH_NR], qs)
		default:
			//skip the rest types of kline
		}
		if e != nil {
			log.Println(e)
			return e
		}
		if retTgqs != nil {
			qmap[t] = retTgqs
		}
	}
	return nil
}

//matchSlice assumes len(nrqs) >= len(tgqs) in normal cases, takes care of missing data in-between,
// trying best to make sure len(retNrqs) == len(retTgqs)
func matchSlice(nrqs, tgqs []*model.Quote) (retNrqs, retTgqs []*model.Quote, err error) {
	if len(nrqs) < len(tgqs) && !conf.Args.DataSource.DropInconsistent {
		return retNrqs, retTgqs, fmt.Errorf("len(nrqs)=%d, len(tgqs)=%d, missing data in nrqs", len(nrqs), len(tgqs))
	}
	s := 0
	r := false
	for _, q := range tgqs {
		f := false
		for j := s; j < len(nrqs); j++ {
			nq := nrqs[j]
			if nq.Date > q.Date {
				break
			} else if nq.Date == q.Date && nq.Klid == q.Klid {
				retNrqs = append(retNrqs, nq)
				retTgqs = append(retTgqs, q)
				s = j + 1
				r = true
				f = true
				break
			} else if r {
				break
			}
		}
		if r && !f {
			break
		}
	}
	if conf.Args.DataSource.DropInconsistent {
		if len(retTgqs) != len(tgqs) {
			var d int64
			var e error
			tab := nrqs[0].Type
			code := nrqs[0].Code
			date := tgqs[0].Date
			if len(retTgqs) != 0 {
				date = retTgqs[len(retTgqs)-1].Date
			}
			d, e = deleteKlineFromDate(tab, code, date)
			if e != nil {
				logrus.Warnf("failed to delete kline for %s %v from date %s: %+v", code, tab, date, e)
				return retNrqs, retTgqs, e
			}
			if d != 0 {
				logrus.Warnf("%s inconsistency found in %v. dropping %d, from date %s", code, tab, d, date)
			}
		}
	} else {
		if len(retTgqs) != len(tgqs) || len(retTgqs) == 0 {
			return retNrqs, retTgqs, fmt.Errorf("data inconsistent. nrqs:%+v\ntgqs:%+v", nrqs, tgqs)
		}
		lastTg := retTgqs[len(retTgqs)-1]
		lastNr := nrqs[len(nrqs)-1]
		if lastTg.Date != lastNr.Date || lastTg.Klid != lastNr.Klid {
			return retNrqs, retTgqs, fmt.Errorf("data inconsistent. nrqs:%+v\ntgqs:%+v", nrqs, tgqs)
		}
	}
	return
}

func deleteKlineFromDate(kltype model.DBTab, code, date string) (d int64, e error) {
	sql := fmt.Sprintf("delete from %v where code = ? and date >= ?", kltype)
	retry := 10
	tried := 0
	for ; tried < retry; tried++ {
		r, e := dbmap.Exec(sql, code, date)
		if e != nil {
			logrus.Warnf("%s failed to delete %v from %s, database error:%+v", code, kltype, date, e)
			if strings.Contains(e.Error(), "Deadlock") {
				time.Sleep(time.Millisecond * time.Duration(100+rand.Intn(900)))
				continue
			} else {
				return d, errors.WithStack(e)
			}
		}
		d, e = r.RowsAffected()
		if e != nil {
			return d, errors.WithStack(e)
		}
		break
	}
	return
}

func inferVarateRgl(stk *model.Stock, tab model.DBTab, nrqs, tgqs []*model.Quote) (
	retTgqs []*model.Quote, e error) {
	var retNrqs []*model.Quote
	retTgqs = make([]*model.Quote, 0)
	if tgqs == nil || len(tgqs) == 0 {
		return retTgqs, fmt.Errorf("%s unable to infer varate_rgl from %v. please provide valid target quotes parameter",
			stk.Code, tab)
	}
	sDate, eDate := tgqs[0].Date, tgqs[len(tgqs)-1].Date
	if nrqs == nil || len(nrqs) < len(tgqs) {
		//load non-reinstated quotes from db
		nrqs = GetKlBtwn(stk.Code, tab, "["+sDate, eDate+"]", false)
	}
	if len(nrqs) == 0 {
		logrus.Warnf("%s %v data not available, skipping varate_rgl calculation", stk.Code, tab)
		return nil, nil
	}
	if !conf.Args.DataSource.DropInconsistent {
		if len(nrqs) < len(tgqs) {
			return retTgqs, fmt.Errorf("%s unable to infer varate rgl from %v. len(nrqs)=%d, len(tgqs)=%d",
				stk.Code, tab, len(nrqs), len(tgqs))
		}
	}
	retNrqs, retTgqs, e = matchSlice(nrqs, tgqs)
	if e != nil {
		return retTgqs, fmt.Errorf("%s failed to infer varate_rgl from %v: %+v", stk.Code, tab, e)
	}
	if len(retNrqs) == 0 || len(retTgqs) == 0 {
		return retTgqs, nil
	}
	//reset start-date and end-date
	sDate = retTgqs[0].Date
	eDate = retTgqs[len(retTgqs)-1].Date
	xemap, e := XdxrDateBetween(stk.Code, sDate, eDate)
	if e != nil {
		return retTgqs, fmt.Errorf("%s unable to infer varate_rgl from %v: %+v", stk.Code, tab, e)
	}
	return retTgqs, transferVarateRgl(stk.Code, tab, retNrqs, retTgqs, xemap)
}

func transferVarateRgl(code string, tab model.DBTab, nrqs, tgqs []*model.Quote,
	xemap map[string]*model.Xdxr) (e error) {
	for i := 0; i < len(tgqs); i++ {
		nrq := nrqs[i]
		tgq := tgqs[i]
		if nrq.Code != tgq.Code || nrq.Date != tgq.Date || nrq.Klid != tgq.Klid {
			return fmt.Errorf("%s unable to infer varate rgl from %v. unmatched nrq & tgq at %d: %+v : %+v",
				code, tab, i, nrq, tgq)
		}
		tvar := nrq.Varate.Float64
		tvarh := nrq.VarateHigh.Float64
		tvaro := nrq.VarateOpen.Float64
		tvarl := nrq.VarateLow.Float64
		// first element is assumed to be dropped, so its values are irrelevant
		if len(xemap) > 0 && i > 0 {
			xe := MergeXdxrBetween(tgqs[i-1].Date, tgq.Date, xemap)
			if xe != nil {
				// adjust fore-day price for regulated varate calculation
				pcl := Reinstate(nrqs[i-1].Close, xe)
				phg := Reinstate(nrqs[i-1].High, xe)
				pop := Reinstate(nrqs[i-1].Open, xe)
				plw := Reinstate(nrqs[i-1].Low, xe)
				tvar = (nrq.Close - pcl) / pcl * 100.
				tvarh = (nrq.High - phg) / phg * 100.
				tvaro = (nrq.Open - pop) / pop * 100.
				tvarl = (nrq.Low - plw) / plw * 100.
			}
		}
		tgq.VarateRgl.Valid = true
		tgq.VarateRglOpen.Valid = true
		tgq.VarateRglHigh.Valid = true
		tgq.VarateRglLow.Valid = true
		tgq.VarateRgl.Float64 = tvar
		tgq.VarateRglOpen.Float64 = tvaro
		tgq.VarateRglHigh.Float64 = tvarh
		tgq.VarateRglLow.Float64 = tvarl
	}
	return nil
}

//MergeXdxrBetween merges financial values of xdxr events between specified start date(excluding) and end date(including).
func MergeXdxrBetween(start, end string, xemap map[string]*model.Xdxr) (rx *model.Xdxr) {
	if xemap == nil {
		return
	}
	for dt, x := range xemap {
		// loop through the map in case multiple xdxr events happen within the same period
		if dt <= start || dt > end {
			continue
		}
		// merge xdxr event data
		if rx == nil {
			rx = &model.Xdxr{
				Code: x.Code,
				Name: x.Name,
				Idx:  x.Idx,
			}
		}
		if x.Divi.Valid {
			rx.Divi.Valid = true
			rx.Divi.Float64 += x.Divi.Float64
		}
		if x.SharesAllot.Valid {
			rx.SharesAllot.Valid = true
			rx.SharesAllot.Float64 += x.SharesAllot.Float64
		}
		if x.SharesCvt.Valid {
			rx.SharesCvt.Valid = true
			rx.SharesCvt.Float64 += x.SharesCvt.Float64
		}
	}
	return
}

func mergeXdxr(xemap map[string]*model.Xdxr, date string, tab model.DBTab) (xe *model.Xdxr, in bool, e error) {
	for dt, x := range xemap {
		switch tab {
		case model.KLINE_WEEK_NR:
			in, e = util.SameWeek(dt, date, "")
		case model.KLINE_MONTH_NR:
			in = dt[:8] == date[:8]
		}
		if e != nil {
			return xe, false, e
		}
		if in {
			// in case multiple xdxr events happen within the same period
			if xe == nil {
				xe = x
			} else {
				if x.Divi.Valid {
					xe.Divi.Valid = true
					xe.Divi.Float64 += x.Divi.Float64
				}
				if x.SharesAllot.Valid {
					xe.SharesAllot.Valid = true
					xe.SharesAllot.Float64 += x.SharesAllot.Float64
				}
				if x.SharesCvt.Valid {
					xe.SharesCvt.Valid = true
					xe.SharesCvt.Float64 += x.SharesCvt.Float64
				}
			}
		}
	}
	return xe, in, e
}
