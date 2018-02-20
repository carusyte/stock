package getd

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"github.com/sirupsen/logrus"
)

//GetKlines Get various types of kline data for the given stocks. Returns the stocks that have been successfully processed.
func GetKlines(stks *model.Stocks, kltype ...model.DBTab) (rstks *model.Stocks) {
	//TODO find a way to get minute level klines
	defer Cleanup()
	log.Printf("begin to fetch kline data: %+v", kltype)
	var wg sync.WaitGroup
	wf := make(chan int, conf.Args.ChromeDP.PoolSize)
	outstks := make(chan *model.Stock, JOB_CAPACITY)
	rstks = new(model.Stocks)
	wgr := collect(rstks, outstks)
	for _, stk := range stks.List {
		wg.Add(1)
		wf <- 1
		go getKline(stk, kltype, &wg, &wf, outstks)
	}
	wg.Wait()
	close(wf)
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

func getKline(stk *model.Stock, kltype []model.DBTab, wg *sync.WaitGroup, wf *chan int, outstks chan *model.Stock) {
	defer func() {
		wg.Done()
		<-*wf
	}()
	_, suc := getKlineThs(stk, kltype)
	if suc {
		outstks <- stk
	}
	// xdxr := latestUFRXdxr(stk.Code)
	// suc := false
	// for _, t := range kltype {
	// 	switch t {
	// 	case model.KLINE_60M:
	// 		_, suc = getMinuteKlines(stk.Code, t)
	// 	case model.KLINE_DAY, model.KLINE_WEEK, model.KLINE_MONTH:
	// 		_, suc = getKlineCytp(stk, t, xdxr == nil)
	// 	case model.KLINE_DAY_NR:
	// 		_, suc = getKlineCytp(stk, t, true)
	// 	default:
	// 		log.Panicf("unhandled kltype: %s", t)
	// 	}
	// 	if !suc {
	// 		break
	// 	} else {
	// 		logrus.Debugf("%s %+v fetched", stk.Code, t)
	// 	}
	// }
	// if suc {
	// 	outstks <- stk
	// }
}

func getMinuteKlines(code string, tab model.DBTab) (klmin []*model.Quote, suc bool) {
	RETRIES := 5
	for rt := 0; rt < RETRIES; rt++ {
		kls, suc, retry := tryMinuteKlines(code, tab)
		if suc {
			return kls, true
		} else {
			if retry && rt+1 < RETRIES {
				log.Printf("%s retrying to get %s [%d]", code, tab, rt+1)
				continue
			} else {
				log.Printf("%s failed getting %s", code, tab)
				return klmin, false
			}
		}
	}
	return klmin, false
}

func tryMinuteKlines(code string, tab model.DBTab) (klmin []*model.Quote, suc, retry bool) {
	//TODO implement minute klines
	//urlt := `https://xueqiu.com/stock/forchartk/stocklist.json?symbol=%s&period=60m&type=before`
	panic("implement me ")
}

func getKlineCytp(stk *model.Stock, klt model.DBTab, incr bool) (kldy []*model.Quote, suc bool) {
	switch conf.Args.Datasource.Kline {
	case conf.THS:
		return klineThs(stk, klt, incr)
	case conf.TENCENT:
		return klineTc(stk, klt, incr)
	default:
		log.Panicf("unrecognized datasource: %+v", conf.Args.Datasource.Kline)
	}
	return
}

func getLongKlines(stk *model.Stock, klt model.DBTab, incr bool) (quotes []*model.Quote, suc bool) {
	switch conf.Args.Datasource.Kline {
	case conf.THS:
		return klineThs(stk, klt, incr)
	case conf.TENCENT:
		return klineTc(stk, klt, incr)
	default:
		log.Panicf("unrecognized datasource: %+v", conf.Args.Datasource.Kline)
	}
	return
}

func binsert(quotes []*model.Quote, table string, lklid int) (c int) {
	if len(quotes) == 0 {
		return 0
	}
	retry := 10
	rt := 0
	lklid++
	code := ""
	var e error
	for ; rt < retry; rt++ {
		valueStrings := make([]string, 0, len(quotes))
		valueArgs := make([]interface{}, 0, len(quotes)*25)
		var code string
		for _, q := range quotes {
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "+
				"?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, q.Code)
			valueArgs = append(valueArgs, q.Date)
			valueArgs = append(valueArgs, q.Klid)
			valueArgs = append(valueArgs, q.Open)
			valueArgs = append(valueArgs, q.High)
			valueArgs = append(valueArgs, q.Close)
			valueArgs = append(valueArgs, q.Low)
			valueArgs = append(valueArgs, q.Volume)
			valueArgs = append(valueArgs, q.Amount)
			valueArgs = append(valueArgs, q.Xrate)
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
			valueArgs = append(valueArgs, q.Udate)
			valueArgs = append(valueArgs, q.Utime)
			code = q.Code
		}

		// delete stale records first
		_, e = dbmap.Exec(fmt.Sprintf("delete from %s where code = ? and klid > ?", table), code, lklid)
		if e != nil {
			log.Printf("%s failed to delete %s where klid > %d", code, table, lklid)
			panic(e)
		}
		//TODO adapt new columns
		stmt := fmt.Sprintf("INSERT INTO %s (code,date,klid,open,high,close,low,"+
			"volume,amount,xrate,varate,varate_h,varate_o,varate_l,varate_rgl,varate_rgl_h,varate_rgl_o,"+
			"varate_rgl_l,lr,lr_h,lr_o,lr_l,lr_vol,udate,utime) "+
			"VALUES %s on duplicate key update date=values(date),"+
			"open=values(open),high=values(high),close=values(close),low=values(low),"+
			"volume=values(volume),amount=values(amount),xrate=values(xrate),varate=values(varate),"+
			"varate_h=values(varate_h),varate_o=values(varate_o),varate_l=values(varate_l),"+
			"varate_rgl=values(varate_rgl),varate_rgl_h=values(varate_rgl_h),"+
			"varate_rgl_o=values(varate_rgl_o),varate_rgl_l=values(varate_rgl_l),"+
			"lr=values(lr),lr_h=values(lr_h),lr_o=values(lr_o),lr_l=values(lr_l),"+
			"lr_vol=values(lr_vol),udate=values(udate),utime=values(utime)",
			table, strings.Join(valueStrings, ","))
		// log.Printf("statememt:\n%+v\nargs:\n%+v", stmt, valueArgs)
		_, e = dbmap.Exec(stmt, valueArgs...)
		if e != nil {
			fmt.Println(e)
			if strings.Contains(e.Error(), "Deadlock") {
				time.Sleep(time.Millisecond * time.Duration(100+rand.Intn(900)))
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

//TODO add function to update regulated varate in non-reinstated kline tables
