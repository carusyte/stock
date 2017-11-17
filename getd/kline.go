package getd

import (
	"fmt"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"log"
	"strings"
	"sync"
	"math"
	"github.com/carusyte/stock/conf"
	"github.com/sirupsen/logrus"
)

//Get various types of kline data for the given stocks. Returns the stocks that have been successfully processed.
func GetKlines(stks *model.Stocks, kltype ... model.DBTab) (rstks *model.Stocks) {
	//TODO find a way to get minute level klines
	defer cleanup()
	log.Printf("begin to fetch kline data: %+v", kltype)
	var wg sync.WaitGroup
	wf := make(chan int, MAX_CONCURRENCY)
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

func cleanup() {
	switch conf.Args.Datasource.Kline {
	case conf.THS:
		cleanupTHS()
	}
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
		denom := preClose
		if preClose == 0 {
			denom = .01
		}
		oq.Varate.Float64 = (oq.Close - preClose) / math.Abs(denom)
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
	xdxr := latestUFRXdxr(stk.Code)
	suc := false
	for _, t := range kltype {
		switch t {
		case model.KLINE_60M:
			_, suc = getMinuteKlines(stk.Code, t)
		case model.KLINE_DAY, model.KLINE_WEEK, model.KLINE_MONTH:
			_, suc = getKlineCytp(stk, t, xdxr == nil)
		case model.KLINE_DAY_NR:
			_, suc = getKlineCytp(stk, t, true)
		default:
			log.Panicf("unhandled kltype: %s", t)
		}
		if !suc {
			break
		} else {
			logrus.Debugf("%s %+v fetched", stk.Code, t)
		}
	}
	if suc {
		outstks <- stk
	}
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
	if len(quotes) > 0 {
		valueStrings := make([]string, 0, len(quotes))
		valueArgs := make([]interface{}, 0, len(quotes)*13)
		var code string
		for _, q := range quotes {
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, round(?,3), ?, ?)")
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
			valueArgs = append(valueArgs, q.Udate)
			valueArgs = append(valueArgs, q.Utime)
			code = q.Code
		}

		// delete stale records first
		lklid++
		_, e := dbmap.Exec(fmt.Sprintf("delete from %s where code = ? and klid > ?", table), code, lklid)
		if e != nil {
			log.Printf("%s failed to delete %s where klid > %d", code, table, lklid)
			panic(code)
		}

		stmt := fmt.Sprintf("INSERT INTO %s (code,date,klid,open,high,close,low,"+
			"volume,amount,xrate,varate,udate,utime) VALUES %s on duplicate key update date=values(date),"+
			"open=values(open),high=values(high),close=values(close),low=values(low),"+
			"volume=values(volume),amount=values(amount),xrate=values(xrate),varate=values(varate),udate=values"+
			"(udate),utime=values(utime)",
			table, strings.Join(valueStrings, ","))
		_, e = dbmap.Exec(stmt, valueArgs...)
		if e != nil {
			fmt.Println(e)
			log.Panicf("%s failed to bulk insert %s", code, table)
		}
		c = len(quotes)
	}
	return
}

func getLatestKl(code string, klt model.DBTab, offset int) (q *model.Quote) {
	e := dbmap.SelectOne(&q, fmt.Sprintf("select code, date, klid from %s where code = ? order by klid desc "+
		"limit 1 offset ?", klt), code, offset)
	if e != nil {
		if "sql: no rows in result set" == e.Error() {
			return nil
		} else {
			log.Panicln("failed to run sql", e)
		}
	}
	return
}
