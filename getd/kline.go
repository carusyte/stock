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
		// q.LrVol = sql.NullFloat64{}
		vol := math.Max(10, q.Volume.Float64)
		prevol := vol
		if i > 0 {
			prevol = math.Max(10, qs[i-1].Volume.Float64)
		}
		q.LrVol = sql.NullFloat64{Float64: math.Log(vol / prevol), Valid: true}
		//calculates LR for MA
		bias := .01
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
		//calculates LR for vol MA
		bias = 10
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
	if prev == 0 && cur == 0 {
		return 0
	} else if prev == 0 {
		if cur > 0 {
			return math.Log((cur + bias) / bias)
		}
		return math.Log(bias / (math.Abs(cur) + bias))
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

func getKline(stk *model.Stock, kltype []model.DBTab, wg *sync.WaitGroup, wf *chan int, outstks chan *model.Stock) {
	defer func() {
		wg.Done()
		<-*wf
	}()
	suc := false
	switch conf.Args.DataSource.Kline {
	case conf.WHT:
		_, suc = getKlineWht(stk, kltype, true)
	case conf.THS:
		_, suc = getKlineThs(stk, kltype)
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
	switch conf.Args.DataSource.Kline {
	case conf.THS:
		return klineThs(stk, klt, incr)
	case conf.TENCENT:
		return klineTc(stk, klt, incr)
	default:
		log.Panicf("unrecognized datasource: %+v", conf.Args.DataSource.Kline)
	}
	return
}

func getLongKlines(stk *model.Stock, klt model.DBTab, incr bool) (quotes []*model.Quote, suc bool) {
	switch conf.Args.DataSource.Kline {
	case conf.THS:
		return klineThs(stk, klt, incr)
	case conf.TENCENT:
		return klineTc(stk, klt, incr)
	default:
		log.Panicf("unrecognized datasource: %+v", conf.Args.DataSource.Kline)
	}
	return
}

func binsert(quotes []*model.Quote, table string, lklid int) (c int) {
	if len(quotes) == 0 {
		return 0
	}
	numFields := 57
	retry := 10
	rt := 0
	lklid++
	code := ""
	holders := make([]string, numFields)
	for i := range holders {
		holders[i] = "?"
	}
	holderString := fmt.Sprintf("(%s)", strings.Join(holders, ","))
	var e error
	for ; rt < retry; rt++ {
		valueStrings := make([]string, 0, len(quotes))
		valueArgs := make([]interface{}, 0, len(quotes)*numFields)
		var code string
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
		_, e = dbmap.Exec(fmt.Sprintf("delete from %s where code = ? and klid > ?", table), code, lklid)
		if e != nil {
			log.Printf("%s failed to delete %s where klid > %d", code, table, lklid)
			panic(e)
		}
		//TODO adapt new columns
		stmt := fmt.Sprintf("INSERT INTO %s (code,date,klid,open,high,close,low,"+
			"volume,amount,xrate,varate,varate_h,varate_o,varate_l,varate_rgl,varate_rgl_h,varate_rgl_o,"+
			"varate_rgl_l,lr,lr_h,lr_o,lr_l,lr_vol,ma5,ma10,ma20,ma30,ma60,ma120,ma200,ma250,"+
			"lr_ma5,lr_ma10,lr_ma20,lr_ma30,lr_ma60,lr_ma120,lr_ma200,lr_ma250,"+
			"vol5,vol10,vol20,vol30,vol60,vol120,vol200,vol250,"+
			"lr_vol5,lr_vol10,lr_vol20,lr_vol30,lr_vol60,lr_vol120,lr_vol200,lr_vol250,"+
			"udate,utime) "+
			"VALUES %s on duplicate key update date=values(date),"+
			"open=values(open),high=values(high),close=values(close),low=values(low),"+
			"volume=values(volume),amount=values(amount),xrate=values(xrate),varate=values(varate),"+
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

//Assign KLID, calculate Varate, add update datetime
func supplementMisc(klines []*model.Quote, start int) {
	//TODO calculate varate for MA
	d, t := util.TimeStr()
	scale := 100.
	preclose, prehigh, preopen, prelow := math.NaN(), math.NaN(), math.NaN(), math.NaN()
	for i := 0; i < len(klines); i++ {
		start++
		klines[i].Klid = start
		klines[i].Udate.Valid = true
		klines[i].Utime.Valid = true
		klines[i].Udate.String = d
		klines[i].Utime.String = t
		klines[i].Varate.Valid = true
		klines[i].VarateHigh.Valid = true
		klines[i].VarateOpen.Valid = true
		klines[i].VarateLow.Valid = true
		if math.IsNaN(preclose) {
			klines[i].Varate.Float64 = 0
			klines[i].VarateHigh.Float64 = 0
			klines[i].VarateOpen.Float64 = 0
			klines[i].VarateLow.Float64 = 0
		} else {
			klines[i].Varate.Float64 = CalVarate(preclose, klines[i].Close, scale)
			klines[i].VarateHigh.Float64 = CalVarate(prehigh, klines[i].High, scale)
			klines[i].VarateOpen.Float64 = CalVarate(preopen, klines[i].Open, scale)
			klines[i].VarateLow.Float64 = CalVarate(prelow, klines[i].Low, scale)
		}
		preclose = klines[i].Close
		prehigh = klines[i].High
		preopen = klines[i].Open
		prelow = klines[i].Low
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
		switch t {
		case model.KLINE_DAY:
			e = inferVarateRgl(stk, model.KLINE_DAY_NR, qmap[model.KLINE_DAY_NR], qs)
		case model.KLINE_WEEK:
			e = inferVarateRgl(stk, model.KLINE_WEEK_NR, qmap[model.KLINE_WEEK_NR], qs)
		case model.KLINE_MONTH:
			e = inferVarateRgl(stk, model.KLINE_MONTH_NR, qmap[model.KLINE_MONTH_NR], qs)
		default:
			//skip the rest types of kline
		}
		if e != nil {
			log.Println(e)
			return e
		}
	}
	return nil
}

func matchSlice(nrqs, tgqs []*model.Quote) (rqs []*model.Quote, err error) {
	s, e := -1, -1
	if len(nrqs) == 1 && len(tgqs) == 1 {
		if nrqs[0].Klid == tgqs[0].Klid && nrqs[0].Date == tgqs[0].Date {
			return nrqs, nil
		}
		return rqs, fmt.Errorf("can't find %+v @%d in the source slice", tgqs[0], 0)
	}
	for i := len(nrqs) - 1; i >= 0; i-- {
		if s < 0 && nrqs[i].Klid == tgqs[0].Klid && nrqs[i].Date == tgqs[0].Date {
			s = i
			break
		} else if e < 0 && nrqs[i].Klid == tgqs[len(tgqs)-1].Klid &&
			nrqs[i].Date == tgqs[len(tgqs)-1].Date {
			e = i
		} else if nrqs[i].Klid < tgqs[0].Klid || nrqs[i].Date < tgqs[0].Date {
			break
		}
	}
	if s < 0 {
		return rqs, fmt.Errorf("can't find %+v @%d in the source slice", tgqs[0], 0)
	} else if e < 0 {
		return rqs, fmt.Errorf("can't find %+v @%d in the source slice", tgqs[len(tgqs)-1], len(tgqs)-1)
	}
	return nrqs[s : e+1], nil
}

func inferVarateRgl(stk *model.Stock, tab model.DBTab, nrqs, tgqs []*model.Quote) error {
	if tgqs == nil || len(tgqs) == 0 {
		return fmt.Errorf("%s unable to infer varate_rgl from %v. please provide valid target quotes parameter",
			stk.Code, tab)
	}
	sDate, eDate := tgqs[0].Date, tgqs[len(tgqs)-1].Date
	if nrqs == nil || len(nrqs) < len(tgqs) {
		//load non-reinstated quotes from db
		nrqs = GetKlBtwn(stk.Code, tab, "["+sDate, eDate+"]", false)
	}
	if len(nrqs) < len(tgqs) {
		return fmt.Errorf("%s unable to infer varate rgl from %v. len(nrqs)=%d, len(tgqs)=%d",
			stk.Code, tab, len(nrqs), len(tgqs))
	}
	nrqs, e := matchSlice(nrqs, tgqs)
	if e != nil {
		return fmt.Errorf("%s failed to infer varate_rgl from %v: %+v", stk.Code, tab, e)
	}
	xemap, e := XdxrDateBetween(stk.Code, sDate, eDate)
	if e != nil {
		return fmt.Errorf("%s unable to infer varate_rgl from %v: %+v", stk.Code, tab, e)
	}
	return transferVarateRgl(stk.Code, tab, nrqs, tgqs, xemap)
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
		if len(xemap) > 0 && i > 0 {
			xdxr := false
			var xe *model.Xdxr
			switch tab {
			case model.KLINE_DAY_NR:
				xe, xdxr = xemap[tgq.Date]
			default:
				xe, xdxr, e = mergeXdxr(xemap, tgq.Date, tab)
			}
			if e != nil {
				return fmt.Errorf("%s unable to infer varate_rgl from %v. : %+v", code, tab, e)
			}
			if xdxr {
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
