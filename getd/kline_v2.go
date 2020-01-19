package getd

import (
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
)

//TrDataQry trading data query parameters
type TrDataQry struct {
	Cycle                               model.CYTP
	Reinstate                           model.Rtype
	Basic, LogRtn, MovAvg, MovAvgLogRtn bool
}

//TradeDataField stands for the common table columns related to trade data.
type TradeDataField string

const (
	//Klid the kline ID for the specific trade data record
	Klid = "klid"
	//Date the date for a specific trade data record
	Date = "date"
)

//GetTrDataDB get specified type of kline data from database.
func GetTrDataDB(code string, qry TrDataQry, limit int, desc bool) (trdat *model.TradeData) {
	//TODO refactor me
	tables := resolveTables(qry)
	var wg, wgr sync.WaitGroup
	//A slice of trading data of arbitrary kind
	ochan := make(chan interface{}, 4)

	//Collect and merge query results
	wgr.Add(1)
	go func() {
		defer wgr.Done()
		for i := range ochan {
			//merge into model.TradeData slice
			switch i.(type) {
			case *[]*model.TradeDataBase:
				trdat.Base = *i.(*[]*model.TradeDataBase)
			case *[]*model.TradeDataLogRtn:
				trdat.LogRtn = *i.(*[]*model.TradeDataLogRtn)
			case *[]*model.TradeDataMovAvg:
				trdat.MovAvg = *i.(*[]*model.TradeDataMovAvg)
			case *[]*model.TradeDataMovAvgLogRtn:
				trdat.MovAvgLogRtn = *i.(*[]*model.TradeDataMovAvgLogRtn)
			default:
				log.Panicf("Unsupported type for query result consolidation: %v", reflect.TypeOf(i).String())
			}
		}
	}()

	for table, typ := range tables {
		wg.Add(1)
		go func() {
			defer wg.Done()
			intf := reflect.New(reflect.SliceOf(typ)).Interface()
			// sql := fmt.Sprintf("select * from %s where code = ? %s %s order by klid %s",
			// 	table, cond1, cond2, d)
			if limit <= 0 {
				sql := fmt.Sprintf("select * from %s where code = ? order by klid", table)
				if desc {
					sql += " desc"
				}
				_, e := dbmap.Select(&intf, sql, code)
				util.CheckErr(e, "failed to query "+table+" for "+code)
			} else {
				d := ""
				if desc {
					d = "desc"
				}
				sql := fmt.Sprintf("select * from (select * from %s where code = ? order by klid desc limit ?) t "+
					"order by t.klid %s", table, d)
				_, e := dbmap.Select(&intf, sql, code, limit)
				util.CheckErr(e, "failed to query "+table+" for "+code)
			}
			ochan <- intf
		}()
	}
	wg.Wait()
	close(ochan)
	wgr.Wait()

	return
}

//GetTrDataBtwn fetches trading data between dates/klids.
func GetTrDataBtwn(code string, qry TrDataQry, field TradeDataField, cond1, cond2 string, desc bool) (trdat *model.TradeData) {
	var (
		v1, v2 interface{}
		args   []interface{}
		e      error
	)
	args = append(args, code)
	if len(cond1) > 0 {
		op := ">"
		if strings.HasPrefix(cond1, "[") {
			op += "="
			v1 = cond1[1:]
		} else {
			v1 = cond1
		}
		cond1 = fmt.Sprintf("and %s %s ?", field, op)
		if Klid == field {
			v1, e = strconv.Atoi(v1.(string))
			util.CheckErr(e, fmt.Sprintf("failed to convert string to int: %s", v1))
		}
		args = append(args, v1)
	}
	if len(cond2) > 0 {
		op := "<"
		if strings.HasSuffix(cond2, "]") {
			op += "="
			v2 = cond2[:len(cond2)-1]
		} else {
			v2 = cond2
		}
		cond2 = fmt.Sprintf("and %s %s ?", field, op)
		if Klid == field {
			v2, e = strconv.Atoi(v2.(string))
			util.CheckErr(e, fmt.Sprintf("failed to convert string to int: %s", v2))
		}
		args = append(args, v2)
	}
	d := ""
	if desc {
		d = "desc"
	}

	tables := resolveTables(qry)
	var wg, wgr sync.WaitGroup
	//A slice of trading data of arbitrary kind
	ochan := make(chan interface{}, 4)

	trdat = &model.TradeData{
		Code:          code,
		Cycle:         qry.Cycle,
		Reinstatement: qry.Reinstate,
	}

	//Collect and merge query results
	wgr.Add(1)
	go func() {
		defer wgr.Done()
		for i := range ochan {
			//merge into model.TradeData slice
			switch i.(type) {
			case *[]*model.TradeDataBase:
				trdat.Base = *i.(*[]*model.TradeDataBase)
			case *[]*model.TradeDataLogRtn:
				trdat.LogRtn = *i.(*[]*model.TradeDataLogRtn)
			case *[]*model.TradeDataMovAvg:
				trdat.MovAvg = *i.(*[]*model.TradeDataMovAvg)
			case *[]*model.TradeDataMovAvgLogRtn:
				trdat.MovAvgLogRtn = *i.(*[]*model.TradeDataMovAvgLogRtn)
			default:
				log.Panicf("Unsupported type for query result consolidation: %v", reflect.TypeOf(i).String())
			}
		}
	}()

	for table, typ := range tables {
		wg.Add(1)
		go func(table string, typ reflect.Type) {
			defer wg.Done()
			intf := reflect.New(reflect.SliceOf(typ)).Interface()
			sql := fmt.Sprintf("select * from %s where code = ? %s %s order by klid %s",
				table, cond1, cond2, d)
			_, e := dbmap.Select(intf, sql, args...)
			util.CheckErr(e, "failed to query "+table+" for "+code)
			ochan <- intf
		}(table, typ)
	}
	wg.Wait()
	close(ochan)
	wgr.Wait()

	return
}

func resolveTables(q TrDataQry) (tables map[string]reflect.Type) {
	tables = make(map[string]reflect.Type)
	//prelim checking
	if !q.Basic && !q.LogRtn && !q.MovAvg && !q.MovAvgLogRtn {
		log.Panicf("Invalid query parameters. Please specify at least one table to query. Params: %+v", q)
	}
	base := "kline_"
	switch q.Cycle {
	case model.DAY:
		base += "d_"
	case model.WEEK:
		base += "w_"
	case model.MONTH:
		base += "m_"
	default:
		log.Panicf("Unsupported cycle type: %v, query param: %+v", q.Cycle, q)
	}
	switch q.Reinstate {
	case model.Backward:
		base += "b"
	case model.Forward:
		base += "f"
	case model.None:
		base += "n"
	default:
		log.Panicf("Unsupported reinstatement type: %v, query param: %+v", q.Reinstate, q)
	}
	if q.Basic {
		tables[base] = reflect.TypeOf(&model.TradeDataBase{})
	}
	if q.LogRtn {
		tables[base+"_lr"] = reflect.TypeOf(&model.TradeDataLogRtn{})
	}
	if q.MovAvg {
		tables[base+"_ma"] = reflect.TypeOf(&model.TradeDataMovAvg{})
	}
	if q.MovAvgLogRtn {
		tables[base+"_ma_lr"] = reflect.TypeOf(&model.TradeDataMovAvgLogRtn{})
	}
	return
}

// returns a mapping of [database table] to [table columns] based on availability of data sets in TradeData.
func resolveTradeDataTables(td *model.TradeData) (tabCols map[string][]string, tabData map[string]interface{}) {
	if td.Empty() {
		return
	}
	tabCols = make(map[string][]string)
	tabData = make(map[string]interface{})
	base := "kline_"
	switch td.Cycle {
	case model.DAY:
		base += "d_"
	case model.WEEK:
		base += "w_"
	case model.MONTH:
		base += "m_"
	default:
		log.Panicf("Unsupported cycle type: %v, query param: %+v", td.Cycle, td)
	}
	switch td.Reinstatement {
	case model.Backward:
		base += "b"
	case model.Forward:
		base += "f"
	case model.None:
		base += "n"
	default:
		log.Panicf("Unsupported reinstatement type: %v, query param: %+v", td.Reinstatement, td)
	}
	if len(td.Base) > 0 {
		tabCols[base] = getTableColumns(model.TradeDataBase{})
		tabData[base] = td.Base
	}
	if len(td.LogRtn) > 0 {
		tabCols[base+"_lr"] = getTableColumns(model.TradeDataLogRtn{})
		tabData[base+"_lr"] = td.LogRtn
	}
	if len(td.MovAvg) > 0 {
		tabCols[base+"_ma"] = getTableColumns(model.TradeDataMovAvg{})
		tabData[base+"_ma"] = td.MovAvg
	}
	if len(td.MovAvgLogRtn) > 0 {
		tabCols[base+"_ma_lr"] = getTableColumns(model.TradeDataMovAvgLogRtn{})
		tabData[base+"_ma_lr"] = td.MovAvgLogRtn
	}
	return
}

//returns the column names of the ORM mapping defined in the struct.
func getTableColumns(i interface{}) (cols []string) {
	t := reflect.TypeOf(i)
	n := t.NumField()
	for i := 0; i < n; i++ {
		f := t.Field(i)
		v, ok := f.Tag.Lookup("db")
		c := ""
		if !ok || len(v) == 0 {
			c = f.Name
		} else {
			c = strings.Split(v, ",")[0]
		}
		cols = append(cols, strings.ToLower(c))
	}
	return
}

//CalLogReturnsV2 calculates log return for high, open, close, low, and volume
// variation rates, or regulated variation rates if available.
func CalLogReturnsV2(trdat *model.TradeData) {
	//TODO refactor me
	hasLogRtn := len(trdat.LogRtn) > 0
	hasMovAvgLogRtn := len(trdat.MovAvgLogRtn) > 0
	for i, b := range trdat.Base {
		var lr *model.TradeDataLogRtn
		var malr *model.TradeDataMovAvgLogRtn
		ma := trdat.MovAvg[i]
		vcl := b.VarateRgl.Float64
		vhg := b.VarateRglHigh.Float64
		vop := b.VarateRglOpen.Float64
		vlw := b.VarateRglLow.Float64
		if !b.VarateRgl.Valid {
			vcl = b.Varate.Float64
		}
		if !b.VarateRglHigh.Valid {
			vhg = b.VarateHigh.Float64
		}
		if !b.VarateRglOpen.Valid {
			vop = b.VarateOpen.Float64
		}
		if !b.VarateRglLow.Valid {
			vlw = b.VarateLow.Float64
		}
		if hasLogRtn {
			lr = trdat.LogRtn[i]
		} else {
			lr = &model.TradeDataLogRtn{
				Code:  b.Code,
				Date:  b.Date,
				Klid:  b.Klid,
				Udate: b.Udate,
				Utime: b.Utime,
			}
			trdat.LogRtn = append(trdat.LogRtn, lr)
		}
		if hasMovAvgLogRtn {
			malr = trdat.MovAvgLogRtn[i]
		} else {
			malr = &model.TradeDataMovAvgLogRtn{
				Code:  b.Code,
				Date:  b.Date,
				Klid:  b.Klid,
				Udate: b.Udate,
				Utime: b.Utime,
			}
			trdat.MovAvgLogRtn = append(trdat.MovAvgLogRtn, malr)
		}
		bias := .01
		lr.Lr = sql.NullFloat64{Float64: math.Log(vcl/100. + 1.), Valid: true}
		lr.High = sql.NullFloat64{Float64: math.Log(vhg/100. + 1.), Valid: true}
		lr.HighClose = sql.NullFloat64{Float64: util.LogReturn(b.Close, b.High, bias), Valid: true}
		lr.Open = sql.NullFloat64{Float64: math.Log(vop/100. + 1.), Valid: true}
		lr.OpenClose = sql.NullFloat64{Float64: util.LogReturn(b.Close, b.Open, bias), Valid: true}
		lr.Low = sql.NullFloat64{Float64: math.Log(vlw/100. + 1.), Valid: true}
		lr.LowClose = sql.NullFloat64{Float64: util.LogReturn(b.Close, b.Low, bias), Valid: true}

		if (trdat.Cycle == model.DAY) && len(conf.Args.DataSource.LimitPriceDayLr) > 0 {
			limit := conf.Args.DataSource.LimitPriceDayLr
			b, t := limit[0], limit[1]
			if lr.Lr.Float64 < b {
				log.Debugf("%s (%s %s) %s %d lr below lower limit %f: %.5f, clipped", lr.Code, trdat.Cycle, trdat.Reinstatement, lr.Date, lr.Klid, b, lr.Lr.Float64)
				lr.Lr.Float64 = b
			} else if lr.Lr.Float64 > t {
				log.Debugf("%s (%s %s) %s %d lr exceeds upper limit %f: %.5f, clipped", lr.Code, trdat.Cycle, trdat.Reinstatement, lr.Date, lr.Klid, t, lr.Lr.Float64)
				lr.Lr.Float64 = t
			}
			if lr.High.Float64 < b {
				log.Debugf("%s (%s %s) %s %d lr_h below lower limit %f: %.5f, clipped", lr.Code, trdat.Cycle, trdat.Reinstatement, lr.Date, lr.Klid, b, lr.High.Float64)
				lr.High.Float64 = b
			} else if lr.High.Float64 > t {
				log.Debugf("%s (%s %s) %s %d lr_h exceeds upper limit %f: %.5f, clipped", lr.Code, trdat.Cycle, trdat.Reinstatement, lr.Date, lr.Klid, t, lr.High.Float64)
				lr.High.Float64 = t
			}
			if lr.HighClose.Float64 < b {
				log.Debugf("%s (%s %s) %s %d lr_h_c below lower limit %f: %.5f, clipped", lr.Code, trdat.Cycle, trdat.Reinstatement, lr.Date, lr.Klid, b, lr.HighClose.Float64)
				lr.HighClose.Float64 = b
			} else if lr.HighClose.Float64 > t {
				log.Debugf("%s (%s %s) %s %d lr_h_c exceeds upper limit %f: %.5f, clipped", lr.Code, trdat.Cycle, trdat.Reinstatement, lr.Date, lr.Klid, t, lr.HighClose.Float64)
				lr.HighClose.Float64 = t
			}
			if lr.Open.Float64 < b {
				log.Debugf("%s (%s %s) %s %d lr_o below lower limit %f: %.5f, clipped", lr.Code, trdat.Cycle, trdat.Reinstatement, lr.Date, lr.Klid, b, lr.Open.Float64)
				lr.Open.Float64 = b
			} else if lr.Open.Float64 > t {
				log.Debugf("%s (%s %s) %s %d lr_o exceeds upper limit %f: %.5f, clipped", lr.Code, trdat.Cycle, trdat.Reinstatement, lr.Date, lr.Klid, t, lr.Open.Float64)
				lr.Open.Float64 = t
			}
			if lr.OpenClose.Float64 < b {
				log.Debugf("%s (%s %s) %s %d lr_o_c below lower limit %f: %.5f, clipped", lr.Code, trdat.Cycle, trdat.Reinstatement, lr.Date, lr.Klid, b, lr.OpenClose.Float64)
				lr.OpenClose.Float64 = b
			} else if lr.OpenClose.Float64 > t {
				log.Debugf("%s (%s %s) %s %d lr_o_c exceeds upper limit %f: %.5f, clipped", lr.Code, trdat.Cycle, trdat.Reinstatement, lr.Date, lr.Klid, t, lr.OpenClose.Float64)
				lr.OpenClose.Float64 = t
			}
			if lr.Low.Float64 < b {
				log.Debugf("%s (%s %s) %s %d lr_l below lower limit %f: %.5f, clipped", lr.Code, trdat.Cycle, trdat.Reinstatement, lr.Date, lr.Klid, b, lr.Low.Float64)
				lr.Low.Float64 = b
			} else if lr.Low.Float64 > t {
				log.Debugf("%s (%s %s) %s %d lr_l exceeds upper limit %f: %.5f, clipped", lr.Code, trdat.Cycle, trdat.Reinstatement, lr.Date, lr.Klid, t, lr.Low.Float64)
				lr.Low.Float64 = t
			}
			if lr.LowClose.Float64 < b {
				log.Debugf("%s (%s %s) %s %d lr_l_c below lower limit %f: %.5f, clipped", lr.Code, trdat.Cycle, trdat.Reinstatement, lr.Date, lr.Klid, b, lr.LowClose.Float64)
				lr.LowClose.Float64 = b
			} else if lr.LowClose.Float64 > t {
				log.Debugf("%s (%s %s) %s %d lr_l_c exceeds upper limit %f: %.5f, clipped", lr.Code, trdat.Cycle, trdat.Reinstatement, lr.Date, lr.Klid, t, lr.LowClose.Float64)
				lr.LowClose.Float64 = t
			}
		}

		if b.Xrate.Valid {
			lr.Xrate.Valid = true
			if i > 0 && trdat.Base[i-1].Xrate.Valid {
				lr.Xrate.Float64 = util.LogReturn(trdat.Base[i-1].Xrate.Float64, b.Xrate.Float64, bias)
			}
		}
		//calculates LR for MA
		if ma.Ma5.Valid {
			malr.Ma5 = sql.NullFloat64{Float64: util.LogReturn(ma.Ma5.Float64, b.Close, bias), Valid: true}
			malr.Ma5Open = sql.NullFloat64{Float64: util.LogReturn(ma.Ma5.Float64, b.Open, bias), Valid: true}
			malr.Ma5High = sql.NullFloat64{Float64: util.LogReturn(ma.Ma5.Float64, b.High, bias), Valid: true}
			malr.Ma5Low = sql.NullFloat64{Float64: util.LogReturn(ma.Ma5.Float64, b.Low, bias), Valid: true}
		}
		if ma.Ma10.Valid {
			malr.Ma10 = sql.NullFloat64{Float64: util.LogReturn(ma.Ma10.Float64, b.Close, bias), Valid: true}
			malr.Ma10Open = sql.NullFloat64{Float64: util.LogReturn(ma.Ma10.Float64, b.Open, bias), Valid: true}
			malr.Ma10High = sql.NullFloat64{Float64: util.LogReturn(ma.Ma10.Float64, b.High, bias), Valid: true}
			malr.Ma10Low = sql.NullFloat64{Float64: util.LogReturn(ma.Ma10.Float64, b.Low, bias), Valid: true}
		}
		if ma.Ma20.Valid {
			malr.Ma20 = sql.NullFloat64{Float64: util.LogReturn(ma.Ma20.Float64, b.Close, bias), Valid: true}
			malr.Ma20Open = sql.NullFloat64{Float64: util.LogReturn(ma.Ma20.Float64, b.Open, bias), Valid: true}
			malr.Ma20High = sql.NullFloat64{Float64: util.LogReturn(ma.Ma20.Float64, b.High, bias), Valid: true}
			malr.Ma20Low = sql.NullFloat64{Float64: util.LogReturn(ma.Ma20.Float64, b.Low, bias), Valid: true}
		}
		if ma.Ma30.Valid {
			malr.Ma30 = sql.NullFloat64{Float64: util.LogReturn(ma.Ma30.Float64, b.Close, bias), Valid: true}
			malr.Ma30Open = sql.NullFloat64{Float64: util.LogReturn(ma.Ma30.Float64, b.Open, bias), Valid: true}
			malr.Ma30High = sql.NullFloat64{Float64: util.LogReturn(ma.Ma30.Float64, b.High, bias), Valid: true}
			malr.Ma30Low = sql.NullFloat64{Float64: util.LogReturn(ma.Ma30.Float64, b.Low, bias), Valid: true}
		}
		if ma.Ma60.Valid {
			malr.Ma60 = sql.NullFloat64{Float64: util.LogReturn(ma.Ma60.Float64, b.Close, bias), Valid: true}
			malr.Ma60Open = sql.NullFloat64{Float64: util.LogReturn(ma.Ma60.Float64, b.Open, bias), Valid: true}
			malr.Ma60High = sql.NullFloat64{Float64: util.LogReturn(ma.Ma60.Float64, b.High, bias), Valid: true}
			malr.Ma60Low = sql.NullFloat64{Float64: util.LogReturn(ma.Ma60.Float64, b.Low, bias), Valid: true}
		}
		if ma.Ma120.Valid {
			malr.Ma120 = sql.NullFloat64{Float64: util.LogReturn(ma.Ma120.Float64, b.Close, bias), Valid: true}
			malr.Ma120Open = sql.NullFloat64{Float64: util.LogReturn(ma.Ma120.Float64, b.Open, bias), Valid: true}
			malr.Ma120High = sql.NullFloat64{Float64: util.LogReturn(ma.Ma120.Float64, b.High, bias), Valid: true}
			malr.Ma120Low = sql.NullFloat64{Float64: util.LogReturn(ma.Ma120.Float64, b.Low, bias), Valid: true}
		}
		if ma.Ma200.Valid {
			malr.Ma200 = sql.NullFloat64{Float64: util.LogReturn(ma.Ma200.Float64, b.Close, bias), Valid: true}
			malr.Ma200Open = sql.NullFloat64{Float64: util.LogReturn(ma.Ma200.Float64, b.Open, bias), Valid: true}
			malr.Ma200High = sql.NullFloat64{Float64: util.LogReturn(ma.Ma200.Float64, b.High, bias), Valid: true}
			malr.Ma200Low = sql.NullFloat64{Float64: util.LogReturn(ma.Ma200.Float64, b.Low, bias), Valid: true}
		}
		if ma.Ma250.Valid {
			malr.Ma250 = sql.NullFloat64{Float64: util.LogReturn(ma.Ma250.Float64, b.Close, bias), Valid: true}
			malr.Ma250Open = sql.NullFloat64{Float64: util.LogReturn(ma.Ma250.Float64, b.Open, bias), Valid: true}
			malr.Ma250High = sql.NullFloat64{Float64: util.LogReturn(ma.Ma250.Float64, b.High, bias), Valid: true}
			malr.Ma250Low = sql.NullFloat64{Float64: util.LogReturn(ma.Ma250.Float64, b.Low, bias), Valid: true}
		}
		bias = 10
		if b.Volume.Valid {
			lr.Volume.Valid = true
			if i > 0 && trdat.Base[i-1].Volume.Valid {
				lr.Volume.Float64 = util.LogReturn(trdat.Base[i-1].Volume.Float64, b.Volume.Float64, bias)
			}
		}
		if b.Amount.Valid {
			lr.Amount.Valid = true
			if i > 0 && trdat.Base[i-1].Amount.Valid {
				lr.Amount.Float64 = util.LogReturn(trdat.Base[i-1].Amount.Float64, b.Amount.Float64, bias)
			}
		}
		//calculates LR for vol MA
		if ma.Vol5.Valid && b.Volume.Valid {
			malr.Vol5 = sql.NullFloat64{
				Float64: util.LogReturn(ma.Vol5.Float64, b.Volume.Float64, bias),
				Valid:   true,
			}
		}
		if ma.Vol10.Valid && b.Volume.Valid {
			malr.Vol10 = sql.NullFloat64{
				Float64: util.LogReturn(ma.Vol10.Float64, b.Volume.Float64, bias),
				Valid:   true,
			}
		}
		if ma.Vol20.Valid && b.Volume.Valid {
			malr.Vol20 = sql.NullFloat64{
				Float64: util.LogReturn(ma.Vol20.Float64, b.Volume.Float64, bias),
				Valid:   true,
			}
		}
		if ma.Vol30.Valid && b.Volume.Valid {
			malr.Vol30 = sql.NullFloat64{
				Float64: util.LogReturn(ma.Vol30.Float64, b.Volume.Float64, bias),
				Valid:   true,
			}
		}
		if ma.Vol60.Valid && b.Volume.Valid {
			malr.Vol60 = sql.NullFloat64{
				Float64: util.LogReturn(ma.Vol60.Float64, b.Volume.Float64, bias),
				Valid:   true,
			}
		}
		if ma.Vol120.Valid && b.Volume.Valid {
			malr.Vol120 = sql.NullFloat64{
				Float64: util.LogReturn(ma.Vol120.Float64, b.Volume.Float64, bias),
				Valid:   true,
			}
		}
		if ma.Vol200.Valid && b.Volume.Valid {
			malr.Vol200 = sql.NullFloat64{
				Float64: util.LogReturn(ma.Vol200.Float64, b.Volume.Float64, bias),
				Valid:   true,
			}
		}
		if ma.Vol250.Valid && b.Volume.Valid {
			malr.Vol250 = sql.NullFloat64{
				Float64: util.LogReturn(ma.Vol250.Float64, b.Volume.Float64, bias),
				Valid:   true,
			}
		}
	}
}

//Assign KLID, calculate Varate, MovAvg, add update datetime
func supplementMiscV2(trdat *model.TradeData, start int) {
	if trdat.MaxLen() == 0 {
		return
	}
	hasMA := len(trdat.MovAvg) > 0
	d, t := util.TimeStr()
	scale := 100.
	preclose, prehigh, preopen, prelow := math.NaN(), math.NaN(), math.NaN(), math.NaN()
	mas := []int{5, 10, 20, 30, 60, 120, 200, 250}
	size := len(trdat.Base)
	maSrc := make([]*model.TradeDataBase, size)
	for i := range maSrc {
		maSrc[i] = trdat.Base[len(maSrc)-1-i]
	}
	//expand maSrc for ma calculation
	sklid := strconv.Itoa(start + 1 - mas[len(mas)-1])
	eklid := strconv.Itoa(start + 1)
	src := GetTrDataBtwn(
		trdat.Code,
		TrDataQry{
			Cycle:     trdat.Cycle,
			Reinstate: trdat.Reinstatement,
			Basic:     true,
		},
		Klid,
		sklid,
		eklid, true)
	//maSrc is in descending order, contrary to klines
	maSrc = append(maSrc, src.Base...)
	for i := 0; i < size; i++ {
		start++

		tdb := trdat.Base[i]
		tdb.Klid = start
		tdb.Udate.Valid = true
		tdb.Utime.Valid = true
		tdb.Udate.String = d
		tdb.Utime.String = t
		tdb.Varate.Valid = true
		tdb.VarateHigh.Valid = true
		tdb.VarateOpen.Valid = true
		tdb.VarateLow.Valid = true

		var tdma *model.TradeDataMovAvg
		if hasMA {
			tdma = trdat.MovAvg[i]
		} else {
			tdma = &model.TradeDataMovAvg{
				Code:  tdb.Code,
				Date:  tdb.Date,
				Klid:  tdb.Klid,
				Udate: tdb.Udate,
				Utime: tdb.Utime,
			}
			trdat.MovAvg = append(trdat.MovAvg, tdma)
		}
		if math.IsNaN(preclose) {
			tdb.Varate.Float64 = 0
			tdb.VarateHigh.Float64 = 0
			tdb.VarateOpen.Float64 = 0
			tdb.VarateLow.Float64 = 0
		} else {
			tdb.Varate.Float64 = CalVarate(preclose, tdb.Close, scale)
			tdb.VarateHigh.Float64 = CalVarate(prehigh, tdb.High, scale)
			tdb.VarateOpen.Float64 = CalVarate(preopen, tdb.Open, scale)
			tdb.VarateLow.Float64 = CalVarate(prelow, tdb.Low, scale)
		}
		preclose = tdb.Close
		prehigh = tdb.High
		preopen = tdb.Open
		prelow = tdb.Low
		//calculate various ma if nil
		start := size - 1 - i
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
				if !tdma.Ma5.Valid {
					tdma.Ma5.Valid = true
					tdma.Ma5.Float64 = ma
				}
				if !tdma.Vol5.Valid {
					tdma.Vol5.Valid = true
					tdma.Vol5.Float64 = mavol
				}
			case 10:
				if !tdma.Ma10.Valid {
					tdma.Ma10.Valid = true
					tdma.Ma10.Float64 = ma
				}
				if !tdma.Vol10.Valid {
					tdma.Vol10.Valid = true
					tdma.Vol10.Float64 = mavol
				}
			case 20:
				if !tdma.Ma20.Valid {
					tdma.Ma20.Valid = true
					tdma.Ma20.Float64 = ma
				}
				if !tdma.Vol20.Valid {
					tdma.Vol20.Valid = true
					tdma.Vol20.Float64 = mavol
				}
			case 30:
				if !tdma.Ma30.Valid {
					tdma.Ma30.Valid = true
					tdma.Ma30.Float64 = ma
				}
				if !tdma.Vol30.Valid {
					tdma.Vol30.Valid = true
					tdma.Vol30.Float64 = mavol
				}
			case 60:
				if !tdma.Ma60.Valid {
					tdma.Ma60.Valid = true
					tdma.Ma60.Float64 = ma
				}
				if !tdma.Vol60.Valid {
					tdma.Vol60.Valid = true
					tdma.Vol60.Float64 = mavol
				}
			case 120:
				if !tdma.Ma120.Valid {
					tdma.Ma120.Valid = true
					tdma.Ma120.Float64 = ma
				}
				if !tdma.Vol120.Valid {
					tdma.Vol120.Valid = true
					tdma.Vol120.Float64 = mavol
				}
			case 200:
				if !tdma.Ma200.Valid {
					tdma.Ma200.Valid = true
					tdma.Ma200.Float64 = ma
				}
				if !tdma.Vol200.Valid {
					tdma.Vol200.Valid = true
					tdma.Vol200.Float64 = mavol
				}
			case 250:
				if !tdma.Ma250.Valid {
					tdma.Ma250.Valid = true
					tdma.Ma250.Float64 = ma
				}
				if !tdma.Vol250.Valid {
					tdma.Vol250.Valid = true
					tdma.Vol250.Float64 = mavol
				}
			default:
				log.Panicf("unsupported MA value: %d", m)
			}
		}
	}
}

func binsertV2(trdat *model.TradeData, lklid int) (c int) {
	if trdat == nil || trdat.Empty() {
		return 0
	}
	c = 0
	if len(trdat.Base) != 0 {
		if c == 0 {
			c = len(trdat.Base)
		} else if c != 0 && len(trdat.Base) != c {
			log.Panicf("mismatched basic trade data length: %d, vs. %d", len(trdat.Base), c)
		}
	}
	if len(trdat.LogRtn) != 0 {
		if c == 0 {
			c = len(trdat.LogRtn)
		} else if c != 0 && len(trdat.LogRtn) != c {
			log.Panicf("mismatched log return trade data length: %d, vs. %d", len(trdat.LogRtn), c)
		}
	}
	if len(trdat.MovAvg) != 0 {
		if c == 0 {
			c = len(trdat.MovAvg)
		} else if c != 0 && len(trdat.MovAvg) != c {
			log.Panicf("mismatched moving average trade data length: %d, vs. %d", len(trdat.MovAvg), c)
		}
	}
	if len(trdat.MovAvgLogRtn) != 0 {
		if c == 0 {
			c = len(trdat.MovAvgLogRtn)
		} else if c != 0 && len(trdat.MovAvgLogRtn) != c {
			log.Panicf("mismatched moving average log return trade data length: %d, vs. %d", len(trdat.MovAvgLogRtn), c)
		}
	}
	retry := conf.Args.DeadlockRetry
	rt := 0
	lklid++
	code := trdat.Code
	tables, data := resolveTradeDataTables(trdat)
	var e error
	// delete stale records first
	for table := range tables {
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
	}

	var wg sync.WaitGroup
	//FIXME relevant table inserts should be grouped into a transaction
	for table, cols := range tables {
		wg.Add(1)
		go insertTradeData(table, cols, data[table], &wg)
	}
	wg.Wait()
	return
}

func insertMinibatchV2(table string, cols []string, v reflect.Value) (c int) {
	//v is a slice of trade data of some kind
	rowSize := v.Len()
	elem := reflect.Indirect(v.Index(0))
	numFields := len(cols)
	retry := conf.Args.DeadlockRetry
	rt := 0
	code := elem.FieldByName("Code").String()
	holders := make([]string, numFields)
	for i := range holders {
		holders[i] = "?"
	}
	holderString := fmt.Sprintf("(%s)", strings.Join(holders, ","))
	var e error
	valueStrings := make([]string, 0, rowSize)
	updateStr := make([]string, 0, numFields)
	valueArgs := make([]interface{}, 0, rowSize*numFields)

	for _, col := range cols {
		if !strings.EqualFold("code", col) && !strings.EqualFold("klid", col) {
			updateStr = append(updateStr, fmt.Sprintf("%[1]s=values(%[1]s)", col))
		}
	}

	for i := 0; i < rowSize; i++ {
		elem := reflect.Indirect(v.Index(i))
		valueStrings = append(valueStrings, holderString)
		for j := 0; j < numFields; j++ {
			valueArgs = append(valueArgs, elem.Field(j).Interface())
		}
	}

	rt = 0
	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s on duplicate key update %s",
		table, strings.Join(cols, ","), strings.Join(valueStrings, ","), strings.Join(updateStr, ","))
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
		return rowSize
	}
	log.Panicf("%s failed to bulk insert %s: %+v", code, table, e)
	return
}

func insertTradeData(table string, cols []string, rows interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	v := reflect.ValueOf(rows)
	len := v.Len()
	batchSize := 1000
	c := 0
	for idx := 0; idx < len; idx += batchSize {
		end := int(math.Min(float64(len), float64(idx+batchSize)))
		c += insertMinibatchV2(table, cols, v.Slice(idx, end))
	}
	if len != c {
		log.Panicf("unmatched given records and actual inserted records: %d vs. %d", len, c)
	}
}

func calcVarateRglV2(stk *model.Stock, tdmap map[model.DBTab]*model.TradeData) (e error) {
	for t, td := range tdmap {
		switch t {
		case model.KLINE_DAY_F:
			e = inferVarateRglV2(stk, tdmap[model.KLINE_DAY_NR], td)
		case model.KLINE_WEEK_F:
			e = inferVarateRglV2(stk, tdmap[model.KLINE_WEEK_NR], td)
		case  model.KLINE_MONTH_F:
			e = inferVarateRglV2(stk, tdmap[model.KLINE_MONTH_NR], td)
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

//inferVarateRglV2 infers regulated varate based on provided non-reinstated and target trade data,
//and updates target trade data accordingly.
func inferVarateRglV2(stk *model.Stock, nrtd, tgtd *model.TradeData) (e error) {
	if tgtd == nil || tgtd.MaxLen() == 0 {
		return fmt.Errorf("%s unable to infer varate_rgl. Please provide valid target trade data input", stk.Code)
	}
	if nrtd == nil || nrtd.MaxLen() < tgtd.MaxLen() {
		//load non-reinstated quotes from db
		sDate, eDate := tgtd.Base[0].Date, tgtd.Base[len(tgtd.Base)-1].Date
		nrtd = GetTrDataBtwn(
			stk.Code,
			TrDataQry{
				Cycle:     tgtd.Cycle,
				Reinstate: model.None,
				Basic:     true,
			},
			Date,
			"["+sDate, eDate+"]",
			false)
	}
	if len(nrtd.Base) == 0 {
		log.Warnf("%s %v non-reinstated data not available, skipping varate_rgl calculation", stk.Code, tgtd.Cycle)
		return nil
	}
	if !conf.Args.DataSource.DropInconsistent {
		if len(nrtd.Base) < len(tgtd.Base) {
			return fmt.Errorf("%s unable to infer varate rgl for (%v,%v). len(nrtd.Base)=%d, len(tgtd.Base)=%d",
				stk.Code, tgtd.Cycle, tgtd.Reinstatement, len(nrtd.Base), len(tgtd.Base))
		}
	}
	e = matchSliceV2(nrtd, tgtd)
	if e != nil {
		return fmt.Errorf("%s failed to infer varate_rgl for (%v,%v): %+v", stk.Code, tgtd.Cycle, tgtd.Reinstatement, e)
	}
	if len(nrtd.Base) == 0 || len(tgtd.Base) == 0 {
		return nil
	}
	//reset start-date and end-date
	sDate := tgtd.Base[0].Date
	eDate := tgtd.Base[len(tgtd.Base)-1].Date
	xemap, e := XdxrDateBetween(stk.Code, sDate, eDate)
	if e != nil {
		return fmt.Errorf("%s unable to infer varate_rgl for (%v,%v): %+v", stk.Code, tgtd.Cycle, tgtd.Reinstatement, e)
	}
	return transferVarateRglV2(stk.Code, tgtd.Cycle, tgtd.Reinstatement, nrtd.Base, tgtd.Base, xemap)
}

//matchSlice assumes nrtd.MaxLen() >= tgtd.MaxLen() in normal cases, takes care of missing data in-between,
// trying best to make sure nrtd.MaxLen() == tgtd.MaxLen()
func matchSliceV2(nrtd, tgtd *model.TradeData) (err error) {
	oLenLR, oLenTG := nrtd.MaxLen(), tgtd.MaxLen()
	if oLenLR < oLenTG && !conf.Args.DataSource.DropInconsistent {
		return fmt.Errorf("nrtd.MaxLen()=%d, tgtd.MaxLen()=%d, missing data in nrtd", nrtd.MaxLen(), tgtd.MaxLen())
	}
	//use map (hash function, specifically) to find the intersection and prune all types of trade data within tgtd
	bHashFunc := func(el interface{}) interface{} {
		b := el.(*model.TradeDataBase)
		return fmt.Sprintf("%s_%d", b.Date, b.Klid)
	}
	nrbHash := hash(bHashFunc, nrtd.Base)
	tgbHash := hash(bHashFunc, tgtd.Base)
	var nrbKeep, tgbKeep []int
	for k, i := range tgbHash {
		if j, ok := nrbHash[k]; ok {
			tgbKeep = append(tgbKeep, i)
			nrbKeep = append(nrbKeep, j)
		}
	}
	nrtd.Keep(nrbKeep...)
	tgtd.Keep(tgbKeep...)
	lenNR, lenTG := nrtd.MaxLen(), tgtd.MaxLen()
	if conf.Args.DataSource.DropInconsistent {
		if lenTG != lenNR {
			var d int64
			var e error
			code := nrtd.Code
			date := tgtd.Base[0].Date
			if lenTG != 0 {
				date = tgtd.Base[len(tgtd.Base)-1].Date
			}
			d, e = deleteTradeDataFromDate(code, date, nrtd.Cycle, nrtd.Reinstatement)
			if e != nil {
				log.Warnf("failed to delete kline for %s (%v,%v) from date %s: %+v",
					code, nrtd.Cycle, nrtd.Reinstatement, date, e)
				return e
			}
			if d != 0 {
				log.Warnf("%s inconsistency found in (%v,%v). dropping %d, from date %s",
					code, nrtd.Cycle, nrtd.Reinstatement, d, date)
			}
		}
	} else {
		if lenTG != oLenTG || lenTG == 0 {
			return fmt.Errorf("data inconsistent. nrtd:%+v\ntgtd:%+v", nrtd, tgtd)
		}
		lastTg := tgtd.Base[len(tgtd.Base)-1]
		lastNr := nrtd.Base[len(nrtd.Base)-1]
		if lastTg.Date != lastNr.Date || lastTg.Klid != lastNr.Klid {
			return fmt.Errorf("data inconsistent. nrtd:%+v\ntgtd:%+v", nrtd, tgtd)
		}
	}
	return
}

//hash maps each element in data slice to its position, using hash function to generate the hash table key.
func hash(hashFunc func(el interface{}) (hashKey interface{}), array interface{}) (hashTable map[interface{}]int) {
	av := reflect.ValueOf(array)
	for i := 0; i < av.Len(); i++ {
		hashTable[hashFunc(av.Index(i).Interface())] = i
	}
	return
}

func deleteTradeDataFromDate(code, date string, cycle model.CYTP, rtype model.Rtype) (d int64, e error) {
	tabs := resolveTables(TrDataQry{
		Cycle:        cycle,
		Reinstate:    rtype,
		Basic:        true,
		LogRtn:       true,
		MovAvg:       true,
		MovAvgLogRtn: true,
	})
	retry := 10
	for t := range tabs {
		tried := 0
		sql := fmt.Sprintf("delete from %v where code = ? and date >= ?", t)
		for ; tried < retry; tried++ {
			r, e := dbmap.Exec(sql, code, date)
			if e != nil {
				log.Warnf("%s failed to delete %v for date %s, database error:%+v", code, t, date, e)
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
	}
	return
}

func transferVarateRglV2(code string, cycle model.CYTP, rtype model.Rtype, nrbase, tgbase []*model.TradeDataBase, xemap map[string]*model.Xdxr) (e error) {
	for i := 0; i < len(tgbase); i++ {
		nrq := nrbase[i]
		tgq := tgbase[i]
		if nrq.Code != tgq.Code || nrq.Date != tgq.Date || nrq.Klid != tgq.Klid {
			return fmt.Errorf("%s unable to infer varate rgl from (%v,%v). unmatched nrq & tgq at %d: %+v : %+v",
				code, cycle, rtype, i, nrq, tgq)
		}
		tvar := nrq.Varate.Float64
		tvarh := nrq.VarateHigh.Float64
		tvaro := nrq.VarateOpen.Float64
		tvarl := nrq.VarateLow.Float64
		// first element is assumed to be dropped, so its values are irrelevant
		if len(xemap) > 0 && i > 0 {
			xe := MergeXdxrBetween(tgbase[i-1].Date, tgq.Date, xemap)
			if xe != nil {
				// adjust fore-day price for regulated varate calculation
				pcl := Reinstate(nrbase[i-1].Close, xe)
				phg := Reinstate(nrbase[i-1].High, xe)
				pop := Reinstate(nrbase[i-1].Open, xe)
				plw := Reinstate(nrbase[i-1].Low, xe)
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
