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
	"github.com/ssgreg/repeat"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
)

//FetchRequest specifies the arguments to fetch data from remote sources
type FetchRequest struct {
	//RemoteSource for the trade data.
	RemoteSource model.DataSource
	//LocalSource for the trade data. *model.MasterKline will be used if not specified.
	LocalSource model.DataSource
	//Cycle for the trade data.
	Cycle model.CYTP
	//Reinstate for the trade data.
	Reinstate model.Rtype
}

//TrDataQry trading data query parameters
type TrDataQry struct {
	//LocalSource for the trade data. *model.MasterKline will be used if not specified.
	LocalSource model.DataSource
	//Cycle for the trade data. All *model.CYTP will be used if not specified.
	Cycle model.CYTP
	//Reinstate for the trade data. All *model.Rtype will be used if not specified.
	Reinstate                           model.Rtype
	Basic, LogRtn, MovAvg, MovAvgLogRtn bool
}

type dbTask struct {
	stock     *model.Stock
	tradeData *model.TradeData
	klid      int
}

//TradeDataField stands for the common table columns related to trade data.
type TradeDataField string

const (
	//Klid the kline ID for the specific trade data record
	Klid TradeDataField = "klid"
	//Date the date for a specific trade data record
	Date TradeDataField = "date"
)

var (
	kfmap map[model.DataSource]klineFetcher
)

//klineFetcher is capable of fetching kline data from a specific data source.
type klineFetcher interface {
	//fetchKline from specific data source for the given stock.
	fetchKline(stk *model.Stock, fr FetchRequest, incr bool) (
		tdmap map[FetchRequest]*model.TradeData, lkmap map[FetchRequest]int, suc, retry bool)
}

//stateful is capable of caching states in memory and provides capability to cleanup those states.
type stateful interface {
	//Cleanup cached states or resources
	cleanup()
}

type extraRequester interface {
	getExtraRequests(frIn []FetchRequest) (frOut []FetchRequest)
}

//GetTrDataDB get specified type of kline data from database.
func GetTrDataDB(code string, qry TrDataQry, limit int, desc bool) (trdat *model.TradeData) {
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
			case *[]*model.TradeDataBasic:
				trdat.Base = *i.(*[]*model.TradeDataBasic)
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
		go func(tab string, typ reflect.Type) {
			defer wg.Done()
			intf := reflect.New(reflect.SliceOf(typ)).Interface()
			// sql := fmt.Sprintf("select * from %s where code = ? %s %s order by klid %s",
			// 	table, cond1, cond2, d)
			if limit <= 0 {
				sql := fmt.Sprintf("select * from %s where code = ? order by klid", tab)
				if desc {
					sql += " desc"
				}
				_, e := dbmap.Select(&intf, sql, code)
				util.CheckErr(e, "failed to query "+tab+" for "+code)
			} else {
				d := ""
				if desc {
					d = "desc"
				}
				sql := fmt.Sprintf("select * from (select * from %s where code = ? order by klid desc limit ?) t "+
					"order by t.klid %s", tab, d)
				_, e := dbmap.Select(&intf, sql, code, limit)
				util.CheckErr(e, "failed to query "+tab+" for "+code)
			}
			ochan <- intf
		}(table, typ)
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
			case *[]*model.TradeDataBasic:
				trdat.Base = *i.(*[]*model.TradeDataBasic)
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
			cols := getTableColumns(typ)
			intf := reflect.New(reflect.SliceOf(typ)).Interface()
			sql := fmt.Sprintf("select %s from %s where code = ? %s %s order by klid %s",
				strings.Join(cols, ","), table, cond1, cond2, d)
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

//GetTrDataAt fetches trading data at specified dates/klids.
func GetTrDataAt(code string, qry TrDataQry, field TradeDataField, desc bool, val ...interface{}) (trdat *model.TradeData) {
	batSize := 1000
	total := float64(len(val))
	numGrp := int(math.Ceil(total / float64(batSize)))
	args := make([][]interface{}, numGrp)
	holders := make([][]string, numGrp)
	for i := range args {
		args[i] = append(args[i], code)
		grpSize := batSize
		if i == len(args)-1 {
			grpSize = int(total) - batSize*(numGrp-1)
		}
		holders[i] = make([]string, grpSize)
		for j := 0; j < grpSize; j++ {
			idx := i*batSize + j
			args[i] = append(args[i], val[idx])
			holders[i][j] = "?"
		}
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
			case []*model.TradeDataBasic:
				trdat.Base = i.([]*model.TradeDataBasic)
			case []*model.TradeDataLogRtn:
				trdat.LogRtn = i.([]*model.TradeDataLogRtn)
			case []*model.TradeDataMovAvg:
				trdat.MovAvg = i.([]*model.TradeDataMovAvg)
			case []*model.TradeDataMovAvgLogRtn:
				trdat.MovAvgLogRtn = i.([]*model.TradeDataMovAvgLogRtn)
			default:
				log.Panicf("Unsupported type for query result consolidation: %v", reflect.TypeOf(i).String())
			}
		}
	}()

	for table, typ := range tables {
		wg.Add(1)
		go func(table string, typ reflect.Type) {
			defer wg.Done()
			intf := reflect.Zero(reflect.SliceOf(typ)).Interface()
			for i := range args {
				cond := fmt.Sprintf("%s in (%s)", field, strings.Join(holders[i], ","))
				ss := reflect.New(reflect.SliceOf(typ)).Interface()
				sql := fmt.Sprintf("select * from %s where code = ? and %s order by klid %s",
					table, cond, d)
				_, e := dbmap.Select(ss, sql, args[i]...)
				util.CheckErr(e, "failed to query "+table+" for "+code)
				intf = reflect.AppendSlice(
					reflect.ValueOf(intf),
					reflect.Indirect(reflect.ValueOf(ss)),
				).Interface()
			}
			ochan <- intf
		}(table, typ)
	}
	wg.Wait()
	close(ochan)
	wgr.Wait()

	return
}

func initKlineFetcher(frs ...FetchRequest) (dsmap map[model.DataSource][]FetchRequest) {
	if kfmap == nil {
		kfmap = make(map[model.DataSource]klineFetcher)
	}
	dsmap = splitFetchRequests(frs...)
	resolveDSMap(dsmap)
	return
}

//check fetReq.RemoteSource. If none is set, use config file data source.
func splitFetchRequests(frs ...FetchRequest) (dsmap map[model.DataSource][]FetchRequest) {
	dsmap = make(map[model.DataSource][]FetchRequest)
	for _, fr := range frs {
		src := model.DataSource(conf.Args.DataSource.Kline)
		if len(fr.RemoteSource) > 0 {
			src = fr.RemoteSource
		} else {
			fr.RemoteSource = src
		}
		dsmap[src] = append(dsmap[src], fr)
	}
	return
}

func resolveDSMap(dsmap map[model.DataSource][]FetchRequest) {
	for src, reqs := range dsmap {
		if _, ok := kfmap[src]; ok {
			continue
		}
		f := resolveKlineFetcher(src)
		kfmap[src] = f
		if ef, ok := f.(extraRequester); ok {
			//layer-2 kline fetcher, won't populate dsmap
			exreqs := ef.getExtraRequests(reqs)
			resolveDSMap(splitFetchRequests(exreqs...))
		}
	}
}

func resolveKlineFetcher(src model.DataSource) (f klineFetcher) {
	switch src {
	case model.WHT:
		// tdmap, lkmap, suc = getKlineWht(stk, kltnv)
		log.Panicf("unsupported data source: %+v", src)
	case model.THS:
		// qmap, lkmap, suc = getKlineThs(stk, kltnv)
		log.Panicf("unsupported data source: %+v", src)
	case model.TC:
		// tdmap, lkmap, suc = getKlineTc(stk, kltnv)
		log.Panicf("unsupported data source: %+v", src)
	case model.XQ:
		f = &XqKlineFetcher{}
	case model.EM:
		f = &EmKlineFetcher{}
	default:
		log.Panicf("unsupported data source: %+v", src)
	}
	return
}

//resolveTableNames resolve trade data queries to database table names.
func resolveTableNames(qs ...FetchRequest) (tables []string) {
	for _, q := range qs {
		base := ""
		qtabs := make([]string, 0, 4)
		if len(q.LocalSource) > 0 {
			base = string(q.LocalSource) + "_"
		} else {
			base = string(model.KlineMaster) + "_"
		}
		switch q.Cycle {
		case model.DAY:
			base += "d_"
		case model.WEEK:
			base += "w_"
		case model.MONTH:
			base += "m_"
		default:
			//cycle not specified, all cycle type will be used
			if len(qtabs) == 0 {
				qtabs = append(qtabs, base+"d_")
				qtabs = append(qtabs, base+"w_")
				qtabs = append(qtabs, base+"m_")
			} else {
				newTables := make([]string, 0, 4)
				for _, t := range tables {
					newTables = append(newTables, t+"b")
					newTables = append(newTables, t+"f")
					newTables = append(newTables, t+"n")
				}
				qtabs = newTables
			}
		}
		switch q.Reinstate {
		case model.Backward:
			base += "b"
		case model.Forward:
			base += "f"
		case model.None:
			base += "n"
		default:
			//reinstate not specified, all reinstatement type will be used
			if len(qtabs) == 0 {
				qtabs = append(qtabs, base+"b")
				qtabs = append(qtabs, base+"f")
				qtabs = append(qtabs, base+"n")
			} else {
				newTables := make([]string, 0, 4)
				for _, t := range tables {
					newTables = append(newTables, t+"b")
					newTables = append(newTables, t+"f")
					newTables = append(newTables, t+"n")
				}
				qtabs = newTables
			}
		}
		if len(qtabs) == 0 {
			qtabs = append(qtabs, base)
		}
		for _, t := range qtabs {
			tables = append(tables, t)
		}
	}
	return
}

func resolveTables(q TrDataQry) (tables map[string]reflect.Type) {
	tables = make(map[string]reflect.Type)
	//prelim checking
	if !q.Basic && !q.LogRtn && !q.MovAvg && !q.MovAvgLogRtn {
		log.Panicf("Invalid query parameters. Please specify at least one table to query. Params: %+v", q)
	}
	base := "kline_"
	if len(q.LocalSource) > 0 {
		base = string(q.LocalSource) + "_"
	}
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
		tables[base] = reflect.TypeOf(&model.TradeDataBasic{})
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
	base := ""
	switch td.Source {
	case "":
		base = string(model.KlineMaster) + "_"
	default:
		base = string(td.Source) + "_"
	}
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
		tabCols[base] = getTableColumns(model.TradeDataBasic{})
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
	var t reflect.Type
	var ok bool
	if t, ok = i.(reflect.Type); ok {
		if reflect.Ptr == t.Kind() {
			//if it's a pointer, must indirect
			t = reflect.Indirect(reflect.Zero(t)).Type()
		}
	} else {
		t = reflect.TypeOf(i)
	}
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
		if "-" != c {
			cols = append(cols, strings.ToLower(c))
		}
	}
	return
}

//CalLogReturnsV2 calculates log return for high, open, close, low, volume,
// and variation rates, or regulated variation rates if available.
func CalLogReturnsV2(trdat *model.TradeData) {
	if trdat == nil {
		return
	}
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
		if vcl/100.+1. <= 0 {
			lr.Close.Valid = true
			v := 0.
			if i > 0 {
				v = util.LogReturn(trdat.Base[i-1].Close, b.Close, bias)
			}
			lr.Close.Float64 = v
		} else {
			lr.Close = sql.NullFloat64{Float64: math.Log(vcl/100. + 1.), Valid: true}
		}
		if vhg/100.+1. <= 0 {
			lr.High.Valid = true
			v := 0.
			if i > 0 {
				v = util.LogReturn(trdat.Base[i-1].High, b.High, bias)
			}
			lr.High.Float64 = v
		} else {
			lr.High = sql.NullFloat64{Float64: math.Log(vhg/100. + 1.), Valid: true}
		}
		if vop/100.+1. <= 0 {
			lr.Open.Valid = true
			v := 0.
			if i > 0 {
				v = util.LogReturn(trdat.Base[i-1].Open, b.Open, bias)
			}
			lr.Open.Float64 = v
		} else {
			lr.Open = sql.NullFloat64{Float64: math.Log(vop/100. + 1.), Valid: true}
		}
		if vlw/100.+1. <= 0 {
			lr.Low.Valid = true
			v := 0.
			if i > 0 {
				v = util.LogReturn(trdat.Base[i-1].Low, b.Low, bias)
			}
			lr.Low.Float64 = v
		} else {
			lr.Low = sql.NullFloat64{Float64: math.Log(vlw/100. + 1.), Valid: true}
		}
		lr.HighClose = sql.NullFloat64{Float64: util.LogReturn(b.Close, b.High, bias), Valid: true}
		lr.OpenClose = sql.NullFloat64{Float64: util.LogReturn(b.Close, b.Open, bias), Valid: true}
		lr.LowClose = sql.NullFloat64{Float64: util.LogReturn(b.Close, b.Low, bias), Valid: true}

		if (trdat.Cycle == model.DAY) && len(conf.Args.DataSource.LimitPriceDayLr) > 0 {
			limit := conf.Args.DataSource.LimitPriceDayLr
			b, t := limit[0], limit[1]
			if lr.Close.Float64 < b {
				log.Debugf("%s (%s %s) %s %d lr close below lower limit %f: %.5f, clipped", lr.Code, trdat.Cycle, trdat.Reinstatement, lr.Date, lr.Klid, b, lr.Close.Float64)
				lr.Close.Float64 = b
			} else if lr.Close.Float64 > t {
				log.Debugf("%s (%s %s) %s %d lr close exceeds upper limit %f: %.5f, clipped", lr.Code, trdat.Cycle, trdat.Reinstatement, lr.Date, lr.Klid, t, lr.Close.Float64)
				lr.Close.Float64 = t
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
	if trdat == nil || trdat.MaxLen() == 0 {
		return
	}
	hasMA := len(trdat.MovAvg) > 0
	d, t := util.TimeStr()
	scale := 100.
	preclose, prehigh, preopen, prelow := math.NaN(), math.NaN(), math.NaN(), math.NaN()
	mas := []int{5, 10, 20, 30, 60, 120, 200, 250}
	size := len(trdat.Base)
	maSrc := make([]*model.TradeDataBasic, size)
	for i := range maSrc {
		maSrc[i] = trdat.Base[len(maSrc)-1-i]
	}
	//expand maSrc for ma calculation
	sklid := strconv.Itoa(start + 1 - mas[len(mas)-1])
	eklid := strconv.Itoa(start + 1)
	src := GetTrDataBtwn(
		trdat.Code,
		TrDataQry{
			LocalSource: trdat.Source,
			Cycle:       trdat.Cycle,
			Reinstate:   trdat.Reinstatement,
			Basic:       true,
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
	if len(trdat.Base) != 0 {
		c = len(trdat.Base)
	}
	if len(trdat.LogRtn) != 0 {
		if c == 0 {
			c = len(trdat.LogRtn)
		} else if len(trdat.LogRtn) != c {
			log.Panicf("mismatched log return trade data length: %d, vs. %d", len(trdat.LogRtn), c)
		}
	}
	if len(trdat.MovAvg) != 0 {
		if c == 0 {
			c = len(trdat.MovAvg)
		} else if len(trdat.MovAvg) != c {
			log.Panicf("mismatched moving average trade data length: %d, vs. %d", len(trdat.MovAvg), c)
		}
	}
	if len(trdat.MovAvgLogRtn) != 0 {
		if c == 0 {
			c = len(trdat.MovAvgLogRtn)
		} else if len(trdat.MovAvgLogRtn) != c {
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
					log.Panicf("%s failed to delete before bulk insert %s: %+v", code, table, e)
				}
			}
			break
		}
		if rt >= retry {
			log.Panicf("%s failed to delete %s where klid > %d", code, table, lklid)
		}
	}

	var wg sync.WaitGroup
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
				log.Panicf("%s failed to bulk insert %s: %+v, value args:\n%+v", code, table, e, valueArgs)
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

//rely on non-reinstated and xdxr data to calculate the regulated varate.
func calcVarateRglV2(stk *model.Stock, tdmap map[FetchRequest]*model.TradeData) (e error) {
	for fr, td := range tdmap {
		if fr.Reinstate != model.Forward {
			continue
		}
		nfr := FetchRequest{
			RemoteSource: fr.RemoteSource,
			LocalSource:  fr.LocalSource,
			Cycle:        fr.Cycle,
			Reinstate:    model.None,
		}
		e = inferVarateRglV2(stk, tdmap[nfr], td)
		if e != nil {
			log.Warn(e)
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
				LocalSource: tgtd.Source,
				Cycle:       tgtd.Cycle,
				Reinstate:   model.None,
				Basic:       true,
			},
			Date,
			"["+sDate, eDate+"]",
			false)
	}
	if len(nrtd.Base) == 0 {
		log.Warnf("%s %v non-reinstated data not available, skipping varate_rgl calculation", stk.Code, tgtd.Cycle)
		return nil
	}
	if !conf.Args.DataSource.Validate.DropInconsistent {
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
	if oLenLR < oLenTG && !conf.Args.DataSource.Validate.DropInconsistent {
		return fmt.Errorf("nrtd.MaxLen()=%d, tgtd.MaxLen()=%d, missing data in nrtd", nrtd.MaxLen(), tgtd.MaxLen())
	}
	//use map (hash function, specifically) to find the intersection and prune all types of trade data within tgtd
	bHashFunc := func(el interface{}) interface{} {
		b := el.(*model.TradeDataBasic)
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
	if conf.Args.DataSource.Validate.DropInconsistent {
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
	hashTable = make(map[interface{}]int)
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

func transferVarateRglV2(code string, cycle model.CYTP, rtype model.Rtype, nrbase, tgbase []*model.TradeDataBasic, xemap map[string]*model.Xdxr) (e error) {
	for i := 0; i < len(tgbase); i++ {
		nrq := nrbase[i]
		tgq := tgbase[i]
		if nrq.Code != tgq.Code || nrq.Date != tgq.Date || nrq.Klid != tgq.Klid {
			return fmt.Errorf("%s unable to infer varate rgl from (%v,%v). unmatched nrq & tgq at %d: %+v vs. %+v",
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

//UpdateValidateKlineParams syncs params from configuration file to database.
func UpdateValidateKlineParams() (e error) {
	d, t := util.TimeStr()
	stmt := fmt.Sprintf("INSERT INTO params (section, param, value, udate, utime) VALUES (?,?,?,?,?)" +
		" on duplicate key update section=values(section),param=values(param),value=values(value)," +
		" udate=values(udate),utime=values(utime)")
	for rt := 0; rt < conf.Args.DeadlockRetry; rt++ {
		_, e = dbmap.Exec(stmt, "Validate Kline", "DataSource", conf.Args.DataSource.Validate.Source, d, t)
		if e != nil {
			fmt.Println(e)
			if strings.Contains(e.Error(), "Deadlock") {
				continue
			} else {
				return errors.Wrap(e, "failed to update params table")
			}
		}
		log.Infof("validate kline params updated")
		return
	}
	return errors.Wrap(e, "failed to update params table")
}

//GetKlinesV2 Get various types of kline data for the given stocks. Returns the stocks that have been successfully processed.
func GetKlinesV2(stks *model.Stocks, fetReq ...FetchRequest) (rstks *model.Stocks) {
	defer Cleanup()
	tabs := resolveTableNames(fetReq...)
	log.Printf("fetching kline data for %d stocks: %+v", stks.Size(), tabs)
	var wg sync.WaitGroup
	parallel := conf.Args.Concurrency
	if conf.THS == conf.Args.DataSource.Kline {
		parallel = conf.Args.ChromeDP.PoolSize
	}
	wf := make(chan int, parallel)
	outstks := make(chan *model.Stock, JobCapacity)
	rstks = new(model.Stocks)
	wgr := collect(rstks, outstks)
	dsmap := initKlineFetcher(fetReq...)
	dbcMap := createDbTaskQueues(dsmap)
	wgdb := saveTradeData(outstks, dbcMap, stks.Size())
	for _, stk := range stks.List {
		wg.Add(1)
		wf <- 1
		go getKlineV2(stk, dsmap, dbcMap, &wg, wf)
	}
	wg.Wait()
	close(wf)
	waitDBTasks(wgdb, dbcMap)
	close(outstks)
	wgr.Wait()
	log.Printf("%d stocks %s data updated.", rstks.Size(), strings.Join(tabs, ", "))
	if stks.Size() != rstks.Size() {
		same, skp := stks.Diff(rstks)
		if !same {
			log.Printf("Failed: %+v", skp)
		}
	}
	return
}

func waitDBTasks(wgdb []*sync.WaitGroup, dbcMap map[FetchRequest]chan *dbTask) {
	for _, ch := range dbcMap {
		close(ch)
	}
	for _, wg := range wgdb {
		wg.Wait()
	}
}

//FreeFetcherResources after usage
func FreeFetcherResources() {
	for _, f := range kfmap {
		if s, ok := f.(stateful); ok {
			s.cleanup()
		}
	}
}

func getKlineFromSource(stk *model.Stock, kf klineFetcher, fetReq ...FetchRequest) (
	tdmap map[FetchRequest]*model.TradeData, lkmap map[FetchRequest]int, suc bool) {

	tdmap = make(map[FetchRequest]*model.TradeData)
	lkmap = make(map[FetchRequest]int)
	code := stk.Code
	xdxr := latestUFRXdxr(stk.Code)

	genop := func(q FetchRequest, incr bool) (op func(c int) (e error)) {
		return func(c int) (e error) {
			rtdMap, rlkMap, suc, retry := kf.fetchKline(stk, q, incr)
			if !suc {
				e = fmt.Errorf("failed to get kline for %s", code)
				if retry {
					log.Printf("%s retrying [%d]", code, c+1)
					return repeat.HintTemporary(e)
				}
				return repeat.HintStop(e)
			}
			for fr, td := range rtdMap {
				if td != nil {
					tabs := resolveTableNames(fr)
					log.Infof("%s %+v fetched: %d", code, tabs, td.MaxLen())
				}
				tdmap[fr] = td
				lkmap[fr] = rlkMap[fr]
			}
			return nil
		}
	}

	suc = true
	for _, q := range fetReq {
		incr := true
		if q.Reinstate == model.Forward {
			incr = xdxr == nil
		}
		e := repeat.Repeat(
			repeat.FnWithCounter(genop(q, incr)),
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

func getKlineV2(stk *model.Stock, dsmap map[model.DataSource][]FetchRequest, qmap map[FetchRequest]chan *dbTask,
	wg *sync.WaitGroup, wf chan int) {
	defer func() {
		wg.Done()
		<-wf
	}()

	tdmap := make(map[FetchRequest]*model.TradeData)
	lkmap := make(map[FetchRequest]int)
	for src, frs := range dsmap {
		kf := kfmap[src]
		tdmapTmp, lkmapTmp, suc := getKlineFromSource(stk, kf, frs...)
		if !suc {
			//abort immediately
			return
		}
		for k, v := range tdmapTmp {
			tdmap[k] = v
		}
		for k, v := range lkmapTmp {
			lkmap[k] = v
		}
	}
	for q, trdat := range tdmap {
		supplementMiscV2(trdat, lkmap[q])
	}
	// if !isIndex(stk.Code) {
	// 	if e := calcVarateRglV2(stk, tdmap); e != nil {
	// 		log.Errorf("%s failed to calculate varate_rgl: %+v", stk.Code, e)
	// 	}
	// }
	for fr, trdat := range tdmap {
		CalLogReturnsV2(trdat)
		if trdat != nil && lkmap[fr] != -1 {
			//incremental fetch. skip the first record which is for varate calculation
			trdat.Base = trdat.Base[1:]
			trdat.LogRtn = trdat.LogRtn[1:]
			trdat.MovAvg = trdat.MovAvg[1:]
			trdat.MovAvgLogRtn = trdat.MovAvgLogRtn[1:]
		}
		qmap[fr] <- &dbTask{
			stock:     stk,
			tradeData: trdat,
			klid:      lkmap[fr],
		}
	}
	return
}

func createDbTaskQueues(dsmap map[model.DataSource][]FetchRequest) (qmap map[FetchRequest]chan *dbTask) {
	qmap = make(map[FetchRequest]chan *dbTask)
	for ds, frs := range dsmap {
		for _, fr := range frs {
			qmap[fr] = make(chan *dbTask, conf.Args.DBQueueCapacity)
		}
		if req, ok := kfmap[ds].(extraRequester); ok {
			exfr := req.getExtraRequests(frs)
			for _, fr := range exfr {
				if _, ok = qmap[fr]; !ok {
					qmap[fr] = make(chan *dbTask, conf.Args.DBQueueCapacity)
				}
			}
		}
	}
	return
}

func saveTradeData(outstks chan *model.Stock,
	dbcMap map[FetchRequest]chan *dbTask, total int) (wgs []*sync.WaitGroup) {
	snmap := new(sync.Map)
	sumFR := len(dbcMap)
	lock := new(sync.RWMutex)
	pk := "progress"
	snmap.Store(pk, 0)
	for _, ch := range dbcMap {
		wg := new(sync.WaitGroup)
		wgs = append(wgs, wg)
		wg.Add(1)
		go func(wg *sync.WaitGroup, ch chan *dbTask) {
			defer wg.Done()
			for j := range ch {
				c := 0
				if j.tradeData != nil {
					c = binsertV2(j.tradeData, j.klid)
				}
				if j.tradeData == nil || c == j.tradeData.MaxLen() {
					lock.Lock()
					var cnt interface{}
					if cnt, _ = snmap.LoadOrStore(j.stock.Code, 0); cnt.(int) == sumFR-1 {
						snmap.Delete(j.stock.Code)
						v, _ := snmap.Load(pk)
						outstks <- j.stock
						p := v.(int) + 1
						pp := float64(p) / float64(total) * 100.
						log.Printf("%s all requested klines fetched. Progress: [%.2f%%]", j.stock.Code, pp)
						snmap.Store(pk, p)
					} else {
						snmap.Store(j.stock.Code, cnt.(int)+1)
					}
					lock.Unlock()
				}
			}
		}(wg, ch)
	}
	return
}
