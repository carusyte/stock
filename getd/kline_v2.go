package getd

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
)

//TrDataQry trading data query parameters
type TrDataQry struct {
	Cycle                               model.CYTP
	Reinstate                           model.Rtype
	Basic, LogRtn, MovAvg, MovAvgLogRtn bool
}

//GetTrDataBtwn fetches trading data between dates.
func GetTrDataBtwn(code string, qry TrDataQry, dt1, dt2 string, desc bool) (trdat model.TradeData) {
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

	tables := resolveTables(qry)
	var wg, wgr sync.WaitGroup
	//A slice of trading data of arbitrary kind
	ochan := make(chan interface{}, 4)

	//Collect and merge query results
	wgr.Add(1)
	go func(wgr *sync.WaitGroup, oc chan interface{}) {
		defer wgr.Done()
		for i := range oc {
			//merge into model.TradeData slice
			switch i.(type) {
			case *[]*model.TradeDataBase:
				trdat.Base = *i.(*[]*model.TradeDataBase)
				// base := *i.(*[]*model.TradeDataBase)
				// if len(trdat) == 0 {
				// 	trdat = make([]*model.TradeData, len(base))
				// 	for i := 0; i < len(base); i++ {
				// 		b := base[i]
				// 		trdat[i] = &model.TradeData{
				// 			Code:          b.Code,
				// 			Date:          b.Date,
				// 			Klid:          b.Klid,
				// 			Cycle:         qry.Cycle,
				// 			Reinstatement: qry.Reinstate,
				// 			Base:          b,
				// 		}
				// 	}
				// } else if len(trdat) != len(base) {
				// 	logrus.Panicf("Length mismatched, TradeData: %d, TradeDataBase: %d", len(trdat), len(base))
				// } else {
				// 	for i := 0; i < len(base); i++ {
				// 		trdat[i].Base = base[i]
				// 	}
				// }
			case *[]*model.TradeDataLogRtn:
				trdat.LogRtn = *i.(*[]*model.TradeDataLogRtn)
				// lr := *i.(*[]*model.TradeDataLogRtn)
				// if len(trdat) == 0 {
				// 	trdat = make([]*model.TradeData, len(lr))
				// 	for i := 0; i < len(lr); i++ {
				// 		l := lr[i]
				// 		trdat[i] = &model.TradeData{
				// 			Code:          l.Code,
				// 			Date:          l.Date,
				// 			Klid:          l.Klid,
				// 			Cycle:         qry.Cycle,
				// 			Reinstatement: qry.Reinstate,
				// 			LogRtn:        l,
				// 		}
				// 	}
				// } else if len(trdat) != len(lr) {
				// 	logrus.Panicf("Length mismatched, TradeData: %d, TradeDataLogRtn: %d", len(trdat), len(lr))
				// } else {
				// 	for i := 0; i < len(lr); i++ {
				// 		trdat[i].LogRtn = lr[i]
				// 	}
				// }
			case *[]*model.TradeDataMovAvg:
				trdat.MovAvg = *i.(*[]*model.TradeDataMovAvg)
				// ma := *i.(*[]*model.TradeDataMovAvg)
				// if len(trdat) == 0 {
				// 	trdat = make([]*model.TradeData, len(ma))
				// 	for i := 0; i < len(ma); i++ {
				// 		m := ma[i]
				// 		trdat[i] = &model.TradeData{
				// 			Code:          m.Code,
				// 			Date:          m.Date,
				// 			Klid:          m.Klid,
				// 			Cycle:         qry.Cycle,
				// 			Reinstatement: qry.Reinstate,
				// 			MovAvg:        m,
				// 		}
				// 	}
				// } else if len(trdat) != len(ma) {
				// 	logrus.Panicf("Length mismatched, TradeData: %d, TradeDataMovAvg: %d", len(trdat), len(ma))
				// } else {
				// 	for i := 0; i < len(ma); i++ {
				// 		trdat[i].MovAvg = ma[i]
				// 	}
				// }
			case *[]*model.TradeDataMovAvgLogRtn:
				trdat.MovAvgLogRtn = *i.(*[]*model.TradeDataMovAvgLogRtn)
				// malr := *i.(*[]*model.TradeDataMovAvgLogRtn)
				// if len(trdat) == 0 {
				// 	trdat = make([]*model.TradeData, len(malr))
				// 	for i := 0; i < len(malr); i++ {
				// 		ml := malr[i]
				// 		trdat[i] = &model.TradeData{
				// 			Code:          ml.Code,
				// 			Date:          ml.Date,
				// 			Klid:          ml.Klid,
				// 			Cycle:         qry.Cycle,
				// 			Reinstatement: qry.Reinstate,
				// 			MovAvgLogRtn:  ml,
				// 		}
				// 	}
				// } else if len(trdat) != len(malr) {
				// 	logrus.Panicf("Length mismatched, TradeData: %d, TradeDataMovAvgLogRtn: %d", len(trdat), len(malr))
				// } else {
				// 	for i := 0; i < len(malr); i++ {
				// 		trdat[i].MovAvgLogRtn = malr[i]
				// 	}
				// }
			default:
				logrus.Panicf("Unsupported type for query result consolidation: %v", reflect.TypeOf(i).String())
			}
		}
	}(&wgr, ochan)

	for table, typ := range tables {
		wg.Add(1)
		go func(code, table string, typ reflect.Type, c1, c2, desc string, wg *sync.WaitGroup, oc chan interface{}) {
			defer wg.Done()
			intf := reflect.New(reflect.SliceOf(typ)).Interface()
			sql := fmt.Sprintf("select * from %s where code = ? %s %s order by klid %s",
				table, dt1cond, dt2cond, d)
			_, e := dbmap.Select(intf, sql, code)
			util.CheckErr(e, "failed to query "+string(table)+" for "+code)
			oc <- intf
		}(code, table, typ, dt1cond, dt2cond, d, &wg, ochan)
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
		logrus.Panicf("Invalid query parameters. Please specify at least one table to query. Params: %+v", q)
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
		logrus.Panicf("Unsupported cycle type: %v, query param: %+v", q.Cycle, q)
	}
	switch q.Reinstate {
	case model.Backward:
		base += "b"
	case model.Forward:
		base += "f"
	case model.None:
		base += "n"
	default:
		logrus.Panicf("Unsupported reinstatement type: %v, query param: %+v", q.Reinstate, q)
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
	base := "kline_"
	switch td.Cycle {
	case model.DAY:
		base += "d_"
	case model.WEEK:
		base += "w_"
	case model.MONTH:
		base += "m_"
	default:
		logrus.Panicf("Unsupported cycle type: %v, query param: %+v", td.Cycle, td)
	}
	switch td.Reinstatement {
	case model.Backward:
		base += "b"
	case model.Forward:
		base += "f"
	case model.None:
		base += "n"
	default:
		logrus.Panicf("Unsupported reinstatement type: %v, query param: %+v", td.Reinstatement, td)
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
