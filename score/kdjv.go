package score

import (
	"github.com/carusyte/stock/model"
	rm "github.com/carusyte/rima/model"
	"math"
	"github.com/carusyte/stock/getd"
	"fmt"
	"github.com/carusyte/stock/util"
	"time"
	"reflect"
	"github.com/pkg/errors"
	logr "github.com/sirupsen/logrus"
	"github.com/montanaflynn/stats"
	"log"
	"sync"
	"runtime"
	"strings"
	"sort"
	"github.com/satori/go.uuid"
	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/rpc"
)

// Medium to Long term model.
// Search for stocks with best KDJ form which closely matches the historical ones indicating the buy opportunity.
type KdjV struct {
	Code  string
	Name  string
	Dod   float64 // Degree of Distinction in stats
	Sfl   float64 // Safe Line in stats
	Bmean float64 // Buy Score Mean in stats
	Smean float64 // Sell Score Mean in stats
	Len   string
	CCMO  string
	CCWK  string
	CCDY  string
}

const (
	WEIGHT_KDJV_MONTH float64 = 40.0
	WEIGHT_KDJV_WEEK  float64 = 30.0
	WEIGHT_KDJV_DAY   float64 = 30.0
)

func (k *KdjV) GetFieldStr(name string) string {
	switch name {
	case "DOD":
		return fmt.Sprintf("%.2f", k.Dod)
	case "SFL":
		return fmt.Sprintf("%.2f", k.Sfl)
	case "BMEAN":
		return fmt.Sprintf("%.2f", k.Bmean)
	case "SMEAN":
		return fmt.Sprintf("%.2f", k.Smean)
	case "LEN":
		return k.Len
	case "KDJ_DY":
		return k.CCDY
	case "KDJ_WK":
		return k.CCWK
	case "KDJ_MO":
		return k.CCMO
	default:
		r := reflect.ValueOf(k)
		f := reflect.Indirect(r).FieldByName(name)
		if !f.IsValid() {
			panic(errors.New("undefined field for KDJV: " + name))
		}
		return fmt.Sprintf("%+v", f.Interface())
	}
}

// The codes slice may contain either stock codes or index codes. If not specified, both will be handled.
func (k *KdjV) Get(codes []string, limit int, ranked bool) (r *Result) {
	r = &Result{}
	r.PfIds = append(r.PfIds, k.Id())
	var (
		stks   []*model.Stock
		idxlst []*model.IdxLst
		items  []*Item
		e      error
	)
	if codes == nil || len(codes) == 0 {
		stks = getd.StocksDb()
		idxlst, e = getd.GetIdxLst()
		if e != nil {
			panic(e)
		}
	} else {
		stks = getd.StocksDbByCode(codes...)
		idxlst, e = getd.GetIdxLst(codes...)
		if e != nil {
			panic(e)
		}
	}
	for _, s := range stks {
		item := new(Item)
		item.Code = s.Code
		item.Name = s.Name
		items = append(items, item)
	}
	for _, idx := range idxlst {
		item := new(Item)
		item.Code = idx.Code
		item.Name = idx.Name
		items = append(items, item)
	}
	pl := 2
	switch conf.Args.RunMode {
	case conf.LOCAL:
		pl = int(float64(runtime.NumCPU()) * 0.7)
	case conf.AUTO:
		rs, h := rpc.AvailableRpcServers(true)
		logr.Debugf("available rpc servers: %d, %.2f%%", rs, h*100)
		if rs > 0 {
			pl = rs
		} else {
			pl = int(float64(runtime.NumCPU()) * 0.7)
		}
	default:
		pl = conf.Args.Concurrency
	}
	logr.Debugf("Parallel Level: %d", pl)
	var wg sync.WaitGroup
	chitm := make(chan *Item, len(items))
	for i := 0; i < pl; i++ {
		wg.Add(1)
		go scoreKdjRoutine(&wg, chitm, len(items))
	}
	for _, itm := range items {
		r.AddItem(itm)
		chitm <- itm
	}
	close(chitm)
	wg.Wait()
	r.SetFields(k.Id(), k.Fields()...)
	if ranked {
		r.Sort()
	}
	r.Shrink(limit)
	return
}

func (k *KdjV) RenewStats(useRaw bool, code ... string) {
	var (
		codes   []string
		stks    []*model.Stock
		idxlst  []*model.IdxLst
		pl      int
		wg, wgr sync.WaitGroup
		e       error
	)
	if code == nil || len(code) == 0 {
		stks = getd.StocksDb()
		idxlst, e = getd.GetIdxLst()
		if e != nil {
			panic(e)
		}
	} else {
		stks = getd.StocksDbByCode(code...)
		idxlst, e = getd.GetIdxLst(code...)
		if e != nil {
			panic(e)
		}
	}
	for _, s := range stks {
		codes = append(codes, s.Code)
	}
	for _, idx := range idxlst {
		codes = append(codes, idx.Code)
	}
	pl = getParallelLevel()
	logr.Debugf("Parallel Level: %d", pl)
	logr.Debugf("#Stocks: %d", len(codes))
	chcde := make(chan string, pl)
	chkps := make(chan *model.KDJVStat, JOB_CAPACITY)
	wgr.Add(1)
	go func(wgr *sync.WaitGroup) {
		defer wgr.Done()
		c := 0
		for kps := range chkps {
			c++
			if kps != nil {
				saveKps(kps)
			}
			logr.Debugf("KDJ stats renew progress: %d/%d, %.2f%%",
				c, len(codes), 100*float64(c)/float64(len(codes)))
		}
	}(&wgr)
	for i, c := range codes {
		wg.Add(1)
		chcde <- c
		go renewKdjStats(c, useRaw, &wg, chcde, chkps)
		if i < pl {
			time.Sleep(time.Millisecond * 500)
		}
	}
	close(chcde)
	wg.Wait()
	close(chkps)
	wgr.Wait()
}

func getParallelLevel() (pl int) {
	switch conf.Args.RunMode {
	case conf.LOCAL:
		pl = int(float64(runtime.NumCPU()) * 0.7)
	case conf.AUTO:
		rs, h := rpc.AvailableRpcServers(true)
		logr.Debugf("available rpc servers: %d, %.2f%%", rs, h*100)
		if rs > 0 {
			pl = int(float64(conf.Args.Concurrency) * h)
		} else {
			pl = int(float64(runtime.NumCPU()) * 0.7)
		}
	default:
		pl = conf.Args.Concurrency
	}
	return
}

func (k *KdjV) SyncKdjFeatDat() bool {
	st := time.Now()
	logr.Debug("Getting all kdj feature data...")
	fdMap, count := getd.GetAllKdjFeatDat()
	var suc bool
	//e := util.RpcCall(global.RPC_SERVER_ADDRESS, "IndcScorer.InitKdjFeatDat", fdMap, &suc)
	es := rpc.RpcPub("DataSync.SyncKdjFd", fdMap, &suc, 3)
	if es != nil && len(es) > 0 {
		logr.Debugf("%d KDJ feature data synchronization failed. time: %.2f", count, time.Since(st).Seconds())
		for _, e := range es {
			logr.Error(e)
		}
		return false
	} else {
		logr.Debugf("%d KDJ feature data has been publish to remote rpc server. time: %.2f",
			count, time.Since(st).Seconds())
		return true
	}
}

func saveKps(kps ... *model.KDJVStat) {
	if kps != nil && len(kps) > 0 {
		valueStrings := make([]string, 0, len(kps))
		valueArgs := make([]interface{}, 0, len(kps)*16)
		for _, k := range kps {
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, k.Code)
			valueArgs = append(valueArgs, k.Dod)
			valueArgs = append(valueArgs, k.Sl)
			valueArgs = append(valueArgs, k.Sh)
			valueArgs = append(valueArgs, k.Bl)
			valueArgs = append(valueArgs, k.Bh)
			valueArgs = append(valueArgs, k.Sor)
			valueArgs = append(valueArgs, k.Bor)
			valueArgs = append(valueArgs, k.Scnt)
			valueArgs = append(valueArgs, k.Bcnt)
			valueArgs = append(valueArgs, k.Smean)
			valueArgs = append(valueArgs, k.Bmean)
			valueArgs = append(valueArgs, k.Frmdt)
			valueArgs = append(valueArgs, k.Todt)
			valueArgs = append(valueArgs, k.Udate)
			valueArgs = append(valueArgs, k.Utime)
		}
		stmt := fmt.Sprintf("INSERT INTO kdjv_stats (code,dod,sl,sh,bl,bh,sor,bor,scnt,bcnt,smean,bmean,"+
			"frmdt,todt,udate,utime) VALUES %s on duplicate key update "+
			"dod=values(dod),sl=values(sl),"+
			"sh=values(sh),bl=values(bl),bh=values(bh),"+
			"sor=values(sor),bor=values(bor),scnt=values(scnt),bcnt=values(bcnt),smean=values(smean),"+
			"bmean=values(bmean),"+
			"frmdt=values(frmdt),todt=values(todt),udate=values(udate),utime=values(utime)",
			strings.Join(valueStrings, ","))
		_, err := dbmap.Exec(stmt, valueArgs...)
		util.CheckErr(err, "failed to bulk update kdjv_stats")
		logr.Debugf("%d kdjv_stats updated", len(kps))
	}
}

// collect kdjv stats and save to database
func renewKdjStats(code string, useRaw bool, wg *sync.WaitGroup, chcde chan string,
	chkps chan *model.KDJVStat) {
	defer func() {
		wg.Done()
		<-chcde
	}()
	start := time.Now()
	var e error
	expvr := 5.0
	mxrt := 2.0
	mxhold := 3
	retro := 600
	kps := new(model.KDJVStat)
	klhist := getd.GetKlineDb(code, model.KLINE_DAY, retro, false)
	if len(klhist) < retro {
		log.Printf("%s insufficient data to collect kdjv stats: %d", code, len(klhist))
		chkps <- nil
		return
	}
	kps.Code = code
	kps.Frmdt = klhist[0].Date
	kps.Todt = klhist[len(klhist)-1].Date
	kps.Udate, kps.Utime = util.TimeStr()
	var buys, sells []float64
	switch conf.Args.RunMode {
	case conf.REMOTE:
		buys, sells, e = kdjScoresRemote(code, klhist, expvr, mxrt, mxhold)
	case conf.LOCAL:
		buys, sells, e = kdjScoresLocal(code, klhist, expvr, mxrt, mxhold, useRaw)
	case conf.AUTO:
		buys, sells, e = kdjScoresAuto(code, klhist, expvr, mxrt, mxhold, useRaw)
	default:
		buys, sells, e = kdjScoresLocal(code, klhist, expvr, mxrt, mxhold, useRaw)
	}
	if e != nil {
		logr.Warn(e)
		return
	}
	sort.Float64s(buys)
	sort.Float64s(sells)
	kps.Bl, e = stats.Round(buys[0], 2)
	util.CheckErr(e, fmt.Sprintf("%s failed to round BL %f", code, buys[0]))
	kps.Sl, e = stats.Round(sells[0], 2)
	util.CheckErr(e, fmt.Sprintf("%s failed to round SL %f", code, sells[0]))
	kps.Bh, e = stats.Round(buys[len(buys)-1], 2)
	util.CheckErr(e, fmt.Sprintf("%s failed to round BH %f", code, buys[len(buys)-1]))
	kps.Sh, e = stats.Round(sells[len(sells)-1], 2)
	util.CheckErr(e, fmt.Sprintf("%s failed to round SH %f", code, sells[len(sells)-1]))
	kps.Bcnt = len(buys)
	kps.Scnt = len(sells)
	kps.Bmean, e = stats.Mean(buys)
	util.CheckErr(e, code+" failed to calculate mean for buy scores")
	kps.Bmean, e = stats.Round(kps.Bmean, 2)
	util.CheckErr(e, fmt.Sprintf("%s failed to round BMean %f", code, kps.Bmean))
	kps.Smean, e = stats.Mean(sells)
	util.CheckErr(e, code+" failed to calculate mean for sell scores")
	kps.Smean, e = stats.Round(kps.Smean, 2)
	util.CheckErr(e, fmt.Sprintf("%s failed to round SMean %f", code, kps.Smean))
	if kps.Sh >= kps.Bl {
		soc, boc := 0, 0
		for _, b := range buys {
			if b <= kps.Sh {
				boc++
			} else {
				break
			}
		}
		for i := len(sells) - 1; i >= 0; i-- {
			s := sells[i]
			if s >= kps.Bl {
				soc++
			} else {
				break
			}
		}
		kps.Bor, e = stats.Round(float64(boc)/float64(kps.Bcnt), 2)
		util.CheckErr(e, fmt.Sprintf("%s failed to round BOR %f", code, kps.Bor))
		kps.Sor, e = stats.Round(float64(soc)/float64(kps.Scnt), 2)
		util.CheckErr(e, fmt.Sprintf("%s failed to round SOR %f", code, kps.Sor))
		dor := math.Abs(kps.Bor - kps.Sor)
		dod := 0.
		x := 0.
		//TODO assess dod more fairly, based on standard deviation?
		if kps.Bor >= kps.Sor {
			x = kps.Bor
		} else {
			//dod = 100 * (-1 + math.Pow(dor+1, 2))
			x = kps.Sor
		}
		dod = 100 * (1 - math.Pow(dor-1, 2))
		dod += 100 * math.Max(0, 1-math.E*math.Pi*math.Pow(x, math.Pi/2.))
		dod = math.Min(100, dod)
		kps.Dod, e = stats.Round(dod, 2)
		util.CheckErr(e, fmt.Sprintf("failed to round DOD: %f", dod))
	} else {
		kps.Dod = 100
	}
	logr.Debugf("%s kdjv DOD: %.2f, time: %.2f", code, kps.Dod, time.Since(start).Seconds())
	chkps <- kps
}

func kdjScoresAuto(code string, klhist []*model.Quote, expvr, mxrt float64, mxhold int, useRaw bool) (
	buys, sells []float64, e error) {
	ars, _ := rpc.AvailableRpcServers(false)
	if ars == 0 {
		logr.Debugf("%s: no available rpc servers, use local power", code)
		buys, sells, e = kdjScoresLocal(code, klhist, expvr, mxrt, mxhold, useRaw)
		return
	}
	cpu, e := util.CpuUsage()
	if e != nil {
		logr.Warnf("%s failed to get cpu usage: %+v", code, e)
	}
	if cpu < conf.Args.CpuUsageThreshold && e == nil {
		logr.Debugf("%s current %%cpu: %.2f use local power", code, cpu)
		buys, sells, e = kdjScoresLocal(code, klhist, expvr, mxrt, mxhold, useRaw)
	} else {
		logr.Debugf("%s current %%cpu: %.2f using remote service", code, cpu)
		buys, sells, e = kdjScoresRemote(code, klhist, expvr, mxrt, mxhold)
		if e != nil {
			//try one more time with local power
			logr.Warnf("remote processing failed, retry with local power\n%+v", e)
			buys, sells, e = kdjScoresLocal(code, klhist, expvr, mxrt, mxhold, useRaw)
		}
	}
	return
}

func kdjScoresLocal(code string, klhist []*model.Quote, expvr, mxrt float64, mxhold int, useRaw bool) (
	buys, sells []float64, e error) {
	st := time.Now()
	buys = getKdjBuyScores(code, klhist, expvr, mxrt, mxhold, useRaw)
	dur := time.Since(st).Seconds()
	logr.Debugf("%s buy points: %d, time: %.2f, %.2f/p", code, len(buys), dur, dur/float64(len(buys)))
	st = time.Now()
	sells = getKdjSellScores(code, klhist, expvr, mxrt, mxhold, useRaw)
	dur = time.Since(st).Seconds()
	logr.Debugf("%s sell points: %d, time: %.2f, %.2f/p", code, len(sells), dur, dur/float64(len(sells)))
	return
}

func kdjScoresRemote(code string, klhist []*model.Quote, expvr, mxrt float64, mxhold int) (
	buys, sells []float64, e error) {
	st := time.Now()
	logr.Debugf("%s connecting rpc server for kdj score calculation...", code)
	_, buys, _, e = fetchKdjScores(getKdjBuySeries(code, klhist, expvr, mxrt, mxhold))
	if e != nil {
		return buys, sells, errors.Wrapf(e, "%s failed to fetch kdj buy scores.", code)
	}
	dur := time.Since(st).Seconds()
	logr.Debugf("%s buy points: %d, time: %.2f, %.2f/p", code, len(buys), dur, dur/float64(len(buys)))
	st = time.Now()
	_, sells, _, e = fetchKdjScores(getKdjSellSeries(code, klhist, expvr, mxrt, mxhold))
	if e != nil {
		return buys, sells, errors.Wrapf(e, "%s failed to fetch kdj sell scores.", code)
	}
	dur = time.Since(st).Seconds()
	logr.Debugf("%s sell points: %d, time: %.2f, %.2f/p", code, len(sells), dur, dur/float64(len(sells)))
	return
}

func fetchKdjScores(s []*rm.KdjSeries) (rowIds []string, scores []float64, details []map[string]interface{}, e error) {
	req := &rm.KdjScoreReq{s, WEIGHT_KDJV_DAY, WEIGHT_KDJV_WEEK, WEIGHT_KDJV_MONTH}
	var rep *rm.KdjScoreRep
	e = rpc.RpcCall("IndcScorer.ScoreKdj", req, &rep, 3)
	if e != nil {
		log.Printf("RPC service IndcScorer.ScoreKdj failed\n%+v", e)
		return nil, nil, nil, e
	} else if len(rep.Scores) != len(rep.RowIds) {
		return nil, nil, nil, errors.Errorf("len of Scores[%d] does not match len of RowIds[%d]",
			len(rep.Scores), len(rep.RowIds))
	} else {
		rowids := make([]string, len(s))
		for i := 0; i < len(s); i++ {
			rowids[i] = s[i].RowId
		}
		equal, rrid, srid := util.DiffStrings(rep.RowIds, rowids)
		if !equal {
			return nil, nil, nil, errors.Errorf("Scores[%d] does not match KdjSeries[%d]:%+v, %+v",
				len(rep.Scores), len(s), rrid, srid)
		}
	}
	return rep.RowIds, rep.Scores, rep.Detail, nil
}

// collect kdjv buy samples
func getKdjBuySeries(code string, klhist []*model.Quote, expvr, mxrt float64,
	mxhold int) (s []*rm.KdjSeries) {
	for i := 1; i < len(klhist)-1; i++ {
		kl := klhist[i]
		sc := kl.Close
		if sc >= klhist[i+1].Close {
			continue
		}
		hc := math.Inf(-1)
		tspan := 0
		pc := klhist[i-1].Close
		for w, j := 0, 0; i+j < len(klhist); j++ {
			nc := klhist[i+j].Close
			if nc > hc {
				hc = nc
				tspan = j
			}
			if pc >= nc {
				rt := (hc - nc) / math.Abs(hc) * 100
				if rt >= mxrt || w > mxhold {
					break
				}
				if j > 0 {
					w++
				}
			} else {
				w = 0
			}
			pc = nc
		}
		if sc == 0 {
			sc = 0.01
			hc += 0.01
		}
		mark := (hc - sc) / math.Abs(sc) * 100
		if mark >= expvr {
			ks := new(rm.KdjSeries)
			s = append(s, ks)
			ks.KdjDy = getd.ToLstJDCross(getd.GetKdjHist(code, model.INDICATOR_DAY, 100, kl.Date))
			ks.KdjWk = getd.ToLstJDCross(getd.GetKdjHist(code, model.INDICATOR_WEEK, 100, kl.Date))
			ks.KdjMo = getd.ToLstJDCross(getd.GetKdjHist(code, model.INDICATOR_MONTH, 100, kl.Date))
			ks.RowId = fmt.Sprintf("BUY-%d-%d-%d-%s", len(ks.KdjDy), len(ks.KdjWk), len(ks.KdjMo), uuid.NewV1())
		}
		i += tspan
	}
	logr.Debugf("%s kdj buy series: %d", code, len(s))
	return s
}

// collect kdjv sell samples
func getKdjSellSeries(code string, klhist []*model.Quote, expvr, mxrt float64,
	mxhold int) (s []*rm.KdjSeries) {
	for i := 1; i < len(klhist)-1; i++ {
		kl := klhist[i]
		sc := kl.Close
		if sc <= klhist[i+1].Close {
			continue
		}
		lc := math.Inf(0)
		tspan := 0
		pc := klhist[i-1].Close
		for w, j := 0, 0; i+j < len(klhist); j++ {
			nc := klhist[i+j].Close
			if nc < lc {
				lc = nc
				tspan = j
			}
			if pc <= nc {
				rt := (nc - lc) / math.Abs(lc) * 100
				if rt >= mxrt || w > mxhold {
					break
				}
				if j > 0 {
					w++
				}
			} else {
				w = 0
			}
			pc = nc
		}
		if sc == 0 {
			sc = -0.01
			lc -= 0.01
		}
		mark := (lc - sc) / math.Abs(sc) * 100
		if mark <= -expvr {
			ks := new(rm.KdjSeries)
			s = append(s, ks)
			ks.KdjMo = getd.ToLstJDCross(getd.GetKdjHist(code, model.INDICATOR_MONTH, 100, kl.Date))
			ks.KdjWk = getd.ToLstJDCross(getd.GetKdjHist(code, model.INDICATOR_WEEK, 100, kl.Date))
			ks.KdjDy = getd.ToLstJDCross(getd.GetKdjHist(code, model.INDICATOR_DAY, 100, kl.Date))
			ks.RowId = fmt.Sprintf("SELL-%d-%d-%d-%s", len(ks.KdjDy), len(ks.KdjWk), len(ks.KdjMo), uuid.NewV1())
		}
		i += tspan
	}
	logr.Debugf("%s kdj sell series: %d", code, len(s))
	return s
}

// collect kdjv buy stats
func getKdjBuyScores(code string, klhist []*model.Quote, expvr, mxrt float64,
	mxhold int, useRawData bool) (s []float64) {
	for i := 1; i < len(klhist)-1; i++ {
		kl := klhist[i]
		sc := kl.Close
		if sc >= klhist[i+1].Close {
			continue
		}
		hc := math.Inf(-1)
		tspan := 0
		pc := klhist[i-1].Close
		for w, j := 0, 0; i+j < len(klhist); j++ {
			nc := klhist[i+j].Close
			if nc > hc {
				hc = nc
				tspan = j
			}
			if pc >= nc {
				rt := (hc - nc) / math.Abs(hc) * 100
				if rt >= mxrt || w > mxhold {
					break
				}
				if j > 0 {
					w++
				}
			} else {
				w = 0
			}
			pc = nc
		}
		if sc == 0 {
			sc = 0.01
			hc += 0.01
		}
		mark := (hc - sc) / math.Abs(sc) * 100
		if mark >= expvr {
			histmo := getd.ToLstJDCross(getd.GetKdjHist(code, model.INDICATOR_MONTH, 100, kl.Date))
			histwk := getd.ToLstJDCross(getd.GetKdjHist(code, model.INDICATOR_WEEK, 100, kl.Date))
			histdy := getd.ToLstJDCross(getd.GetKdjHist(code, model.INDICATOR_DAY, 100, kl.Date))
			if useRawData {
				s = append(s, wgtKdjScoreRaw(nil, histmo, histwk, histdy))
			} else {
				s = append(s, wgtKdjScore(nil, histmo, histwk, histdy))
			}
		}
		i += tspan
	}
	return s
}

// collect kdjv sell stats
func getKdjSellScores(code string, klhist []*model.Quote, expvr, mxrt float64,
	mxhold int, useRawData bool) (s []float64) {
	for i := 1; i < len(klhist)-1; i++ {
		kl := klhist[i]
		sc := kl.Close
		if sc <= klhist[i+1].Close {
			continue
		}
		lc := math.Inf(0)
		tspan := 0
		pc := klhist[i-1].Close
		for w, j := 0, 0; i+j < len(klhist); j++ {
			nc := klhist[i+j].Close
			if nc < lc {
				lc = nc
				tspan = j
			}
			if pc <= nc {
				rt := (nc - lc) / math.Abs(lc) * 100
				if rt >= mxrt || w > mxhold {
					break
				}
				if j > 0 {
					w++
				}
			} else {
				w = 0
			}
			pc = nc
		}
		if sc == 0 {
			sc = -0.01
			lc -= 0.01
		}
		mark := (lc - sc) / math.Abs(sc) * 100
		if mark <= -expvr {
			histmo := getd.ToLstJDCross(getd.GetKdjHist(code, model.INDICATOR_MONTH, 100, kl.Date))
			histwk := getd.ToLstJDCross(getd.GetKdjHist(code, model.INDICATOR_WEEK, 100, kl.Date))
			histdy := getd.ToLstJDCross(getd.GetKdjHist(code, model.INDICATOR_DAY, 100, kl.Date))
			if useRawData {
				s = append(s, wgtKdjScoreRaw(nil, histmo, histwk, histdy))
			} else {
				s = append(s, wgtKdjScore(nil, histmo, histwk, histdy))
			}
		}
		i += tspan
	}
	return s
}

func scoreKdjRoutine(wg *sync.WaitGroup, chitm chan *Item, total int) {
	defer wg.Done()
	ars, _ := rpc.AvailableRpcServers(false)
	if ars == 0 {
		logr.Warn("no available rpc servers, use local power")
		for item := range chitm {
			scoreKdjLocal(item)
		}
	} else {
		//calculate buffer size based on available rpc servers and total
		bufSize := int(math.Ceil(float64(total) / float64(ars)))
		iBuf := make([]*Item, 0, bufSize)
		for item := range chitm {
			iBuf = append(iBuf, item)
			if len(iBuf) >= bufSize {
				// buffer is full, fire to remote server
				e := scoreKdjRemote(iBuf)
				if e != nil {
					// fall back to local power
					logr.Warnf("remote processing failed, retry with local power\n%+v", e)
					for _, bitm := range iBuf {
						scoreKdjLocal(bitm)
					}
				}
				iBuf = nil
			}
		}
		// process remaining items in iBuf
		if len(iBuf) > 0 {
			e := scoreKdjRemote(iBuf)
			if e != nil {
				// fall back to local power
				logr.Warnf("remote processing failed, fall back to local power\n%+v", e)
				for _, bitm := range iBuf {
					scoreKdjLocal(bitm)
				}
			}
			iBuf = nil
		}
	}
	return
}

func scoreKdjRemote(items []*Item) (e error) {
	start := time.Now()
	itmMap := make(map[string]*Item)
	var pid string
	ks := make([]*rm.KdjSeries, len(items))
	for i, item := range items {
		kdjv := new(KdjV)
		pid = kdjv.Id()
		kdjv.Code = item.Code
		kdjv.Name = item.Name
		item.Profiles = make(map[string]*Profile)
		ip := new(Profile)
		item.Profiles[pid] = ip
		ip.FieldHolder = kdjv

		k := new(rm.KdjSeries)
		k.RowId = fmt.Sprintf("%s:%s", item.Code, uuid.NewV1())
		k.KdjDy = getd.ToLstJDCross(getd.GetKdjHist(item.Code, model.INDICATOR_DAY, 100, ""))
		k.KdjWk = getd.ToLstJDCross(getd.GetKdjHist(item.Code, model.INDICATOR_WEEK, 100, ""))
		k.KdjMo = getd.ToLstJDCross(getd.GetKdjHist(item.Code, model.INDICATOR_MONTH, 100, ""))
		kdjv.Len = fmt.Sprintf("%d/%d/%d", len(k.KdjDy), len(k.KdjWk), len(k.KdjMo))
		if len(k.KdjDy) == 0 || len(k.KdjWk) == 0 || len(k.KdjMo) == 0 {
			logr.Warnf("%s len(%d,%d,%d) disqualified for kdjv score calculation", item.Code,
				len(k.KdjDy), len(k.KdjWk), len(k.KdjMo))
			continue;
		}
		ks[i] = k
		var stat *model.KDJVStat
		e := dbmap.SelectOne(&stat, "select * from kdjv_stats where code = ?", item.Code)
		if e != nil {
			if "sql: no rows in result set" != e.Error() {
				log.Panicf("%s failed to query kdjv stats\n%+v", item.Code, e)
			}
		} else {
			kdjv.Sfl = stat.Sh
			kdjv.Bmean = stat.Bmean
			kdjv.Smean = stat.Smean
			kdjv.Dod = stat.Dod
		}
		itmMap[k.RowId] = item
	}
	logr.Debugf("ready to call rpc service, input size: %d", len(ks))
	ids, ss, dets, e := fetchKdjScores(ks)
	if e != nil {
		return errors.Wrapf(e, "%d failed to calculate kdj scores", len(items))
	}
	for i, id := range ids {
		ipf := itmMap[id].Profiles[pid]
		itmMap[id].Score += ss[i]
		ipf.Score = ss[i]
		//get hdr pdr etc from remote service
		kdjv := ipf.FieldHolder.(*KdjV)
		d := dets[i]
		kdjv.CCDY = fmt.Sprintf("%.2f/%.2f/%.2f/%.2f\n%.2f/%.2f/%.2f/%.2f\n",
			d["D.bhdr"], d["D.bpdr"], d["D.bmpd"], d["D.bdi"], d["D.shdr"], d["D.spdr"], d["D.smpd"], d["D.sdi"])
		kdjv.CCWK = fmt.Sprintf("%.2f/%.2f/%.2f/%.2f\n%.2f/%.2f/%.2f/%.2f\n",
			d["W.bhdr"], d["W.bpdr"], d["W.bmpd"], d["W.bdi"], d["W.shdr"], d["W.spdr"], d["W.smpd"], d["W.sdi"])
		kdjv.CCMO = fmt.Sprintf("%.2f/%.2f/%.2f/%.2f\n%.2f/%.2f/%.2f/%.2f\n",
			d["M.bhdr"], d["M.bpdr"], d["M.bmpd"], d["M.bdi"], d["M.shdr"], d["M.spdr"], d["M.smpd"], d["M.sdi"])
	}
	tt := time.Since(start).Seconds()
	logr.Debugf("%d kdj scores calculated using rpc service, time: %.2f, %.2f/stk",
		len(items), tt, tt/float64(len(items)))
	return nil
}

func scoreKdjLocal(item *Item) {
	start := time.Now()
	logr.Debugf("calculating %s...", item.Code)
	kdjv := new(KdjV)
	kdjv.Code = item.Code
	kdjv.Name = item.Name
	item.Profiles = make(map[string]*Profile)
	ip := new(Profile)
	item.Profiles[kdjv.Id()] = ip
	ip.FieldHolder = kdjv

	histmo := getd.ToLstJDCross(getd.GetKdjHist(item.Code, model.INDICATOR_MONTH, 100, ""))
	histwk := getd.ToLstJDCross(getd.GetKdjHist(item.Code, model.INDICATOR_WEEK, 100, ""))
	histdy := getd.ToLstJDCross(getd.GetKdjHist(item.Code, model.INDICATOR_DAY, 100, ""))
	kdjv.Len = fmt.Sprintf("%d/%d/%d", len(histdy), len(histwk), len(histmo))

	//warn if...

	//ip.Score = wgtKdjScoreRaw(kdjv, histmo, histwk, histdy)
	ip.Score = wgtKdjScore(kdjv, histmo, histwk, histdy)
	item.Score += ip.Score

	var stat *model.KDJVStat
	e := dbmap.SelectOne(&stat, "select * from kdjv_stats where code = ?", item.Code)
	if e != nil {
		if "sql: no rows in result set" != e.Error() {
			log.Panicf("%s failed to query kdjv stats\n%+v", item.Code, e)
		}
	} else {
		kdjv.Sfl = stat.Sh
		kdjv.Bmean = stat.Bmean
		kdjv.Smean = stat.Smean
		kdjv.Dod = stat.Dod
	}

	logr.Debugf("%s %s kdjv: %.2f, time: %.2f", item.Code, item.Name, ip.Score, time.Since(start).Seconds())
}

func wgtKdjScoreRaw(kdjv *KdjV, histmo, histwk, histdy []*model.Indicator) (s float64) {
	s += scoreKdjRaw(kdjv, model.MONTH, histmo) * WEIGHT_KDJV_MONTH
	s += scoreKdjRaw(kdjv, model.WEEK, histwk) * WEIGHT_KDJV_WEEK
	s += scoreKdjRaw(kdjv, model.DAY, histdy) * WEIGHT_KDJV_DAY
	s /= WEIGHT_KDJV_MONTH + WEIGHT_KDJV_WEEK + WEIGHT_KDJV_DAY
	s = math.Min(100, math.Max(0, s))
	return
}

func wgtKdjScore(kdjv *KdjV, histmo, histwk, histdy []*model.Indicator) (s float64) {
	s += scoreKdj(kdjv, model.MONTH, histmo) * WEIGHT_KDJV_MONTH
	s += scoreKdj(kdjv, model.WEEK, histwk) * WEIGHT_KDJV_WEEK
	s += scoreKdj(kdjv, model.DAY, histdy) * WEIGHT_KDJV_DAY
	s /= WEIGHT_KDJV_MONTH + WEIGHT_KDJV_WEEK + WEIGHT_KDJV_DAY
	s = math.Min(100, math.Max(0, s))
	return
}

func wgtKdjScoreRpc(kdjv *KdjV, histmo, histwk, histdy []*model.Indicator) (s float64) {
	s += scoreKdj(kdjv, model.MONTH, histmo) * WEIGHT_KDJV_MONTH
	s += scoreKdj(kdjv, model.WEEK, histwk) * WEIGHT_KDJV_WEEK
	s += scoreKdj(kdjv, model.DAY, histdy) * WEIGHT_KDJV_DAY
	s /= WEIGHT_KDJV_MONTH + WEIGHT_KDJV_WEEK + WEIGHT_KDJV_DAY
	s = math.Min(100, math.Max(0, s))
	return
}

//Score by assessing the historical data against pruned kdj feature data.
func scoreKdj(v *KdjV, cytp model.CYTP, kdjhist []*model.Indicator) (s float64) {
	var val string
	byfds, slfds := getKDJfdViews(cytp, len(kdjhist))
	hdr, pdr, mpd, bdi := calcKdjDI(kdjhist, byfds)
	val = fmt.Sprintf("%.2f/%.2f/%.2f/%.2f\n", hdr, pdr, mpd, bdi)
	hdr, pdr, mpd, sdi := calcKdjDI(kdjhist, slfds)
	val += fmt.Sprintf("%.2f/%.2f/%.2f/%.2f\n", hdr, pdr, mpd, sdi)
	dirat := .0
	s = .0
	if sdi == 0 {
		dirat = bdi
	} else {
		dirat = (bdi - sdi) / math.Abs(sdi)
	}
	if dirat > 0 && dirat < 0.995 {
		s = 30 * (0.0015 + 3.3609*dirat - 4.3302*math.Pow(dirat, 2.) + 2.5115*math.Pow(dirat, 3.) -
			0.5449*math.Pow(dirat, 4.))
	} else if dirat >= 0.995 {
		s = 30
	}
	if bdi > 0.201 && bdi < 0.81 {
		s += 70 * (0.0283 - 1.8257*bdi + 10.4231*math.Pow(bdi, 2.) - 10.8682*math.Pow(bdi, 3.) + 3.2234*math.Pow(bdi, 4.))
	} else if bdi >= 0.81 {
		s += 70
	}
	if v != nil {
		switch cytp {
		case model.DAY:
			v.CCDY = val
		case model.WEEK:
			v.CCWK = val
		case model.MONTH:
			v.CCMO = val
		default:
			log.Panicf("unsupported cytp: %s", cytp)
		}
	}
	return
}

//Score by assessing the historical data against raw kdj feature data.
func scoreKdjRaw(v *KdjV, cytp model.CYTP, kdjhist []*model.Indicator) (s float64) {
	var val string
	byhist, slhist := getKDJfdrViews(cytp, len(kdjhist))
	hdr, pdr, mpd, bdi := calcKdjDIRaw(kdjhist, byhist)
	val = fmt.Sprintf("%.2f/%.2f/%.2f/%.2f\n", hdr, pdr, mpd, bdi)
	hdr, pdr, mpd, sdi := calcKdjDIRaw(kdjhist, slhist)
	val += fmt.Sprintf("%.2f/%.2f/%.2f/%.2f\n", hdr, pdr, mpd, sdi)
	dirat := .0
	s = .0
	if sdi == 0 {
		dirat = bdi
	} else {
		dirat = (bdi - sdi) / math.Abs(sdi)
	}
	if dirat > 0 && dirat < 0.995 {
		s = 30 * (0.0015 + 3.3609*dirat - 4.3302*math.Pow(dirat, 2.) + 2.5115*math.Pow(dirat, 3.) -
			0.5449*math.Pow(dirat, 4.))
	} else if dirat >= 0.995 {
		s = 30
	}
	if bdi > 0.201 && bdi < 0.81 {
		s += 70 * (0.0283 - 1.8257*bdi + 10.4231*math.Pow(bdi, 2.) - 10.8682*math.Pow(bdi, 3.) + 3.2234*math.Pow(bdi, 4.))
	} else if bdi >= 0.81 {
		s += 70
	}
	if v != nil {
		switch cytp {
		case model.DAY:
			v.CCDY = val
		case model.WEEK:
			v.CCWK = val
		case model.MONTH:
			v.CCMO = val
		default:
			log.Panicf("unsupported cytp: %s", cytp)
		}
	}
	return
}

func getKDJfdrViews(cytp model.CYTP, len int) (buy, sell []*model.KDJfdrView) {
	buy = make([]*model.KDJfdrView, 0, 1024)
	sell = make([]*model.KDJfdrView, 0, 1024)
	for i := -2; i < 3; i++ {
		n := len + i
		if n >= 2 {
			buy = append(buy, getd.GetKdjFeatDatRaw(cytp, true, n)...)
			sell = append(sell, getd.GetKdjFeatDatRaw(cytp, false, n)...)
		}
	}
	return
}

func getKDJfdViews(cytp model.CYTP, len int) (buy, sell []*model.KDJfdView) {
	buy = make([]*model.KDJfdView, 0, 1024)
	sell = make([]*model.KDJfdView, 0, 1024)
	for i := -2; i < 3; i++ {
		n := len + i
		if n >= 2 {
			buy = append(buy, getd.GetKdjFeatDat(cytp, true, n)...)
			sell = append(sell, getd.GetKdjFeatDat(cytp, false, n)...)
		}
	}
	return
}

// Evaluates KDJ DEVIA indicator against raw feature data, returns the following result:
// Ratio of high DEVIA, ratio of positive DEVIA, mean of positive DEVIA, and DEVIA indicator, ranging from 0 to 1
func calcKdjDIRaw(hist []*model.Indicator, fdvs []*model.KDJfdrView) (hdr, pdr, mpd, di float64) {
	if len(hist) == 0 {
		return 0, 0, 0, 0
	}
	hk := make([]float64, len(hist))
	hd := make([]float64, len(hist))
	hj := make([]float64, len(hist))
	code := hist[0].Code
	for i, h := range hist {
		hk[i] = h.KDJ_K
		hd[i] = h.KDJ_D
		hj[i] = h.KDJ_J
	}
	pds := make([]float64, 0, 16)
	hdc := .0
	for _, fd := range fdvs {
		//skip the identical
		if code == fd.Code && hist[0].Klid == fd.Klid[0] {
			continue
		}
		mod := 1.0
		tsmp, e := time.Parse("2006-01-02", fd.SmpDate)
		util.CheckErr(e, "failed to parse sample date: "+fd.SmpDate)
		days := time.Now().Sub(tsmp).Hours() / 24.0
		if days > 800 {
			mod = math.Max(0.8, -0.0003*math.Pow(days-800, 1.0002)+1)
		}
		bkd := bestKdjDevi(hk, hd, hj, fd.K, fd.D, fd.J) * mod
		if bkd >= 0 {
			pds = append(pds, bkd)
			if bkd >= 0.8 {
				hdc++
			}
		}
	}
	total := float64(len(fdvs))
	pdr = float64(len(pds)) / total
	hdr = hdc / total
	var e error
	if len(pds) > 0 {
		mpd, e = stats.Mean(pds)
		util.CheckErr(e, code+" failed to calculate mean of devia")
	}
	di = 0.5 * math.Min(1, math.Pow(hdr+0.92, 50))
	di += 0.3 * math.Min(1, math.Pow(math.Log(pdr+1), 0.37)+0.4*math.Pow(pdr, math.Pi)+math.Pow(pdr, 0.476145))
	di += 0.2 * math.Min(1, math.Pow(math.Log(math.Pow(mpd, math.E*math.Pi/1.1)+1), 0.06)+
		math.E/1.25/math.Pi*math.Pow(mpd, math.E*math.Pi))
	return
}

// Evaluates KDJ DEVIA indicator against pruned feature data, returns the following result:
// Ratio of high DEVIA, ratio of positive DEVIA, mean of positive DEVIA, and DEVIA indicator, ranging from 0 to 1
func calcKdjDI(hist []*model.Indicator, fdvs []*model.KDJfdView) (hdr, pdr, mpd, di float64) {
	if len(hist) == 0 {
		return 0, 0, 0, 0
	}
	code := hist[0].Code
	hk := make([]float64, len(hist))
	hd := make([]float64, len(hist))
	hj := make([]float64, len(hist))
	for i, h := range hist {
		hk[i] = h.KDJ_K
		hd[i] = h.KDJ_D
		hj[i] = h.KDJ_J
	}
	pds := make([]float64, 0, 16)
	for _, fd := range fdvs {
		bkd := bestKdjDevi(hk, hd, hj, fd.K, fd.D, fd.J)
		if bkd >= 0 {
			pds = append(pds, bkd)
			pdr += fd.Weight
			if bkd >= 0.8 {
				hdr += fd.Weight
			}
		}
	}
	var e error
	if len(pds) > 0 {
		mpd, e = stats.Mean(pds)
		util.CheckErr(e, code+" failed to calculate mean of positive devia")
	}
	di = 0.5 * math.Min(1, math.Pow(hdr+0.92, 50))
	di += 0.3 * math.Min(1, math.Pow(math.Log(pdr+1), 0.37)+0.4*math.Pow(pdr, math.Pi)+math.Pow(pdr, 0.476145))
	di += 0.2 * math.Min(1, math.Pow(math.Log(math.Pow(mpd, math.E*math.Pi/1.1)+1), 0.06)+
		math.E/1.25/math.Pi*math.Pow(mpd, math.E*math.Pi))
	return
}

// Calculates the best match KDJ DEVIA, len(sk)==len(sd)==len(sj),
// and len(sk) and len(tk) can vary.
// DEVIA ranges from negative infinite to 1, with 1 indicating the most relevant KDJ data sets.
func bestKdjDevi(sk, sd, sj, tk, td, tj []float64) float64 {
	//should we also consider the len(x) to weigh the final result?
	dif := len(sk) - len(tk)
	if dif > 0 {
		cc := -100.0
		for i := 0; i <= dif; i++ {
			e := len(sk) - dif + i
			tcc := getd.CalcKdjDevi(sk[i:e], sd[i:e], sj[i:e], tk, td, tj)
			if tcc > cc {
				cc = tcc
			}
		}
		return cc
	} else if dif < 0 {
		cc := -100.0
		dif *= -1
		for i := 0; i <= dif; i++ {
			e := len(tk) - dif + i
			tcc := getd.CalcKdjDevi(sk, sd, sj, tk[i:e], td[i:e], tj[i:e])
			if tcc > cc {
				cc = tcc
			}
		}
		return cc
	} else {
		return getd.CalcKdjDevi(sk, sd, sj, tk, td, tj)
	}
}

func extractKdjFd(fds []*model.KDJfdRaw) (k, d, j []float64) {
	for _, f := range fds {
		k = append(k, f.K)
		d = append(d, f.D)
		j = append(j, f.J)
	}
	return
}

func (k *KdjV) Id() string {
	return "KDJV"
}

func (k *KdjV) Fields() []string {
	return []string{"DOD", "SFL", "BMEAN", "SMEAN", "LEN", "KDJ_DY", "KDJ_WK", "KDJ_MO"}
}

func (k *KdjV) Description() string {
	panic("implement me")
}

func (k *KdjV) Geta() (r *Result) {
	return k.Get(nil, -1, false)
}
