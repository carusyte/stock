package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"time"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/sampler"
	"github.com/sirupsen/logrus"

	"github.com/carusyte/stock/getd"
	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/score"
	"github.com/carusyte/stock/util"
)

func main() {
	defer shutdownHook()

	//logr.SetLevel(logr.DebugLevel)

	if conf.Args.Scorer.FetchData {
		getData()
	}
	hidBlueKdjSt()

	//pruneKdjFd(true)
	//kdjFirst()
	//holistic()
	//BLUE
	// blue()
	//blueKdjv()
	//kdjOnly()
	//renewKdjStats(true)
	// test()
	// testSplitAfter()
	// fixVarate()

	// sampleKeyPoints()
}

func shutdownHook() {
	if r := recover(); r != nil {
		if er, hasError := r.(error); hasError {
			log.Printf("caught error:%+v, trying to cleanup...", er)
			getd.Cleanup()
		}
	}
}

func sampleKeyPoints() {
	start := time.Now()
	sampler.SampAllKeyPoints(-1, 120, nil)
	log.Printf("time cost: %.2f", time.Since(start).Seconds())
}

func panicHandler(output string) {
	//clear ChromeDP resource
	getd.Cleanup()
	// output contains the full output (including stack traces) of the
	// panic. Put it in a file or something.
	fmt.Printf("The child panicked:\n\n%s\n", output)
	os.Exit(1)
}

func testSplitAfter() {
	fmt.Println(strings.SplitN("hello_world_n_you", "_", 3))
}

func fixVarate() {
	getd.FixVarate()
	logrus.Info("all varate has been fixed.")
}

func test() {
	// stocks := new(model.Stocks)
	// s := &model.Stock{}
	// s.Code = "000009"
	// s.Name = "中国宝安"
	// stocks.Add(s)
	// getd.GetKlines(stocks,
	// 	model.KLINE_DAY,
	// 	model.KLINE_WEEK,
	// 	model.KLINE_MONTH,
	// 	model.KLINE_MONTH_NR,
	// 	model.KLINE_DAY_NR,
	// 	model.KLINE_WEEK_NR,
	// )
	allstk := getd.StocksDb()
	stocks := new(model.Stocks)
	stocks.Add(allstk...)
	getd.GetKlines(stocks,
		model.KLINE_WEEK,
		model.KLINE_MONTH,
		model.KLINE_DAY,
		model.KLINE_DAY_NR,
		model.KLINE_WEEK_NR,
		model.KLINE_MONTH_NR)
	e := getd.AppendVarateRgl(allstk...)
	if e != nil {
		log.Println(e)
	} else {
		log.Printf("%v stocks varate_rgl fixed", len(allstk))
	}
}

func hidBlueKdjSt() {
	start := time.Now()
	kdjst := new(score.KdjSt)
	idxlst, e := getd.GetIdxLst()
	if e != nil {
		panic(e)
	}
	idxc := make([]string, len(idxlst))
	for i, idx := range idxlst {
		idxc[i] = idx.Code
	}
	c, e := global.Dbmap.SelectInt("select round(count(*) * ? ) from basics",
		conf.Args.Scorer.HidBlueBaseRatio)
	if e != nil {
		log.Println("failed to count from basics")
		log.Println(e)
	}
	r1 := new(score.HiD).Geta()
	r1.Weight = 1. - conf.Args.Scorer.BlueWeight
	r2 := new(score.BlueChip).Geta()
	r2.Weight = conf.Args.Scorer.BlueWeight
	r1r2 := score.Combine(r1, r2).Sort().Shrink(int(c))
	n := int(math.Max(0, math.Floor(float64(c)*conf.Args.Scorer.HidBlueStarRatio)))
	r1r2.Mark(n, score.StarMark)
	n = -int(math.Max(0, math.Floor(float64(c)*conf.Args.Scorer.HidBlueRearWarnRatio)))
	r1r2.Mark(n, score.WarnMark)
	r1r2.Weight = 1. - conf.Args.Scorer.KdjStWeight
	r3 := kdjst.Get(r1r2.Stocks(), -1, false)
	r3.Weight = conf.Args.Scorer.KdjStWeight
	log.Printf("\n%+v", kdjst.Get(idxc, -1, false))
	fmt.Println()
	log.Printf("\n%+v", score.Combine(r1r2, r3).
		Sort().Highlight(conf.Args.Scorer.Highlight...))
	log.Printf("Time Cost: %v", time.Since(start).Seconds())
}

func blueKdjv() {
	start := time.Now()
	r2 := new(score.BlueChip).Geta().Sort().Shrink(1000)
	r2.Weight = 0
	r3 := new(score.KdjV).Get(r2.Stocks(), -1, false)
	r3.Weight = 1
	log.Printf("\n%+v", score.Combine(r2, r3).Sort())
	log.Printf("Time Cost: %v", time.Since(start).Seconds())
}

func pruneKdjFd(resume bool) {
	getd.PruneKdjFeatDat(getd.KDJ_FD_PRUNE_PREC, getd.KDJ_PRUNE_RATE, resume)
}

func renewKdjStats(resume bool) {
	kv := new(score.KdjV)
	if resume {
		sql, e := global.Dot.Raw("KDJV_STATS_UNDONE")
		util.CheckErr(e, "failed to get sql KDJV_STATS_UNDONE")
		var stocks []string
		_, e = global.Dbmap.Select(&stocks, sql)
		kv.RenewStats(false, stocks...)
	} else {
		kv.RenewStats(false)
	}
}

func blue() {
	r := new(score.BlueChip).Get(nil, -1, true)
	log.Printf("\n%+v", r)
}

func empirical() {
	start := time.Now()
	kdjv := new(score.KdjV)
	idxlst, e := getd.GetIdxLst()
	if e != nil {
		panic(e)
	}
	idxc := make([]string, len(idxlst))
	for i, idx := range idxlst {
		idxc[i] = idx.Code
	}
	r1 := new(score.HiD).Geta()
	r1.Weight = 0.5
	r2 := new(score.BlueChip).Geta()
	r2.Weight = 0.5
	r1r2 := score.Combine(r1, r2).Sort().Shrink(300)
	r1r2.Weight = 0
	r3 := kdjv.Get(r1r2.Stocks(), -1, false)
	r3.Weight = 1
	log.Printf("\n%+v", kdjv.Get(idxc, -1, false))
	fmt.Println()
	log.Printf("\n%+v", score.Combine(r1r2, r3).Sort())
	log.Printf("Time Cost: %v", time.Since(start).Seconds())
}

func kdjFirst() {
	start := time.Now()
	r1 := new(score.KdjV).Geta().Sort().Shrink(50)
	r2 := new(score.HiD).Get(r1.Stocks(), -1, false)
	r2.Weight = 0.2
	r3 := new(score.BlueChip).Get(r1.Stocks(), -1, false)
	r3.Weight = 0.8
	log.Printf("\n%+v", score.Combine(r2, r3, r1).Sort())
	log.Printf("Time Cost: %v", time.Since(start).Seconds())
}

func kdjOnly(code ...string) {
	start := time.Now()
	r1 := new(score.KdjV).Get(code, -1, true)
	log.Printf("\n%+v", r1)
	log.Printf("Time Cost: %v", time.Since(start).Seconds())
}

func getData() {
	getd.Get()
}
