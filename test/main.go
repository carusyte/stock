package main

import (
	"time"
	"github.com/carusyte/stock/score"
	"log"
	"github.com/carusyte/stock/getd"
	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/util"
)

func main() {
	//logr.SetLevel(logr.DebugLevel)
	//getData()
	//pruneKdjFd(true)
	//kdjFirst()
	//holistic()
	//BLUE
	//blue()
	//kdjOnly()
	renewKdjStats(false)
}

func pruneKdjFd(resume bool) {
	getd.PruneKdjFeatDat(getd.KDJ_FD_PRUNE_PREC, getd.KDJ_FD_PRUNE_PASS, resume)
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

func holistic() {
	start := time.Now()
	//r1 := new(score.HiD).Geta()
	//r1.Weight = 0.1
	r2 := new(score.BlueChip).Geta().Sort().Shrink(500)
	r2.Weight = 0
	//r1r2 := score.Combine(r1, r2)
	//r1r2.Weight = 0
	r3 := new(score.KdjV).Get(r2.Stocks(), -1, false)
	r3.Weight = 1
	log.Printf("\n%+v", score.Combine(r2, r3).Sort())
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

func kdjOnly(code ... string) {
	start := time.Now()
	r1 := new(score.KdjV).Get(code, -1, true)
	log.Printf("\n%+v", r1)
	log.Printf("Time Cost: %v", time.Since(start).Seconds())
}

func getData() {
	getd.Get()
}
