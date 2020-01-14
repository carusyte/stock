package main

import (
	"strings"

	"github.com/carusyte/stock/cmd"
	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"github.com/pkg/profile"

	"github.com/carusyte/stock/getd"
	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/score"
)

var log = global.Log

func main() {
	log.Info("Starting...")
	switch strings.ToLower(conf.Args.Profiling) {
	case "cpu":
		defer profile.Start().Stop()
	case "mem":
		defer profile.Start(profile.MemProfile).Stop()
	}

	cmd.Execute()
}

func fixVarate() {
	getd.FixVarate()
	log.Info("all varate has been fixed.")
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
