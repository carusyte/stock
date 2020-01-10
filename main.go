package main

import (
	"io"
	"os"
	"strings"

	"github.com/carusyte/stock/cmd"
	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/model"
	"github.com/pkg/profile"
	"github.com/sirupsen/logrus"

	"github.com/carusyte/stock/getd"
	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/score"
	"github.com/carusyte/stock/util"
)

const (
	//LOGFILE the path for the global log file
	LOGFILE = "stock.log"
)

func init() {
	if _, e := os.Stat(LOGFILE); e == nil {
		os.Remove(LOGFILE)
	}
	logFile, e := os.OpenFile(LOGFILE, os.O_CREATE|os.O_RDWR, 0666)
	if e != nil {
		logrus.Panicln("failed to open log file", e)
	}
	mw := io.MultiWriter(os.Stdout, logFile)
	logrus.SetOutput(mw)
}

func main() {
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
		logrus.Println(e)
	} else {
		logrus.Printf("%v stocks varate_rgl fixed", len(allstk))
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
