package main

import (
	"github.com/carusyte/stock/db"
	"github.com/carusyte/stock/util"
	"gopkg.in/gorp.v2"
	"io"
	"log"
	"os"
)

var (
	dbmap *gorp.DbMap
)

func init() {
	logFile, err := os.OpenFile("calk.log", os.O_CREATE|os.O_RDWR, 0666)
	util.CheckErr(err, "failed to open log file")
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)
	dbmap = db.Get(true, false)
}

func main() {
	stks := GetStockList()
	GetKlines(stks)
}
