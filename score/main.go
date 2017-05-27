package main

import (
	"github.com/carusyte/stock/db"
	"github.com/carusyte/stock/util"
	"gopkg.in/gorp.v2"
	"io"
	"log"
	"os"
)

const MAX_CONCURRENCY = 200
const JOB_CAPACITY = 512
const LOGFILE = "score.log"

var dbmap *gorp.DbMap

func init() {
	if _, err := os.Stat(LOGFILE); err == nil {
		os.Remove(LOGFILE)
	}
	logFile, err := os.OpenFile(LOGFILE, os.O_CREATE|os.O_RDWR, 0666)
	util.CheckErr(err, "failed to open log file")
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)
	dbmap = db.Get(true, false)
}

//TODO implement scoring
func main() {
	if len(os.Args) < 2 {
		log.Println("scorer is required")
		os.Exit(1)
	}
}
