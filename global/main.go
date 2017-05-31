package global

import (
	"github.com/carusyte/stock/db"
	"github.com/carusyte/stock/util"
	"github.com/gchaincl/dotsql"
	"gopkg.in/gorp.v2"
	"io"
	"log"
	"os"
)

const LOGFILE = "stock.log"
const MAX_CONCURRENCY = 128
const JOB_CAPACITY = 512
// will make some of the requests via proxy, 0.6 = 3/5
const PART_PROXY = 0
const PROXY_ADDR = "127.0.0.1:1080"

var (
	Dbmap *gorp.DbMap
	Dot *dotsql.DotSql
)

func init() {
	var e error
	if _, e = os.Stat(LOGFILE); e == nil {
		os.Remove(LOGFILE)
	}
	logFile, e := os.OpenFile(LOGFILE, os.O_CREATE|os.O_RDWR, 0666)
	util.CheckErr(e, "failed to open log file")
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)
	Dbmap = db.Get(true, false)
	util.PART_PROXY = PART_PROXY
	util.PROXY_ADDR = PROXY_ADDR
	Dot, e = dotsql.LoadFromFile("/Users/jx/ProgramData/go/src/github.com/carusyte/stock/ask/sql.txt")
	util.CheckErr(e, "failed to init dotsql")
}
