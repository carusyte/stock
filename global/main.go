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
const MAX_CONCURRENCY = 16
const JOB_CAPACITY = 512
const RUN_MODE = RPC_SERVICE

// will make some of the requests via proxy, 0.6 = 3/5
const PART_PROXY = 0
const PROXY_ADDR = "127.0.0.1:1080"

var (
	Dbmap *gorp.DbMap
	Dot   *dotsql.DotSql
)

type RunMode string

const(
	LOCAL RunMode= "local"
	RPC_SERVICE RunMode= "rpc"
	DISTRIBUTED RunMode= "distributed"

	RPC_SERVER_ADDRESS = "115.159.237.46:45321"
	//RPC_SERVER_ADDRESS = "localhost:45321"    // for local test
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
	sqlp := "../sql/sql.txt"
	if _, e = os.Stat(sqlp); e != nil {
		pwd, _ := os.Getwd()
		sqlp = pwd + "/sql/sql.txt"
	}
	Dot, e = dotsql.LoadFromFile(sqlp)
	util.CheckErr(e, "failed to init dotsql")
}
