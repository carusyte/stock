package global

import (
	"github.com/carusyte/stock/db"
	"github.com/gchaincl/dotsql"
	"gopkg.in/gorp.v2"
	"io"
	"log"
	"os"
	"github.com/carusyte/stock/conf"
	"fmt"
)

var (
	Dbmap *gorp.DbMap
	Dot   *dotsql.DotSql

	//RPC_SERVER_ADDRESS = "localhost:45321"    // for local test
)

const (
	// will make some of the requests via proxy, 0.6 = 3/5
	PART_PROXY = 0
	PROXY_ADDR = "127.0.0.1:1080"

	LOGFILE         = "stock.log"
	MAX_CONCURRENCY = 16
	JOB_CAPACITY    = 512
)

func init() {
	var e error
	if _, e = os.Stat(LOGFILE); e == nil {
		os.Remove(LOGFILE)
	}
	logFile, e := os.OpenFile(LOGFILE, os.O_CREATE|os.O_RDWR, 0666)
	if e != nil {
		log.Panicln("failed to open log file", e)
	}
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)
	Dbmap = db.Get(true, false)
	sqlp := "../sql/sql.txt"
	if conf.Args.SqlFileLocation != "" {
		sqlp = fmt.Sprintf("%s/sql.txt", conf.Args.SqlFileLocation)
	}
	if _, e = os.Stat(sqlp); e != nil {
		pwd, _ := os.Getwd()
		sqlp = pwd + "/sql/sql.txt"
	}
	Dot, e = dotsql.LoadFromFile(sqlp)
	if e != nil {
		log.Panicln("failed to init dotsql", e)
	}
}
