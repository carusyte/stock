package getd

import (
	"github.com/carusyte/stock/db"
	"github.com/carusyte/stock/util"
	"gopkg.in/gorp.v2"
	"io"
	"log"
	"os"
)

const MAX_CONCURRENCY = 16
const JOB_CAPACITY = 512
const LOGFILE = "getd.log"

// will make some of the requests via proxy, 0.6 = 3/5
const PART_PROXY = 0
const PROXY_ADDR = "127.0.0.1:1080"

var (
	dbmap *gorp.DbMap
)

func init() {
	if _, err := os.Stat(LOGFILE); err == nil {
		os.Remove(LOGFILE)
	}
	logFile, err := os.OpenFile(LOGFILE, os.O_CREATE|os.O_RDWR, 0666)
	util.CheckErr(err, "failed to open log file")
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)
	dbmap = db.Get(true, false)
	util.PART_PROXY = PART_PROXY
	util.PROXY_ADDR = PROXY_ADDR
}

func main() {
	Get()
}
