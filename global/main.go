package global

import (
	"fmt"
	"os"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/db"
	"github.com/gchaincl/dotsql"
	"github.com/sirupsen/logrus"
	"gopkg.in/gorp.v2"
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

	MAX_CONCURRENCY = 16
	JOB_CAPACITY    = 512
)

func init() {
	var e error
	Dbmap = db.Get(true, false)
	sqlp := "../sql/sql.txt"
	if conf.Args.SQLFileLocation != "" {
		sqlp = fmt.Sprintf("%s/sql.txt", conf.Args.SQLFileLocation)
	}
	if _, e = os.Stat(sqlp); e != nil {
		pwd, _ := os.Getwd()
		sqlp = pwd + "/sql/sql.txt"
	}
	Dot, e = dotsql.LoadFromFile(sqlp)
	if e != nil {
		logrus.Panicln("failed to init dotsql", e)
	}
}
