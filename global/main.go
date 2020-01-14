package global

import (
	"fmt"
	"io"
	"os"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/db"
	"github.com/gchaincl/dotsql"
	"github.com/sirupsen/logrus"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
	"gopkg.in/gorp.v2"
)

var (
	Dbmap *gorp.DbMap
	Dot   *dotsql.DotSql
	Log   = logrus.New()
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
		Log.Panicln("failed to init dotsql", e)
	}

	switch conf.Args.LogLevel {
	case "debug":
		Log.SetLevel(logrus.DebugLevel)
	case "info":
		Log.SetLevel(logrus.InfoLevel)
	case "warning":
		Log.SetLevel(logrus.WarnLevel)
	case "error":
		Log.SetLevel(logrus.ErrorLevel)
	case "fatal":
		Log.SetLevel(logrus.FatalLevel)
	case "panic":
		Log.SetLevel(logrus.PanicLevel)
	}

	Log.SetFormatter(&prefixed.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		FullTimestamp:   true,
		ForceFormatting: true,
		// ForceColors:     true,
	})
	if _, e := os.Stat(conf.Args.LogFile); e == nil {
		os.Remove(conf.Args.LogFile)
	}
	logFile, e := os.OpenFile(conf.Args.LogFile, os.O_CREATE|os.O_RDWR, 0666)
	if e != nil {
		Log.Panicln("failed to open log file", e)
	}
	mw := io.MultiWriter(os.Stdout, logFile)
	Log.SetOutput(mw)
}
