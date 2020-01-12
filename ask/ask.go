package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/carusyte/stock/advisor"
	"github.com/carusyte/stock/db"
	"github.com/carusyte/stock/getd"
	"gopkg.in/gorp.v2"
)

const APP_VERSION = "0.1"
const MAX_CONCURRENCY = 200
const JOB_CAPACITY = 512
const LOGFILE = "ask.log"

var dbmap *gorp.DbMap

var (
	versionFlag *bool   = flag.Bool("v", false, "Print the version number.")
	advisorId   *string = flag.String("a", "", "The Adviser Id.")
	refresh     *bool   = flag.Bool("r", false, "Refresh local data before providing any advice.")
)

func init() {
	// if _, err := os.Stat(LOGFILE); err == nil {
	// 	os.Remove(LOGFILE)
	// }
	// logFile, err := os.OpenFile(LOGFILE, os.O_CREATE|os.O_RDWR, 0666)
	// util.CheckErr(err, "failed to open log file")
	// mw := io.MultiWriter(os.Stdout, logFile)
	// log.SetOutput(mw)
	dbmap = db.Get(true, false)
}

func main() {

	flag.Parse() // Scan the arguments list

	if *versionFlag {
		fmt.Println("Version:", APP_VERSION)
		return
	}
	if advisorId == nil {
		fmt.Println("Advisor Id is needed.")
		return
	}
	if *refresh {
		getd.Get()
	}

	var t *advisor.Table
	avr := advisor.New()
	arg := *advisorId
	switch {
	case strings.EqualFold("HiDivi", arg):
		t = avr.HiDivi(25)

	default:
		os.Exit(1)
	}

	fmt.Printf("%v", t)
}
