package indc

import (
	"io"
	"os"

	"github.com/carusyte/stock/db"
	"github.com/carusyte/stock/util"
	"gopkg.in/gorp.v2"
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

// func TestKdj(t *testing.T) {
// 	var q []*model.Quote
// 	_, e := dbmap.Select(&q, `select code, date, klid, high,low,close,open,xrate,volume,amount from kline_d where code = '600104' and date >= '2016' order by date asc`)
// 	util.CheckErr(e, "failed")
// 	fmt.Print("High: ")
// 	for _, qe:=range q{
// 		fmt.Printf("%.3f,",qe.High)
// 	}
// 	fmt.Println()
// 	fmt.Print("Low: ")
// 	for _, qe:=range q{
// 		fmt.Printf("%.3f,",qe.Low)
// 	}
// 	fmt.Println()
// 	fmt.Print("Close: ")
// 	for _, qe:=range q{
// 		fmt.Printf("%.3f,",qe.Close)
// 	}
// 	fmt.Println()
// 	i := DeftKDJ(q)
// 	fmt.Print("K: ")
// 	for _, ie := range i{
// 		fmt.Printf("%.3f,",ie.KDJ_K)
// 	}
// 	fmt.Println()

// 	fmt.Print("D: ")
// 	for _, ie := range i{
// 		fmt.Printf("%.3f,",ie.KDJ_D)
// 	}
// 	fmt.Println()

// 	fmt.Print("J: ")
// 	for _, ie := range i{
// 		fmt.Printf("%.3f,",ie.KDJ_J)
// 	}
// 	fmt.Println()
// }
