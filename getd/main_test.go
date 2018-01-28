package getd

import (
	"fmt"
	"testing"
	"time"

	"github.com/carusyte/stock/model"
	"github.com/montanaflynn/stats"
)

func TestCalcAllIndcs(t *testing.T) {
	start := time.Now()
	defer StopWatch("GETD_TOTAL", start)
	stk := StocksDb()
	stks := new(model.Stocks)
	stks.Add(stk...)

	stci := time.Now()
	CalcIndics(stks)
	StopWatch("CALC_INDICS", stci)
}

func TestCalcIndcs(t *testing.T) {
	s := &model.Stock{}
	s.Code = "603313"
	s.Name = "梦百合"
	stks := &model.Stocks{}
	stks.Add(s)
	CalcIndics(stks)
}

func TestParseIfengBonus(t *testing.T) {
	s := &model.Stock{}
	s.Code = "000727"
	s.Name = `华东科技`
	ParseIfengBonus(s)
}

func TestGet(t *testing.T) {
	// For better performance, if you want to update local data only
	// please use test/main_test.go instead
	Get()
}

func TestMean(t *testing.T) {
	var data = []float64{1, 2, 3, 4, 4, 5}
	r, _ := stats.Median(data)
	fmt.Println(r)
}

func TestAnalyzeKdjCC(t *testing.T) {
	//smpKdjFeat("600104", model.MONTH, 5.0, 2.0, 2, 5, 600)
	SmpKdjFeat("600048", model.MONTH, 5.0, 2.0, 2)
	SmpKdjFeat("600048", model.WEEK, 5.0, 2.0, 2)
	SmpKdjFeat("600048", model.DAY, 5.0, 2.0, 2)
}

func TestCalcDay(t *testing.T) {
	//calcDay("600104", 3)
}

func TestEpoch(t *testing.T) {
	epoch := time.Now().UnixNano() / int64(time.Millisecond)
	fmt.Println(epoch)
}
