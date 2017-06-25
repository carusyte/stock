package getd

import (
	"testing"
	"time"
	"github.com/carusyte/stock/model"
	"github.com/montanaflynn/stats"
	"fmt"
)

func TestCalcAllIndcs(t *testing.T) {
	start := time.Now()
	defer stop("GETD_TOTAL", start)
	stks := GetStockInfo()
	stop("STOCK_LIST", start)

	stci := time.Now()
	CalcIndics(stks)
	stop("CALC_INDICS", stci)
}


func TestParseIfengBonus(t *testing.T) {
	s := &model.Stock{}
	s.Code = "000727"
	s.Name = `华东科技`
	ParseIfengBonus(s)
}

func TestGet(t *testing.T) {
	Get()
}

func TestMean(t *testing.T) {
	var data = []float64{1, 2, 3, 4, 4, 5}
	r, _ := stats.Median(data)
	fmt.Println(r)
}