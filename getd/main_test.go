package getd

import (
	"testing"
	"time"
	"github.com/carusyte/stock/model"
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