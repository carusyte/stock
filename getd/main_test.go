package getd

import (
	"testing"
	"time"
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
