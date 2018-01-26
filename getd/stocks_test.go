package getd

import (
	"log"
	"testing"

	"github.com/carusyte/stock/model"
)

func TestGetStockInfo(t *testing.T) {
	GetStockInfo()
}

func TestGetFromExchanges(t *testing.T) {
	allstk := getFromExchanges()
	log.Printf("found stocks: %d", allstk.Size())
}

func TestThsIndustry(t *testing.T) {
	s := &model.Stock{
		Code: "600104",
	}
	thsIndustry(s)
	t.Errorf("%+v", s)
}

func TestThsShares(t *testing.T) {
	allstk := StocksDb()
	for _, s := range allstk {
		thsShares(s)
	}
	t.Fail()
}
