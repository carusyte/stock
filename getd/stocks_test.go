package getd

import (
	"math/rand"
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
		Code: "000411",
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

func TestXQShares(t *testing.T) {
	allstk := StocksDb()
	s := allstk[rand.Intn(len(allstk))]
	xqShares(s, nil, nil, nil)
	log.Printf("%+v", s)
	t.Fail()
}
