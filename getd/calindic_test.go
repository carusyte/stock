package getd

import (
	"testing"

	"github.com/carusyte/stock/model"
	"github.com/sirupsen/logrus"
)

func TestCalcIndics(t *testing.T) {
	log.SetLevel(logrus.DebugLevel)
	stks := StocksDb()
	allstk := new(model.Stocks)
	for _, s := range stks {
		allstk.Add(s)
	}
	CalcIndics(allstk)
}

func TestCalIndicators4Indices(t *testing.T) {
	log.SetLevel(logrus.DebugLevel)
	stks, e := GetIdxLst()
	if e != nil {
		panic(e)
	}
	allstk := new(model.Stocks)
	for _, s := range stks {
		allstk.Add(&model.Stock{Code: s.Code, Name: s.Name, Source: s.Src})
	}
	CalcIndics(allstk)
}
