package getd

import (
	"github.com/sirupsen/logrus"
	"testing"
	"github.com/carusyte/stock/model"
)

func TestCalcIndics(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	stks := StocksDb()
	allstk := new(model.Stocks)
	for _, s := range stks {
		allstk.Add(s)
	}
	CalcIndics(allstk)
}