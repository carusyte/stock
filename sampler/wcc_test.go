package sampler

import (
	"testing"

	"github.com/carusyte/stock/model"
)

func Test_calWcc(t *testing.T) {
	stocks := new(model.Stocks)
	stocks.Add(&model.Stock{
		Code: "600104",
		Name: "上汽集团",
	})
	CalWcc(stocks)
}
