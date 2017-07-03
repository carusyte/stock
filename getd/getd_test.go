package getd

import (
	"testing"
	"github.com/carusyte/stock/model"
)

func TestFinMark(t *testing.T) {
	s := &model.Stock{}
	s.Code = "601377"
	s.Name = "兴业证券"
	ss := new(model.Stocks)
	ss.Add(s)
	finMark(ss)
}

