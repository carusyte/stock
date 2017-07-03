package getd

import (
	"github.com/carusyte/stock/model"
	"testing"
)

func TestGetFinance(t *testing.T) {
	s := &model.Stock{}
	s.Code = "000017"
	s.Name = "深中华A"
	ss := new(model.Stocks)
	ss.Add(s)
	GetFinance(ss)
}

//test getXDXR individually
func TestGetXDXR(t *testing.T) {
	ss := new(model.Stocks)
	s := &model.Stock{}
	s.Code = "601088"
	s.Name = "中国神华"
	ss.Add(s)
	s = &model.Stock{}
	s.Code = "601377"
	s.Name = "兴业证券"
	ss.Add(s)
	GetXDXRs(ss)
}