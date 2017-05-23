package main

import (
	"github.com/carusyte/stock/model"
	"testing"
)

func TestGetFinance(t *testing.T) {
	s := &model.Stock{}
	s.Code = "601088"
	s.Name = "中国神华"
	ss := []*model.Stock{s}
	GetFinance(ss)
}

//test getXDXR individually
func TestGetXDXR(t *testing.T) {
	s := &model.Stock{}
	s.Code = "601088"
	s.Name = "中国神华"
	ss := []*model.Stock{s}
	s = &model.Stock{}
	s.Code = "600104"
	s.Name = "上汽集团"
	ss = append(ss, s)
	GetXDXRs(ss)
}