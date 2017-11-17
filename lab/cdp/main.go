package main

import (
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/getd"
)

func main() {
	s := &model.Stock{}
	s.Code = "000626"
	s.Name = "远大控股"
	ss := new(model.Stocks)
	ss.Add(s)
	getd.GetKlines(ss, model.KLINE_DAY_NR, model.KLINE_DAY, model.KLINE_WEEK, model.KLINE_MONTH)
}
