package getd

import (
	"reflect"
	"testing"

	"github.com/carusyte/stock/model"
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

func TestGetPerfPrediction(t *testing.T) {
	stk := &model.Stock{}
	stk.Code = "600383"
	stk.Name = "金地集团"
	s := &model.Stocks{}
	s.Add(stk)

	tests := []struct {
		name      string
		args      *model.Stocks
		wantRstks *model.Stocks
	}{
		{
			name:      "normal",
			args:      s,
			wantRstks: s,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotRstks := GetFinPrediction(tt.args); !reflect.DeepEqual(gotRstks, tt.wantRstks) {
				t.Errorf("GetPerfPrediction() = %v, want %v", gotRstks, tt.wantRstks)
			}
		})
	}
}

func TestGetAllPerfPrediction(t *testing.T) {
	stks := StocksDb()
	allstk := new(model.Stocks)
	for _, s := range stks {
		allstk.Add(s)
	}
	GetFinPrediction(allstk)
}

func TestSubSlice(t *testing.T) {
	s := []int{1, 2, 3, 4, 5}
	t.Errorf("slice: %+v", s)
	s = subSlice(s)
	t.Errorf("subslice: %+v", s)
}

func subSlice(s []int) []int {
	s = append(s[:1], s[2:]...)
	return s
}
