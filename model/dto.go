package model

import (
	"database/sql"
	"encoding/json"
	"fmt"
)

type Stock struct {
	Code             string
	Name             string
	Industry         sql.NullString
	Area             sql.NullString
	Pe               float32
	Outstanding      float32
	Totals           float32
	TotalAssets      float64
	LiquidAssets     float64
	FixedAssets      float64
	Reserved         float64
	ReservedPerShare float32
	Esp              float32
	Bvps             float32
	Pb               float32
	TimeToMarket     string
	Undp             float64
	Perundp          float32
	Rev              float32
	Profit           float32
	Gpr              float32
	Npr              float32
	Holders          int64
}

type Quote struct {
	Code   string `db:",size:6"`
	Date   string `db:",size:10"`
	Klid   int
	Open   float64
	High   float64
	Close  float64
	Low    float64
	Volume float64
	Amount float64
}

func (q *Quote) String() string{
	j, e := json.Marshal(q)
	if e != nil {
		fmt.Println(e)
	}
	return fmt.Sprintf("%v", string(j))
}

type Kline struct {
	Quote
	Factor sql.NullFloat64
}

type KlineW struct {
	Quote
}

type KlineM struct {
	KlineW
}

type Indicator struct {
	Code  string `db:",size:6"`
	Date  string `db:",size:10"`
	Klid  int
	KDJ_K float64
	KDJ_D float64
	KDJ_J float64
}

type IndicatorW struct {
	Indicator
}

type IndicatorM struct {
	Indicator
}

func (k *KlineW) String() string {
	j, e := json.Marshal(k)
	if e != nil {
		fmt.Println(e)
	}
	return fmt.Sprintf("%v", string(j))
}
