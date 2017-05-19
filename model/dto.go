package model

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/carusyte/stock/util"
)

type Stock struct {
	Code             string
	Name             string
	Industry         sql.NullString
	Area             sql.NullString
	Pe               sql.NullFloat64
	Outstanding      sql.NullFloat64
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
	Price            sql.NullFloat64
	Varate           sql.NullFloat64
	Var              sql.NullFloat64
	Xrate            sql.NullFloat64
	Volratio         sql.NullFloat64
	Ampl             sql.NullFloat64
	Turnover         sql.NullFloat64
	Accer            sql.NullFloat64
	CircMarVal       sql.NullFloat64
}

func (s *Stock) String() string {
	j, e := json.Marshal(s)
	if e != nil {
		fmt.Println(e)
	}
	return fmt.Sprintf("%v", string(j))
}

type Xdxr struct {
	Code        string
	Name        string
	Index       int
	ReportYear  sql.NullString `db:"report_year"`
	BoardDate   sql.NullString `db:"board_date"`
	Divi        sql.NullFloat64
	Shares      sql.NullFloat64
	GmsDate     sql.NullString `db:"gms_date"`
	ImplDate    sql.NullString `db:"impl_date"`
	Plan        sql.NullString
	RecordDate  sql.NullString `db:"record_date"`
	XdxrDate    sql.NullString `db:"xdxr_date"`
	PayoutDate  sql.NullString `db:"payout_date"`
	Progress    sql.NullString
	PayoutRatio sql.NullFloat64        `db:"payout_ratio"`
	DivRate     sql.NullFloat64        `db:"div_rate"`
}

func (x *Xdxr) String() string {
	j, e := json.Marshal(x)
	if e != nil {
		fmt.Println(e)
	}
	return fmt.Sprintf("%v", string(j))
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
	Xrate  sql.NullFloat64
}

func (q *Quote) String() string {
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

type Klast struct {
	//Rt string `json:"rt"`
	Num int `json:"num"`
	//Total int `json:"total"`
	Start string `json:"start"`
	//Year map[string]int `json:"year"`
	Name string `json:"name"`
	Khist
	//IssuePrice *float32 `json:"issuePrice,string"`
}

type Khist struct {
	Data string `json:"data"`
}

type Ktoday struct {
	Quote
}

func (kt *Ktoday) UnmarshalJSON(b []byte) error {
	var f interface{}
	json.Unmarshal(b, &f)

	m := f.(map[string]interface{})

	for k := range m {
		qm := m[k].(map[string]interface{})
		kt.Code = k[3:]
		kt.Date = qm["1"].(string)
		kt.Date = kt.Date[:4] + "-" + kt.Date[4:6] + "-" + kt.Date[6:]
		kt.Open = util.Str2F64(qm["7"].(string))
		kt.High = util.Str2F64(qm["8"].(string))
		kt.Low = util.Str2F64(qm["9"].(string))
		kt.Close = util.Str2F64(qm["11"].(string))
		kt.Volume = qm["13"].(float64)
		kt.Amount = util.Str2F64(qm["19"].(string))
		kt.Xrate = sql.NullFloat64{util.Str2F64(qm["1968584"].(string)), true}
	}

	return nil
}
