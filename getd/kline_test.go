package getd

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"testing"

	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
)

func TestParseLastJson(t *testing.T) {
	code := "600242"
	//get last kline data
	url_last := fmt.Sprintf("http://d.10jqka.com.cn/v2/line/hs_%s/01/last.js", code)
	body, e := util.HttpGetBytes(url_last)
	if e != nil {
		t.Error(e)
	}
	klast := model.Klast{}
	e = json.Unmarshal(strip(body), &klast)
	if e != nil {
		t.Fatalf("body:\n%+v\n%+v", string(body), e)
	}

	if klast.Data == "" {
		log.Printf("%s last data may not be ready yet", code)
		return
	}
	log.Printf("%+v", klast.Year)
	for k := range klast.Year {
		log.Printf("%s : %d", k, klast.Year[k])
	}
	log.Printf("%+v", klast.Year["hello"])
}

func TestGetKlines(t *testing.T) {
	s := &model.Stock{}
	s.Code = "000042"
	s.Name = "中洲控股"
	s.Market = sql.NullString{String: "SZ", Valid: true}
	ss := new(model.Stocks)
	ss.Add(s)
	GetKlines(ss, model.KLINE_DAY_NR)
	// model.KLINE_DAY,
	// 		model.KLINE_WEEK, model.KLINE_MONTH,
	// 		model.KLINE_MONTH_NR, model.KLINE_WEEK_NR
	t.Fail()
}

func TestGetKlinesFromWht(t *testing.T) {
	s := &model.Stock{}
	s.Code = "000585"
	s.Name = "东北电气"
	s.Market = sql.NullString{String: "SZ", Valid: true}
	getKlineWht(s, []model.DBTab{model.KLINE_DAY, model.KLINE_DAY_NR}, true)
	// model.KLINE_DAY,
	// 		model.KLINE_WEEK, model.KLINE_MONTH,
	// 		model.KLINE_MONTH_NR, model.KLINE_WEEK_NR
	t.Fail()
}

func TestKlineDayNRFromWht(t *testing.T) {
	stks := StocksDb()
	for _, s := range stks {
		getKlineWht(s, []model.DBTab{model.KLINE_DAY_NR}, true)
	}
	t.Fail()
}

func TestGetVldKline(t *testing.T) {
	//603999
	s := &model.Stock{}
	s.Code = "603999"
	s.Name = "读者传媒"
	s.Market = sql.NullString{String: "SH", Valid: true}
	ss := new(model.Stocks)
	ss.Add(s)
	GetKlines(ss, model.KLINE_DAY_VLD)
	// model.KLINE_DAY,
	// 		model.KLINE_WEEK, model.KLINE_MONTH,
	// 		model.KLINE_MONTH_NR, model.KLINE_WEEK_NR
	t.Fail()
}
