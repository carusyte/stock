package getd

import (
	"encoding/json"
	"fmt"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"log"
	"testing"
)

func TestGetDailyKlines(t *testing.T) {
	getDailyKlines("600242", model.KLINE_DAY, true)
}

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
	s.Code = "601377"
	s.Name = "兴业证券"
	ss := new(model.Stocks)
	ss.Add(s)
	GetKlines(ss, model.KLINE_DAY_NR, model.KLINE_DAY)
}
