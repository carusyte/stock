package getd

import (
	"fmt"
	"testing"

	"github.com/carusyte/stock/model"
)

func TestGetTrDataBtwn(t *testing.T) {
	code := "600014"
	qry := TrDataQry{
		Cycle:     model.DAY,
		Reinstate: model.Backward,
		Basic:     true,
		LogRtn:    true,
	}
	dt1, dt2 := "[2019-11-30", "2019-12-10]"
	desc := true
	r := GetTrDataBtwn(code, qry, dt1, dt2, desc)
	fmt.Println(r)
	t.Fail()
}
