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
	r := GetTrDataBtwn(code, qry, Date, dt1, dt2, desc)
	fmt.Println(r)
	t.Fail()
}

func TestTrDataQryCopy(t *testing.T) {
	q := TrDataQry{
		LocalSource: model.KlineMaster,
		Cycle:       model.DAY,
		Reinstate:   model.Backward,
		Basic:       true,
	}
	qs := []TrDataQry{q, q}
	qs[1].LocalSource = model.EM
	qs[1].Cycle = model.WEEK
	qs[1].Reinstate = model.Forward
	qs[1].LogRtn = true
	log.Debugf("qs:%+v", qs)
}

func TestLogFormat(t *testing.T){
	log.Debugf("- [%.2f%%]", 23.456)
	log.Debugf("[%.2f%%]", 23.456)
}

func TestTrDataQryMap(t *testing.T) {
	q := TrDataQry{
		LocalSource: model.KlineMaster,
		Cycle:       model.DAY,
		Reinstate:   model.Backward,
		Basic:       true,
	}
	qs := []TrDataQry{
		q,
		q,
		q,
		TrDataQry{
			LocalSource: model.KlineMaster,
			Cycle:       model.DAY,
			Reinstate:   model.Backward,
			Basic:       true,
		}}
	qs[1].LocalSource = model.EM
	qs[1].Cycle = model.WEEK
	qs[1].Reinstate = model.Forward
	qs[1].LogRtn = true
	qmap := map[TrDataQry]string{
		qs[0]: "qs[0]",
		qs[1]: "qs[1]",
		qs[2]: "qs[2]",
		qs[3]: "qs[3]",
	}
	log.Debugf("qs:%+v", qs)
	log.Debugf("qmap:%+v", qmap)
}
