package sampler

import (
	"fmt"
	"testing"

	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/model"
)

func TestLremaSample(t *testing.T) {
	t.Fail()
	code := "002600"
	qryKlid := ""
	qryKlid = fmt.Sprintf(" and klid >= %d", 1219)
	query, e := global.Dot.Raw("QUERY_NR_DAILY")
	if e != nil {
		panic(e)
	}
	query = fmt.Sprintf(query, qryKlid)
	var klhist []*model.Quote
	_, e = dbmap.Select(&klhist, query, code)
	if e != nil {
		panic(e)
	}

	g := new(remaLrGrader)
	g.sample(code, 5, klhist)

}
