package sampler

import (
	"database/sql"
	"fmt"

	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/model"
)

var dbmap = global.Dbmap

//KeyPoints sample key points against non-reinstated daily kline of the specified stock.
// if resample is 0, only sample new key points (existing data will not be resampled).
// if resample is -1, resample all the key points.
// grader function g(c,k) can be nil, in which case the default "Double S" function will be used.
// suggested 120 prior.
func KeyPoints(code string, resample, prior int,
	g func(code string, klhist []*model.Quote) (kpts []*model.KeyPoint, err error)) (kpts []*model.KeyPoint, err error) {
	// keep track of latest selected klid;
	var lkp *model.KeyPoint
	if resample == 0 {
		err = dbmap.SelectOne(&lkp, `select klid from kpts where code = ? order by klid desc limit 1`, code)
	} else if resample > 0 {
		err = dbmap.SelectOne(&lkp, `select klid from kpts where code = ? `+
			`order by klid desc offset ? limit 1`, code, resample)
	}
	if err != nil && sql.ErrNoRows != err {
		return
	}
	qryKlid := ""
	if lkp != nil {
		qryKlid = fmt.Sprintf(" and klid > %d", lkp.Klid)
	} else if prior > 0 {
		qryKlid = fmt.Sprintf(" and klid >= %d", prior)
	}
	query, err := global.Dot.Raw("QUERY_NR_DAILY")
	if err != nil {
		return kpts, err
	}
	query = fmt.Sprintf(query, qryKlid)
	var klhist []*model.Quote
	_, err = dbmap.Select(&klhist, query, code)
	if err != nil {
		if sql.ErrNoRows != err {
			return
		}
		return kpts, nil
	}
	if g == nil {
		return dsGrader(code, klhist)
	}
	return g(code, klhist)
}
