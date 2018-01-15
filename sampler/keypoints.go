package sampler

import (
	"database/sql"
	"fmt"

	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/model"
)

var dbmap = global.Dbmap

//KeyPoints sample key points against daily kline of the specified code.
// if resample is 0, only sample new key points (existing data will not be resampled).
// if resample is -1, resample all the key points.
// suggested 20 prior, 20 rear.
func KeyPoints(code string, resample, prior, rear int) (kpts []*model.KeyPoint, err error) {
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
	query := fmt.Sprintf("select * from kline_d where code = ? %s order by klid", qryKlid)
	var klhist []*model.Quote
	_, err = dbmap.Select(&klhist, query, code)
	if err != nil && sql.ErrNoRows != err {
		return
	}
	return scanKeyPoints(code, klhist, rear)
}

func scanKeyPoints(code string, klhist []*model.Quote, rear int) (kpts []*model.KeyPoint, err error) {
	for i, k := range klhist {

	}
}
