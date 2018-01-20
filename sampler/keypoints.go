package sampler

import (
	"database/sql"
	"fmt"

	"github.com/sirupsen/logrus"

	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/model"
)

var dbmap = global.Dbmap

//KeyPoints sample key points against daily kline of the specified code.
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
	query := fmt.Sprintf("select * from kline_d where code = ? %s order by klid", qryKlid)
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

// Principal grading policy:
// Evaluate regional performance on a 10 day basis
// Accumulate Fall (af) ∈ [-15, 0], ζ(af) ∈ [-10, 3]; ζ(-5)=0;
// Regional Rise (rr) ∈ [0, 30], φ(rr) ∈ [-3, 7]; φ(3)=0;
// Final Grade Ψ(af,rr) = max(-10, ζ(af) + φ(rr))
// Both ζ(af) and φ(rr) resemble the shape "S" in the coordinate,
// Hence given the name "Double S" grader function.
var dsGrader = func(code string, klhist []*model.Quote) (kpts []*model.KeyPoint, err error) {
	frame := 10
	if len(klhist) < frame {
		return
	}
refLoop:
	for refIdx, refQt := range klhist {
		if refIdx >= len(klhist)-frame {
			return
		}
		//TODO grade it!
		af := .0
		rr := .0
		for i := refIdx + 1; i < refIdx+frame; i++ {
			cmpQt := klhist[i]
			if !cmpQt.Varate.Valid {
				logrus.Warnf("%s nil varate, skip this point [%d, %s]",
					code, cmpQt.Klid, cmpQt.Date)
				continue refLoop
			}
			if cmpQt.Varate.Float64 < 0 {
				af += cmpQt.Varate.Float64
			}
			if i == refIdx+frame-1 && cmpQt.Close > 0 {
				refClose := .0
				if refQt.Close == 0 {

				}
				rr = cmpQt.Close - refQt.Close
			}
		}
	}
}
