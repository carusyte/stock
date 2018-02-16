package sampler

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/carusyte/stock/getd"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

// LrGrader is a Log Return Grader. Principal grading policy:
// Evaluate regional performance on a 60 day basis
// Score according to the close price log return.
var LrGrader = func(code string, klhist []*model.Quote) (kpts []*model.KeyPoint, err error) {
	frame := 60
	if len(klhist) < frame {
		log.Printf("%s insufficient data for key point sampling: %d, minimum %d required",
			code, len(klhist), frame)
		return
	}
	for refIdx, refQt := range klhist {
		if refIdx >= len(klhist)-frame {
			return
		}
		clr := calCompoundLogReturn(code, klhist, refIdx, frame)
		s := 0.
		// aim to grade 7 levels from -3 to 3 evenly according to kpts distribution
		if clr < -0.2082507 {
			s = -3
		} else if clr < -0.10761734 {
			s = -2
		} else if clr < -0.03457234 {
			s = -1
		} else if clr < 0.0345492 {
			s = 0
		} else if clr < 0.11655325 {
			s = 1
		} else if clr < 0.24390236 {
			s = 2
		} else {
			s = 3
		}
		xmap, err := getd.XdxrDateBetween(code, klhist[0].Date, klhist[len(klhist)-1].Date)
		if err != nil {
			err = errors.WithStack(err)
			return kpts, err
		}
		af, rr, ok := calcAFRR(code, refQt.Close, klhist, refIdx, frame, xmap)
		if !ok {
			continue
		}
		d, t := util.TimeStr()
		kp := &model.KeyPoint{
			UUID:     fmt.Sprintf("%s", uuid.NewV1()),
			Code:     code,
			Klid:     refQt.Klid,
			Date:     refQt.Date,
			Score:    s,
			SumFall:  af,
			RgnRise:  rr,
			RgnLen:   frame,
			UnitRise: rr / float64(frame),
			Clr:      sql.NullFloat64{Valid: true, Float64: clr},
			Udate:    d,
			Utime:    t,
			Flag:     sql.NullString{Valid: false},
		}
		kpts = append(kpts, kp)
	}
	return
}

func calCompoundLogReturn(code string, klhist []*model.Quote,
	start, frame int) (clr float64) {
	for i := start + 1; i <= start+frame; i++ {
		if k := klhist[i]; k.Lr.Valid {
			clr += k.Lr.Float64
		}
	}
	return
}
