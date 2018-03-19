package sampler

import (
	"database/sql"
	"fmt"
	"log"
	"math"

	"github.com/carusyte/stock/getd"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"github.com/montanaflynn/stats"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
)

// Double Wave grader principal grading policy:
// Evaluate regional performance on a 10 day basis
// Accumulate Fall (af) ∈ (-∞, 0], ζ(af) ∈ [-10, 3]; ζ(-5)=0;
// Regional Rise (rr) ∈ [0, +∞), φ(rr) ∈ [-3, +∞]; φ(3)=0, φ(10)≈7;
// Final Grade Ψ(af,rr) = max(-10, min(10, ζ(af) + φ(rr)))
type dwGrader struct{}

func (g *dwGrader) sample(code string, frame int, klhist []*model.Quote) (kpts []*model.KeyPoint, err error) {
	if len(klhist) < frame {
		log.Printf("%s insufficient data for key point sampling: %d, minimum %d required",
			code, len(klhist), frame)
		return
	}
	xmap, err := getd.XdxrDateBetween(code, klhist[0].Date, klhist[len(klhist)-1].Date)
	if err != nil {
		err = errors.WithStack(err)
		return kpts, err
	}
	for refIdx, refQt := range klhist {
		if refIdx >= len(klhist)-frame {
			return
		}
		af, rr, ok := calcAFRR(code, refQt.Close, klhist, refIdx, frame, xmap)
		if !ok {
			continue
		}
		//grade it
		psi, err := stats.Round(zeta(af)+phi(rr), 0)
		if err != nil {
			err = errors.WithStack(err)
			return kpts, err
		}
		psi = math.Max(-10, math.Min(10, psi))
		d, t := util.TimeStr()
		kp := &model.KeyPoint{
			UUID:     fmt.Sprintf("%s", uuid.NewV1()),
			Code:     code,
			Klid:     refQt.Klid,
			Date:     refQt.Date,
			Score:    psi,
			SumFall:  af,
			RgnRise:  rr,
			RgnLen:   frame,
			UnitRise: rr / float64(frame),
			Udate:    d,
			Utime:    t,
			Flag:     sql.NullString{Valid: false},
		}
		kpts = append(kpts, kp)
	}
	return
}

func (g *dwGrader) stats(frame int) (e error) {
	return nil
}

func zeta(af float64) float64 {
	if af <= -15 {
		return -10
	} else if -15 < af && af <= -5 {
		return math.Max(-10, 1./(-0.0285*(af+2.31))-13)
	} else if -5 < af && af <= 0 {
		// return math.Min(3, math.Log(3.876*(af+5.28)))
		return math.Min(3, 0.15*math.Pow(af+5., math.Log(20.)/math.Log(5.)))
	}
	log.Printf("error, invalid parameter af: %f, returning 0", af)
	return 0
}

func phi(rr float64) float64 {
	if rr <= 0 {
		return 0
	}
	if 0 < rr && rr <= 3 {
		return 3.*math.Log(rr+1)/math.Log(4.) - 3.
	}
	e := math.E
	x := rr - 1.86*e
	a := 0.681 * e
	sqrt := math.Sqrt(1 + math.Pow(x, 2))
	return a*math.Log(x+sqrt) + e // sinh^-1
	// if rr < 30 {
	// 	x := rr - 3.
	// 	//1.75 * sinh^-1(rr - 3)
	// 	return math.Max(-3, 1.75*math.Log(x+math.Sqrt(1.+math.Pow(x, 2))))
	// }
	// return 7.
}

func calcAFRR(code string, base float64, klhist []*model.Quote,
	start, frame int, xmap map[string]*model.Xdxr) (af, rr float64, ok bool) {
	x := getd.MergeXdxrBetween(klhist[start+1].Date, klhist[start+frame].Date, xmap)
	for i := start + 1; i <= start+frame; i++ {
		cmpQt := klhist[i]
		if !cmpQt.VarateRgl.Valid {
			logrus.Warnf("%s nil varate_rgl, skip this point [%d, %s]",
				code, cmpQt.Klid, cmpQt.Date)
			return af, rr, false
		}
		if cmpQt.VarateRgl.Float64 < 0 {
			af += cmpQt.VarateRgl.Float64
		}
		if i == start+frame {
			if cmpQt.Close <= 0 {
				break
			}
			refClose := getd.Reinstate(base, x)
			if refClose == 0 {
				refClose = 0.01
			}
			//cmpQt.Close is non-reinstated hence never 0
			rr = (cmpQt.Close - refClose) / refClose * 100.
		}
	}
	return af, rr, true
}
