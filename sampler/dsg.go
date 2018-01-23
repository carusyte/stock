package sampler

import (
	"fmt"
	"log"
	"math"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/getd"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
)

// Principal grading policy:
// Evaluate regional performance on a 10 day basis
// Accumulate Fall (af) ∈ [-15, 0], ζ(af) ∈ [-10, 3]; ζ(-5)=0;
// Regional Rise (rr) ∈ [0, 30], φ(rr) ∈ [-3, 7]; φ(3)=0;
// Final Grade Ψ(af,rr) = max(-10, ζ(af) + φ(rr))
// Both ζ(af) and φ(rr) resemble the shape "S" in the coordinate,
// Hence given the name "Double S" grader function.
var dsGrader = func(code string, klhist []*model.Quote) (kpts []*model.KeyPoint, err error) {
	frame := conf.Args.Sampler.KeyPointEvalFrame
	if len(klhist) < frame {
		log.Printf("%s insufficient data for key point sampling: %d, minimum %d required",
			code, len(klhist), frame)
		return
	}
	xmap, err := getd.XdxrDateBetween(code, klhist[0].Date, klhist[len(klhist)-1].Date)
	if err != nil {
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
		psi := math.Max(-10, zeta(af)+phi(rr))
		d, t := util.TimeStr()
		kp := &model.KeyPoint{
			UUID:     fmt.Sprintf("%s", uuid.NewV1()),
			Code:     code,
			Klid:     refQt.Klid,
			Date:     refQt.Date,
			Score:    psi,
			RgnRise:  rr,
			RgnLen:   frame,
			UnitRise: rr / float64(frame),
			Udate:    d,
			Utime:    t,
		}
		kpts = append(kpts, kp)
	}
	return
}

func zeta(af float64) float64 {
	if af <= -15 {
		return -10
	} else if -15 < af && af <= -5 {
		return math.Max(-10, 1./(-0.0285*(af+2.31))-13)
	} else if -5 < af && af <= 0 {
		return math.Min(3, math.Log(3.876*(af+5.28)))
	}
	log.Printf("error, invalid parameter af: %f, returning 0", af)
	return 0
}

func phi(rr float64) float64 {
	if rr < 30 {
		x := rr - 3.
		//1.75 * sinh^-1(rr - 3)
		return math.Max(-3, 1.75*math.Log(x+math.Sqrt(1.+math.Pow(x, 2))))
	}
	return 7.
}

func calcAFRR(code string, base float64, klhist []*model.Quote,
	start, frame int, xmap map[string]*model.Xdxr) (af, rr float64, ok bool) {
	var x *model.Xdxr
	for i := start + 1; i < start+frame; i++ {
		cmpQt := klhist[i]
		if !cmpQt.VarateRgl.Valid {
			logrus.Warnf("%s nil varate_rgl, skip this point [%d, %s]",
				code, cmpQt.Klid, cmpQt.Date)
			return af, rr, false
		}
		if cmpQt.VarateRgl.Float64 < 0 {
			af += cmpQt.VarateRgl.Float64
		}
		checkXdxr(cmpQt, x, xmap)
		if i == start+frame-1 {
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

func checkXdxr(q *model.Quote, x *model.Xdxr, xmap map[string]*model.Xdxr) bool {
	if xe, ok := xmap[q.Date]; ok {
		if x == nil {
			x = xe
		} else {
			// merge xdxr event data into x
			if xe.Divi.Valid {
				x.Divi.Valid = true
				x.Divi.Float64 += xe.Divi.Float64
			}
			if xe.SharesAllot.Valid {
				x.SharesAllot.Valid = true
				x.SharesAllot.Float64 += xe.SharesAllot.Float64
			}
			if xe.SharesCvt.Valid {
				x.SharesCvt.Valid = true
				x.SharesCvt.Float64 += xe.SharesCvt.Float64
			}
		}
		return true
	}
	return false
}
