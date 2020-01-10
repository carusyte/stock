package sampler

import (
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"sync"

	"github.com/carusyte/stock/indc"
	"github.com/sirupsen/logrus"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

const graderRemaLr = "RemaLr"

// remalrGrader is a Log Return Grader with reversal EMA evaluation. Principal grading policy:
// Evaluate regional performance on respective day-span basis
// Score according to the close price log return ema calculated from the far-most future to the nearest.
type remaLrGrader struct {
	gstats sync.Map //map[int][]*model.GraderStats
}

func (g *remaLrGrader) sample(code string, frame int, klhist []*model.Quote) (kpts []*model.KeyPoint, err error) {
	for refIdx, refQt := range klhist {
		if refIdx >= len(klhist)-frame {
			return
		}
		remalr := remaLr(code, klhist, refIdx, frame)
		uuid := fmt.Sprintf("%s", uuid.Must(uuid.NewV1()))
		s, err := g.score(uuid, remalr, frame)
		if err != nil {
			return nil, err
		}
		// xmap, err := getd.XdxrDateBetween(code, klhist[0].Date, klhist[len(klhist)-1].Date)
		// if err != nil {
		// 	err = errors.WithStack(err)
		// 	return kpts, err
		// }
		af, rr, ok := calcAFRR(code, refQt.Close, klhist, refIdx, frame, nil)
		if !ok {
			continue
		}
		d, t := util.TimeStr()
		kp := &model.KeyPoint{
			UUID:     uuid,
			Code:     code,
			Klid:     refQt.Klid,
			Date:     refQt.Date,
			Score:    s,
			SumFall:  af,
			RgnRise:  rr,
			UnitRise: rr / float64(frame),
			RemaLr:   sql.NullFloat64{Float64: remalr, Valid: true},
			Udate:    d,
			Utime:    t,
		}
		kpts = append(kpts, kp)
	}
	return
}

func (g *remaLrGrader) stats(frame int) (e error) {
	//check if stats present
	gs, e := dbmap.Select(&model.GraderStats{}, "select * from grader_stats where grader = ? and frame = ?", graderRemaLr, frame)
	if e != nil && e != sql.ErrNoRows {
		return errors.WithStack(e)
	}
	if len(gs) > 0 && !conf.Args.Sampler.RefreshGraderStats {
		return nil
	}
	query := fmt.Sprintf("select count(*) from kpts%d", frame)
	total, e := dbmap.SelectInt(query)
	if e != nil {
		return errors.WithStack(e)
	}
	nclass := conf.Args.Sampler.GraderScoreClass
	baseQuota := int(total) / nclass
	quotas := make([]int, nclass)
	for i := range quotas {
		quotas[i] = baseQuota
	}
	rmd := int(total) % nclass
	if rmd > 0 {
		perm := rand.Perm(nclass)
		for i := 0; i < rmd; i++ {
			c := perm[i]
			quotas[c] = quotas[c] + 1
		}
	}
	var kpts *model.KeyPoint
	stats := make([]*model.GraderStats, nclass)
	prev := 0.
	cur := 0.
	uuid := ""
	logrus.Printf("score quotas for frame %d: %+v", frame, quotas)
	for i, q := range quotas {
		s := i - nclass/2
		ud, ut := util.TimeStr()
		stats[i] = &model.GraderStats{
			Grader: graderRemaLr,
			Frame:  frame,
			Score:  float64(s),
			Size:   q,
			Udate:  sql.NullString{String: ud, Valid: true},
			Utime:  sql.NullString{String: ut, Valid: true},
		}
		if i == 0 {
			sql := fmt.Sprintf("select * from kpts%d order by rema_lr, uuid limit 1 offset ?", frame)
			e = dbmap.SelectOne(&kpts, sql, q-1)
			cur = kpts.RemaLr.Float64
		} else if i < len(quotas)-1 {
			sql := fmt.Sprintf("select * from kpts%d "+
				"where rema_lr > ? or (rema_lr = ? and uuid > ?) "+
				"order by rema_lr, uuid limit 1 offset ?", frame)
			e = dbmap.SelectOne(&kpts, sql, prev, prev, uuid, q-1)
			cur = kpts.RemaLr.Float64
		}
		if e != nil {
			return errors.WithStack(e)
		}
		usql := ""
		if i == 0 {
			logrus.Printf("kpts%d score:[%d] threshold:[%f] uuid:[%s]", frame, s, cur, kpts.UUID)
			usql = fmt.Sprintf("update kpts%d set score = ? where rema_lr < ? or (rema_lr = ? and uuid <= ?)", frame)
			_, e = dbmap.Exec(usql, s, cur, cur, kpts.UUID)
		} else if i < len(quotas)-1 {
			logrus.Printf("kpts%d score:[%d] threshold:[%f] uuid:[%s]", frame, s, cur, kpts.UUID)
			usql = fmt.Sprintf("update kpts%d set score = ? "+
				"where (rema_lr > ? or (rema_lr = ? and uuid > ?)) "+
				"and (rema_lr < ? or (rema_lr = ? and uuid <= ?))", frame)
			_, e = dbmap.Exec(usql, s, prev, prev, uuid, cur, cur, kpts.UUID)
		} else {
			logrus.Printf("kpts%d score:[%d] threshold:[> %f] uuid:[> %s]", frame, s, prev, uuid)
			usql = fmt.Sprintf("update kpts%d set score = ? where rema_lr > ? or (rema_lr = ? and uuid > ?)", frame)
			_, e = dbmap.Exec(usql, s, prev, prev, uuid)
		}
		if e != nil {
			return errors.Wrapf(errors.WithStack(e), "sql failed: %s", usql)
		}
		if i < len(quotas)-1 {
			prev = cur
			uuid = kpts.UUID
			stats[i].Threshold = sql.NullFloat64{Float64: cur, Valid: true}
			stats[i].UUID = sql.NullString{String: uuid, Valid: true}
		}
	}
	_, e = dbmap.Exec("delete from grader_stats where grader = ? and frame = ?", graderRemaLr, frame)
	if e != nil {
		return errors.WithStack(e)
	}
	for _, gs := range stats {
		e = dbmap.Insert(gs)
		if e != nil {
			return errors.WithStack(e)
		}
	}
	return nil
}

func (g *remaLrGrader) score(uuid string, remalr float64, frame int) (s float64, e error) {
	//load stats if needed
	var stats []*model.GraderStats
	if istats, ok := g.gstats.Load(frame); ok {
		stats = istats.([]*model.GraderStats)
	} else {
		stats = make([]*model.GraderStats, 0, 16)
		_, e := dbmap.Select(&stats, "select * from grader_stats where grader = ? and frame = ? order by score",
			graderRemaLr, frame)
		if e != nil && e != sql.ErrNoRows {
			return s, errors.WithStack(e)
		}
		g.gstats.Store(frame, stats)
	}
	if len(stats) == 0 {
		return 0, nil
	}
	// scoring
	for i, gs := range stats {
		if i < len(stats)-1 &&
			(remalr < gs.Threshold.Float64 || (remalr == gs.Threshold.Float64 && uuid <= gs.UUID.String)) {
			return gs.Score, nil
		} else if i == len(stats)-1 {
			lgs := stats[i-1]
			if remalr > lgs.Threshold.Float64 || (remalr == lgs.Threshold.Float64 && uuid > lgs.UUID.String) {
				return gs.Score, nil
			}
		}
	}
	return s, errors.Errorf("remalr out of grader stats coverage: %f, %s", remalr, uuid)
}

func remaLr(code string, klhist []*model.Quote, start, frame int) (remalr float64) {
	n := math.Max(3, float64(frame)/5.0)
	for i := start + frame; i > start; i-- {
		dn := math.Min(n, float64(start+frame+1-i))
		if k := klhist[i]; k.Lr.Valid {
			remalr = indc.EMA(k.Lr.Float64, remalr, dn)
		}
	}
	return
}
