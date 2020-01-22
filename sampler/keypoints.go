package sampler

import (
	"database/sql"
	"fmt"
	"runtime"
	"strings"
	"sync"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/model"
	"github.com/pkg/errors"
)

var (
	chkpts map[int]chan []*model.KeyPoint
)

//SampAllKeyPoints sample all keypoints using goroutine and save sampled data to kpts table.
func SampAllKeyPoints() (e error) {
	log.Printf("sampling key points...")
	var stks []*model.Stock
	dbmap.Select(&stks, "select * from basics")
	log.Printf("%d stocks loaded from db", len(stks))
	if len(stks) == 0 {
		log.Printf("no stock available, skipping key point sampling")
		return nil
	}

	var wg sync.WaitGroup
	pl := int(float64(runtime.NumCPU()) * 0.8)
	wf := make(chan int, pl)
	fail := make(chan string, global.JobCapacity)
	failstks := make([]string, 0, 16)
	wgr := collect(&failstks, fail)
	chkpts = createDbJobQueues()
	wgdb := goSaveKpts(fail)
	for _, stk := range stks {
		wg.Add(1)
		wf <- 1
		go sampKeyPoints(stk.Code, &wg, &wf, fail)
	}
	wg.Wait()
	close(wf)
	waitKpSave(wgdb)
	close(fail)
	wgr.Wait()
	log.Printf("kpts data saved. %d / %d, failed: %+v", len(stks)-len(failstks), len(stks), failstks)

	frames := conf.Args.Sampler.GraderTimeFrames
	failFrames := graderStats(frames)
	log.Printf("grader stats collected. %d / %d, frames failed: %+v", len(frames)-len(failFrames), len(frames), failFrames)

	return e
}

func waitKpSave(wgs []*sync.WaitGroup) {
	for _, ch := range chkpts {
		close(ch)
	}
	for _, wg := range wgs {
		wg.Wait()
	}
}

func goSaveKpts(fail chan string) (wgs []*sync.WaitGroup) {
	for f, ch := range chkpts {
		wg := new(sync.WaitGroup)
		wgs = append(wgs, wg)
		wg.Add(1)
		go func(f int, wg *sync.WaitGroup, ch chan []*model.KeyPoint, fail chan string) {
			defer wg.Done()
			table := fmt.Sprintf("kpts%d", f)
			for kpts := range ch {
				e := SaveKpts(table, kpts...)
				if e != nil {
					if len(kpts) > 0 {
						fail <- kpts[0].Code
					}
				} else {
					if len(kpts) > 0 {
						log.Printf("%s %d %s saved", kpts[0].Code, len(kpts), table)
					}
				}
			}
		}(f, wg, ch, fail)
	}
	return
}

func createDbJobQueues() (qmap map[int]chan []*model.KeyPoint) {
	qmap = make(map[int]chan []*model.KeyPoint)
	for _, f := range conf.Args.Sampler.GraderTimeFrames {
		qmap[f] = make(chan []*model.KeyPoint, conf.Args.DBQueueCapacity)
	}
	return
}

func graderStats(frames []int) (fail []int) {
	for _, frame := range frames {
		log.Printf("collecting stats for frame %d", frame)
		e := grader.stats(frame)
		if e != nil {
			log.Printf("grader failed to collect stats for frame %d : %+v", frame, e)
			fail = append(fail, frame)
		}
	}
	return
}

func sampKeyPoints(code string, wg *sync.WaitGroup, wf *chan int, fail chan string) {
	defer func() {
		wg.Done()
		<-*wf
	}()
	prior := conf.Args.Sampler.PriorLength
	e := KeyPoints(code, conf.Args.Sampler.Resample, prior)
	if e != nil {
		log.Printf("%s sampling failed, %+v", code, e)
		fail <- code
		return
	}

}

//KeyPoints sample key points against backward-reinstated daily kline of the specified stock.
// if resample is 0, only sample new key points (existing data will not be resampled).
// if resample is -1, resample all the key points.
func KeyPoints(code string, resample, prior int) (err error) {
	frames := conf.Args.Sampler.GraderTimeFrames
	for _, frame := range frames {
		// keep track of latest selected klid;
		var lkp *model.KeyPoint
		if resample == 0 {
			err = dbmap.SelectOne(&lkp,
				fmt.Sprintf(`select klid from kpts%d where code = ? order by klid desc limit 1`, frame), code)
		} else if resample > 0 {
			err = dbmap.SelectOne(&lkp,
				fmt.Sprintf(`select klid from kpts%d where code = ? `+
					`order by klid desc limit 1 offset ?`, frame), code, resample)
		}
		if err != nil && sql.ErrNoRows != err {
			return errors.WithStack(err)
		}
		qryKlid := ""
		if lkp != nil {
			qryKlid = fmt.Sprintf(" and klid > %d", lkp.Klid)
		} else if prior > 0 {
			qryKlid = fmt.Sprintf(" and klid >= %d", prior)
		}
		// use backward reinstated kline
		query, e := global.Dot.Raw("QUERY_BWR_DAILY")
		if e != nil {
			return errors.WithStack(e)
		}
		query = fmt.Sprintf(query, qryKlid)
		var klhist []*model.Quote
		_, err = dbmap.Select(&klhist, query, code)
		if err != nil {
			if sql.ErrNoRows != err {
				err = errors.WithStack(err)
				return
			}
			return nil
		}

		if len(klhist) < frame {
			log.Printf("%s insufficient data for key point sampling: %d, %d required",
				code, len(klhist), frame)
			return nil
		}
		var r []*model.KeyPoint
		r, err = grader.sample(code, frame, klhist)
		if err != nil {
			return
		}
		chkpts[frame] <- r
		log.Printf("%s kpts%d sampled: %d", code, frame, len(r))
	}
	return nil
}

// SaveKpts update existing keypoint data or insert new ones in database.
func SaveKpts(table string, kpts ...*model.KeyPoint) (err error) {
	if len(kpts) == 0 {
		return nil
	}
	retry := 10
	rt := 0
	code := ""
	for ; rt < retry; rt++ {
		code = kpts[0].Code
		valueStrings := make([]string, 0, len(kpts))
		valueArgs := make([]interface{}, 0, len(kpts)*12)
		for _, e := range kpts {
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, e.Code)
			valueArgs = append(valueArgs, e.Date)
			// don't overwrite existing flags
			// valueArgs = append(valueArgs, e.Flag)
			valueArgs = append(valueArgs, e.Klid)
			// valueArgs = append(valueArgs, e.RgnLen)
			valueArgs = append(valueArgs, e.RgnRise)
			valueArgs = append(valueArgs, e.Score)
			valueArgs = append(valueArgs, e.SumFall)
			valueArgs = append(valueArgs, e.UnitRise)
			valueArgs = append(valueArgs, e.Clr)
			valueArgs = append(valueArgs, e.RemaLr)
			valueArgs = append(valueArgs, e.UUID)
			valueArgs = append(valueArgs, e.Udate)
			valueArgs = append(valueArgs, e.Utime)
		}
		stmt := fmt.Sprintf("INSERT INTO %s (code,date,klid,rgn_rise,score,sum_fall,unit_rise,"+
			"clr,rema_lr,uuid,udate,utime) VALUES %s "+
			"on duplicate key update date=values(date),"+
			"rgn_rise=values(rgn_rise),score=values(score),sum_fall=values(sum_fall),unit_rise=values(unit_rise),"+
			"clr=values(clr),rema_lr=values(rema_lr),uuid=values(uuid),udate=values(udate),utime=values(utime)",
			table, strings.Join(valueStrings, ","))
		_, err := global.Dbmap.Exec(stmt, valueArgs...)
		if err != nil {
			fmt.Println(err)
			if strings.Contains(err.Error(), "Deadlock") {
				continue
			} else {
				return errors.Wrap(errors.WithStack(err), code+": failed to bulk update "+table)
			}
		}
		break
	}
	if rt >= retry {
		return errors.Wrap(err, code+": failed to bulk update "+table)
	}
	return nil
}

func collect(stocks *[]string, rstks chan string) (wgr *sync.WaitGroup) {
	wgr = new(sync.WaitGroup)
	wgr.Add(1)
	go func() {
		defer wgr.Done()
		for stk := range rstks {
			*stocks = append(*stocks, stk)
		}
	}()
	return wgr
}
