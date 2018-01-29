package sampler

import (
	"database/sql"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"

	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/model"
	"github.com/pkg/errors"
)

var dbmap = global.Dbmap

//SampAllKeyPoints sample all keypoints using goroutine and save sampled data to kpts table.
func SampAllKeyPoints(resample, prior int,
	g func(code string, klhist []*model.Quote) (kpts []*model.KeyPoint, err error)) (e error) {
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
	fail := make(chan string, global.JOB_CAPACITY)
	failstks := make([]string, 0, 16)
	wgr := collect(&failstks, fail)
	for _, stk := range stks {
		wg.Add(1)
		wf <- 1
		go goSampKeyPoints(stk.Code, resample, prior, g, &wg, &wf, fail)
	}
	wg.Wait()
	close(wf)
	close(fail)
	wgr.Wait()

	log.Printf("kpts data saved. %d / %d, failed: %+v", len(stks)-len(failstks), len(stks), failstks)
	return e
}

func goSampKeyPoints(code string, resample, prior int,
	g func(code string, klhist []*model.Quote) (kpts []*model.KeyPoint, err error),
	wg *sync.WaitGroup, wf *chan int, fail chan string) {
	defer func() {
		wg.Done()
		<-*wf
	}()
	kpts, e := KeyPoints(code, resample, prior, g)
	if e != nil {
		log.Printf("%s sampling failed, %+v", code, e)
		fail <- code
		return
	}
	e = SaveKpts(kpts...)
	if e != nil {
		log.Printf("%s failed to save, %+v", code, e)
		fail <- code
	}
	log.Printf("%s %d keypoints sampled", code, len(kpts))
}

//KeyPoints sample key points against non-reinstated daily kline of the specified stock.
// if resample is 0, only sample new key points (existing data will not be resampled).
// if resample is -1, resample all the key points.
// grader function g(c,k) can be nil, in which case the default "Double Wave" function will be used.
// suggested 120 prior.
func KeyPoints(code string, resample, prior int,
	g func(code string, klhist []*model.Quote) (kpts []*model.KeyPoint, err error)) (kpts []*model.KeyPoint, err error) {
	// keep track of latest selected klid;
	var lkp *model.KeyPoint
	if resample == 0 {
		err = dbmap.SelectOne(&lkp, `select klid from kpts where code = ? order by klid desc limit 1`, code)
	} else if resample > 0 {
		err = dbmap.SelectOne(&lkp, `select klid from kpts where code = ? `+
			`order by klid desc limit 1 offset ?`, code, resample)
	}
	if err != nil && sql.ErrNoRows != err {
		return kpts, errors.WithStack(err)
	}
	qryKlid := ""
	if lkp != nil {
		qryKlid = fmt.Sprintf(" and klid > %d", lkp.Klid)
	} else if prior > 0 {
		qryKlid = fmt.Sprintf(" and klid >= %d", prior)
	}
	query, err := global.Dot.Raw("QUERY_NR_DAILY")
	if err != nil {
		err = errors.WithStack(err)
		return kpts, err
	}
	query = fmt.Sprintf(query, qryKlid)
	var klhist []*model.Quote
	_, err = dbmap.Select(&klhist, query, code)
	if err != nil {
		if sql.ErrNoRows != err {
			err = errors.WithStack(err)
			return
		}
		return kpts, nil
	}
	if g == nil {
		return dwGrader(code, klhist)
	}
	return g(code, klhist)
}

// SaveKpts update existing keypoint data or insert new ones in database.
func SaveKpts(kpts ...*model.KeyPoint) (err error) {
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
			valueArgs = append(valueArgs, e.Flag)
			valueArgs = append(valueArgs, e.Klid)
			valueArgs = append(valueArgs, e.RgnLen)
			valueArgs = append(valueArgs, e.RgnRise)
			valueArgs = append(valueArgs, e.Score)
			valueArgs = append(valueArgs, e.SumFall)
			valueArgs = append(valueArgs, e.UnitRise)
			valueArgs = append(valueArgs, e.UUID)
			valueArgs = append(valueArgs, e.Udate)
			valueArgs = append(valueArgs, e.Utime)
		}
		stmt := fmt.Sprintf("INSERT INTO kpts (code,date,flag,klid,rgn_len,rgn_rise,score,sum_fall,unit_rise,"+
			"uuid,udate,utime) VALUES %s "+
			"on duplicate key update date=values(date),flag=values(flag),rgn_len=values(rgn_len),"+
			"rgn_rise=values(rgn_rise),score=values(score),sum_fall=values(sum_fall),unit_rise=values(unit_rise),"+
			"uuid=values(uuid),udate=values(udate),utime=values(utime)",
			strings.Join(valueStrings, ","))
		_, err := global.Dbmap.Exec(stmt, valueArgs...)
		if err != nil {
			fmt.Println(err)
			if strings.Contains(err.Error(), "Deadlock") {
				continue
			} else {
				return errors.Wrap(errors.WithStack(err), code+": failed to bulk update kpts")
			}
		}
	}
	if rt >= retry {
		return errors.Wrap(err, code+": failed to bulk update kpts")
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
