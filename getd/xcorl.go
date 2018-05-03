package getd

import (
	"database/sql"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/model"
	"github.com/pkg/errors"
)

type xCorlTrnDBJob struct {
	stock  *model.Stock
	xcorls []*model.XCorlTrn
}

//CalXCorl calculates cross correlation for stocks
func CalXCorl(stocks *model.Stocks) (rstks *model.Stocks) {
	var wg sync.WaitGroup
	pl := int(float64(runtime.NumCPU()) * 0.8)
	wf := make(chan int, pl)
	suc := make(chan *model.Stock, global.JOB_CAPACITY)
	rstks = &model.Stocks{}
	wgr := collect(rstks, suc)
	chxcorl := make(chan *xCorlTrnDBJob, conf.Args.DBQueueCapacity)
	wgdb := goSaveXCorlTrn(chxcorl, suc)
	for _, stk := range stocks.List {
		wg.Add(1)
		wf <- 1
		go sampXCorlTrn(stk.Code, &wg, &wf)
	}
	wg.Wait()
	close(wf)

	close(chxcorl)
	wgdb.Wait()

	close(suc)
	wgr.Wait()

	log.Printf("xcorl_trn data saved. %d / %d", rstks.Size(), stocks.Size())
	if stocks.Size() != rstks.Size() {
		same, skp := stocks.Diff(rstks)
		if !same {
			log.Printf("Failed: %+v", skp)
		}
	}

	return
}

func sampXCorlTrn(code string, wg *sync.WaitGroup, wf *chan int, out chan *xCorlTrnDBJob) {
	defer func() {
		wg.Done()
		<-*wf
	}()
	//TODO realize me
	var err error
	prior := conf.Args.Sampler.PriorLength
	resample := conf.Args.Sampler.Resample
	span := conf.Args.Sampler.XCorlSpan
	shift := conf.Args.Sampler.XCorlShift
	// keep track of latest selected klid;
	var lxc *model.XCorlTrn
	if resample == 0 {
		err = dbmap.SelectOne(&lxc, `select distinct klid from xcorl_trn where code = ? `+
			`order by klid desc limit 1`, code)
	} else if resample > 0 {
		err = dbmap.SelectOne(&lxc,
			`select distinct klid from xcorl_trn where code = ? `+
				`order by klid desc limit 1 offset ?`, code, resample)
	}
	if err != nil && sql.ErrNoRows != err {
		log.Printf(`%s failed to query last xcorl_trn, %+v`, code, err)
		return
	}
	qryKlid := ""
	eklid := span
	if lxc != nil {
		qryKlid = fmt.Sprintf(" and klid > %d", lxc.Klid)
		eklid += lxc.Klid
	} else if prior > 0 {
		qryKlid = fmt.Sprintf(" and klid > %d", prior-1)
		eklid += prior - 1
	}
	qryKlid += fmt.Sprintf(" and klid <= %d", eklid)
	// use backward reinstated kline
	query, e := global.Dot.Raw("QUERY_BWR_DAILY")
	if e != nil {
		log.Printf(`%s failed to load sql QUERY_BWR_DAILY, %+v`, code, err)
		return
	}
	query = fmt.Sprintf(query, qryKlid)
	var klhist []*model.Quote
	_, err = dbmap.Select(&klhist, query, code)
	if err != nil {
		if sql.ErrNoRows != err {
			log.Printf(`%s failed to load kline hist data, %+v`, code, err)
			return
		}
		log.Printf(`%s no data in kline_d_b %s`, code, qryKlid)
		return
	}

	if len(klhist) < span {
		log.Printf("%s insufficient data for xcorl_trn sampling: %d, %d required",
			code, len(klhist), span)
		return
	}
	//TODO: query reference security kline_d_b with shifted matching dates & calculate correlation
	var r []*model.KeyPoint
	r, err = grader.sample(code, frame, klhist)
	if err != nil {
		return
	}
	chkpts[frame] <- r
	log.Printf("%s xcorl_trn sampled: %d", code, len(r))
}

func goSaveXCorlTrn(chxcorl chan *xCorlTrnDBJob, suc chan *model.Stock) (wg *sync.WaitGroup) {
	wg = new(sync.WaitGroup)
	wg.Add(1)
	go func(wg *sync.WaitGroup, ch chan *xCorlTrnDBJob, suc chan *model.Stock) {
		defer wg.Done()
		for x := range ch {
			e := SaveXCorlTrn(x.xcorls...)
			if e == nil {
				suc <- x.stock
				log.Printf("%s %d %s saved", x.stock.Code, len(x.xcorls), "xcorl_trn")
			}
		}
	}(wg, chxcorl, suc)
	return
}

// SaveXCorlTrn update existing xcorl_trn data or insert new ones in database.
func SaveXCorlTrn(xs ...*model.XCorlTrn) (err error) {
	if len(xs) == 0 {
		return nil
	}
	retry := 10
	rt := 0
	code := ""
	for ; rt < retry; rt++ {
		code = xs[0].Code
		valueStrings := make([]string, 0, len(xs))
		valueArgs := make([]interface{}, 0, len(xs)*7)
		for _, e := range xs {
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, e.Code)
			valueArgs = append(valueArgs, e.Klid)
			valueArgs = append(valueArgs, e.Date)
			valueArgs = append(valueArgs, e.Rcode)
			valueArgs = append(valueArgs, e.Corl)
			valueArgs = append(valueArgs, e.Udate)
			valueArgs = append(valueArgs, e.Utime)
		}
		stmt := fmt.Sprintf("INSERT INTO xcorl_trn (code,klid,date,rcode,corl,"+
			"udate,utime) VALUES %s "+
			"on duplicate key update corl=values(corl),"+
			"udate=values(udate),utime=values(utime)",
			strings.Join(valueStrings, ","))
		_, err := dbmap.Exec(stmt, valueArgs...)
		if err != nil {
			fmt.Println(err)
			if strings.Contains(err.Error(), "Deadlock") {
				continue
			} else {
				return errors.Wrap(errors.WithStack(err), code+": failed to bulk update xcorl_trn")
			}
		}
		break
	}
	if rt >= retry {
		return errors.Wrap(err, code+": failed to bulk update xcorl_trn")
	}
	return nil
}
