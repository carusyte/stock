package getd

import (
	"database/sql"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"

	"github.com/carusyte/stock/util"
	"github.com/montanaflynn/stats"

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
		go sampXCorlTrn(stk, &wg, &wf, chxcorl)
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

func sampXCorlTrn(stock *model.Stock, wg *sync.WaitGroup, wf *chan int, out chan *xCorlTrnDBJob) {
	defer func() {
		wg.Done()
		<-*wf
	}()
	code := stock.Code
	var err error
	prior := conf.Args.Sampler.PriorLength
	resample := conf.Args.Sampler.Resample
	shift := conf.Args.Sampler.XCorlShift
	span := conf.Args.Sampler.XCorlSpan
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
	maxKlid, err := dbmap.SelectInt(`select max(klid) from kline_d_b where code = ?`, code)
	if err != nil {
		log.Printf(`%s failed to query max klid, %+v`, code, err)
		return
	}
	start := 0
	if lxc != nil {
		start = lxc.Klid - shift + 1
	} else if prior > 0 {
		start = prior - shift
	}
	r := &xCorlTrnDBJob{
		stock: stock,
	}
	count := 0
	stop := false
	var xt []*model.XCorlTrn
	for klid := start; klid <= int(maxKlid)-span-shift; klid++ {
		stop, xt = sampXCorlTrnAt(stock, klid, out)
		if stop {
			break
		}
		if len(xt) > 0 {
			r.xcorls = append(r.xcorls, xt...)
			count++
		}
	}
	if !stop {
		out <- r
		log.Printf("%s xcorl_trn sampled: %d", code, count)
	}
}

func sampXCorlTrnAt(stock *model.Stock, klid int, out chan *xCorlTrnDBJob) (stop bool, xt []*model.XCorlTrn) {
	span := conf.Args.Sampler.XCorlSpan
	shift := conf.Args.Sampler.XCorlShift
	code := stock.Code
	qryKlid := ""
	if klid > 0 {
		qryKlid = fmt.Sprintf(" and klid >= %d", klid)
	}
	qryKlid += fmt.Sprintf(" and klid <= %d", klid+span)
	// use backward reinstated kline
	query, err := global.Dot.Raw("QUERY_BWR_DAILY")
	if err != nil {
		log.Printf(`%s failed to load sql QUERY_BWR_DAILY, %+v`, code, err)
		return true, xt
	}
	query = fmt.Sprintf(query, qryKlid)
	var klhist []*model.Quote
	_, err = dbmap.Select(&klhist, query, code)
	if err != nil {
		if sql.ErrNoRows != err {
			log.Printf(`%s failed to load kline hist data, %+v`, code, err)
			return true, xt
		}
		log.Printf(`%s no data in kline_d_b %s`, code, qryKlid)
		return
	}
	if len(klhist)-shift < span {
		log.Printf("%s insufficient data for xcorl_trn sampling: %d, %d required",
			code, len(klhist)-shift, span)
		return
	}

	//query reference security kline_d_b with shifted matching dates & calculate correlation
	skl := klhist[shift-1]
	dates := make([]string, len(klhist)-shift)
	lrs := make([]float64, len(klhist)-shift)
	for i, k := range klhist {
		if i < len(klhist)-shift {
			dates[i] = k.Date
		}
		if i >= shift {
			if !k.Lr.Valid {
				log.Printf(`%s %s log return is null, skipping`, code, k.Date)
				return
			}
			lrs[i-shift] = k.Lr.Float64
		}
	}
	query, err = global.Dot.Raw("QUERY_BWR_DAILY_4_XCORL_TRN")
	if err != nil {
		log.Printf(`%s failed to load sql QUERY_BWR_DAILY_4_XCORL_TRN, %+v`, code, err)
		return true, xt
	}
	dateStr := util.Join(dates, ",", true)
	query = fmt.Sprintf(query, dateStr, len(dates))
	var rhist []*model.Quote
	_, err = dbmap.Select(&rhist, query, code)
	if err != nil {
		if sql.ErrNoRows != err {
			log.Printf(`%s failed to load reference kline data, %+v`, code, err)
			return true, xt
		}
		log.Printf(`%s no available reference data between %s and %s`,
			code, dates[0], dates[len(dates)-1])
		return
	}
	lcode := ""
	bucket := make([]float64, 0, 16)
	for i, k := range rhist {
		//push kline data into bucket for the same code
		if lcode == k.Code || lcode == "" {
			if k.Lr.Valid {
				bucket = append(bucket, k.Lr.Float64)
			} else {
				log.Printf(`%s reference %s %s log return is null`, code, k.Code, k.Date)
			}
			lcode = k.Code
			if i != len(rhist)-1 {
				continue
			}
		}
		//process filled bucket
		if len(bucket) != len(lrs) {
			log.Printf(`%s reference %s data unmatched, skipping`, code, lcode)
			bucket = make([]float64, 0, 16)
			if k.Lr.Valid {
				bucket = append(bucket, k.Lr.Float64)
			} else {
				log.Printf(`%s reference %s %s log return is null`, code, k.Code, k.Date)
			}
			lcode = k.Code
			continue
		}
		corl, err := stats.Correlation(lrs, bucket)
		if err != nil {
			log.Printf(`%s failed calculate correlation at klid %d, %+v`, code, klid, err)
			return true, xt
		}
		dt, tm := util.TimeStr()
		x := &model.XCorlTrn{
			Code:  code,
			Klid:  skl.Klid,
			Date:  skl.Date,
			Rcode: lcode,
			Corl:  corl,
			Udate: sql.NullString{Valid: true, String: dt},
			Utime: sql.NullString{Valid: true, String: tm},
		}
		xt = append(xt, x)
		bucket = make([]float64, 0, 16)
		if k.Lr.Valid {
			bucket = append(bucket, k.Lr.Float64)
		} else {
			log.Printf(`%s reference %s %s log return is null`, code, k.Code, k.Date)
		}
		lcode = k.Code
	}
	return
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
