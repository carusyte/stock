package sampler

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"strings"
	"sync"

	"github.com/carusyte/stock/getd"
	"github.com/carusyte/stock/util"
	"github.com/montanaflynn/stats"
	uuid "github.com/satori/go.uuid"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/model"
	"github.com/pkg/errors"
)

type xCorlTrnDBJob struct {
	stock  *model.Stock
	fin    int //-1:abort, 0:unfinished, 1:finished
	xcorls []*model.XCorlTrn
}

//CalXCorl calculates cross correlation for stocks
func CalXCorl(stocks *model.Stocks) {
	if stocks == nil {
		stocks = &model.Stocks{}
		stocks.Add(getd.StocksDb()...)
	}
	var wg sync.WaitGroup
	pl := int(float64(runtime.NumCPU()) * 0.8)
	wf := make(chan int, pl)
	suc := make(chan string, global.JOB_CAPACITY)
	var rstks []string
	wgr := collect(&rstks, suc)
	chxcorl := make(chan *xCorlTrnDBJob, conf.Args.DBQueueCapacity)
	wgdb := goSaveXCorlTrn(chxcorl, suc)
	log.Printf("calculating cross correlations for training, parallel level:%d", pl)
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

	log.Printf("xcorl_trn data saved. %d / %d", len(rstks), stocks.Size())
	if stocks.Size() != len(rstks) {
		codes := make([]string, stocks.Size())
		for i, s := range stocks.List {
			codes[i] = s.Code
		}
		eq, fs, _ := util.DiffStrings(codes, rstks)
		if !eq {
			log.Printf("Unsaved: %+v", fs)
		}
	}
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
	syear := conf.Args.Sampler.XCorlStartYear
	portion := conf.Args.Sampler.XCorlPortion
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
	maxk := int(maxKlid)
	if maxk+1 < prior {
		log.Printf("%s insufficient data for xcorl_trn sampling: got %d, prior of %d required",
			code, maxk+1, prior)
		return
	}
	start := 0
	if lxc != nil {
		start = lxc.Klid - shift + 1
	} else if len(syear) > 0 {
		sklid, err := dbmap.SelectInt(`select min(klid) from kline_d_b where code = ? and date >= ?`, code, syear)
		if err != nil {
			log.Printf(`%s failed to query min klid, %+v`, code, err)
			return
		}
		if int(sklid)+1 < prior {
			log.Printf("%s insufficient data for xcorl_trn sampling: got %d, prior of %d required",
				code, int(sklid)+1, prior)
			return
		}
		start = int(sklid)
	} else if prior > 0 {
		start = prior - shift
	}
	stop := false
	var xt []*model.XCorlTrn
	for klid := start; klid <= maxk-span-shift; klid++ {
		if rand.Float64() > portion {
			continue
		}
		stop, xt = sampXCorlTrnAt(stock, klid)
		if stop {
			out <- &xCorlTrnDBJob{
				stock: stock,
				fin:   -1,
			}
			break
		}
		if len(xt) > 0 {
			out <- &xCorlTrnDBJob{
				stock:  stock,
				fin:    0,
				xcorls: xt,
			}
		}
	}
	out <- &xCorlTrnDBJob{
		stock: stock,
		fin:   1,
	}
}

func sampXCorlTrnAt(stock *model.Stock, klid int) (stop bool, xt []*model.XCorlTrn) {
	span := conf.Args.Sampler.XCorlSpan
	shift := conf.Args.Sampler.XCorlShift
	prior := conf.Args.Sampler.PriorLength
	code := stock.Code
	qryKlid := ""
	offset := span - 1
	if klid > 0 {
		qryKlid = fmt.Sprintf(" and klid >= %d", klid-offset)
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
	if len(klhist) < span+offset+shift {
		log.Printf("%s insufficient data for xcorl_trn sampling at klid %d: %d, %d required",
			code, klid, len(klhist)-shift, span+offset)
		return
	}

	//query reference security kline_d_b with shifted matching dates & calculate correlation
	skl := klhist[shift+offset-1]
	log.Printf("%s sampling xcorl at %d, %s", skl.Code, skl.Klid, skl.Date)
	dates := make([]string, len(klhist)-shift)
	lrs := make([]float64, len(klhist)-shift-offset)
	for i, k := range klhist {
		if i < len(klhist)-shift {
			dates[i] = k.Date
		}
		if i >= shift+offset {
			if !k.Lr.Valid {
				log.Printf(`%s %s log return is null, skipping`, code, k.Date)
				return
			}
			lrs[i-shift-offset] = k.Lr.Float64
		}
	}
	var codes []string
	dateStr := util.Join(dates, ",", true)
	query = fmt.Sprintf(`select code from kline_d_b where code <> ? and date in (%s) `+
		`group by code having count(*) = ? and min(klid) >= ?`, dateStr)
	_, err = dbmap.Select(&codes, query, code, len(dates), prior-1)
	if err != nil {
		if sql.ErrNoRows != err {
			log.Printf(`%s failed to load reference kline data, %+v`, code, err)
			return true, xt
		}
		log.Printf(`%s no available reference data between %s and %s`,
			code, dates[0], dates[len(dates)-1])
		return
	}
	if len(codes) == 0 {
		log.Printf(`%s no available reference data between %s and %s`,
			code, dates[0], dates[len(dates)-1])
		return
	}

	query, err = global.Dot.Raw("QUERY_BWR_DAILY_4_XCORL_TRN")
	if err != nil {
		log.Printf(`%s failed to load sql QUERY_BWR_DAILY_4_XCORL_TRN, %+v`, code, err)
		return true, xt
	}
	codeStr := util.Join(codes, ",", true)
	dateStr = util.Join(dates[offset:], ",", true)
	query = fmt.Sprintf(query, codeStr, dateStr)
	var rhist []*model.Quote
	_, err = dbmap.Select(&rhist, query)
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
			UUID:  fmt.Sprintf("%s", uuid.NewV1()),
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

func goSaveXCorlTrn(chxcorl chan *xCorlTrnDBJob, suc chan string) (wg *sync.WaitGroup) {
	wg = new(sync.WaitGroup)
	wg.Add(1)
	go func(wg *sync.WaitGroup, ch chan *xCorlTrnDBJob, suc chan string) {
		defer wg.Done()
		counter := make(map[string]int)
		for x := range ch {
			code := x.stock.Code
			if x.fin < 0 {
				log.Printf("%s failed samping xcorl_trn", code)
			} else if x.fin == 0 && len(x.xcorls) > 0 {
				x1 := x.xcorls[0]
				e := saveXCorlTrn(x.xcorls...)
				if e == nil {
					counter[code] += len(x.xcorls)
					log.Printf("%s %d xcorl_trn saved, start date:%s", code, len(x.xcorls), x1.Date)
				} else {
					log.Panicf("%s %s db operation error:%+v", code, x1.Date, e)
				}
			} else {
				log.Printf("%s finished xcorl_trn sampling, total: %d", code, counter[code])
				suc <- x.stock.Code
			}
		}
	}(wg, chxcorl, suc)
	return
}

// saveXCorlTrn update existing xcorl_trn data or insert new ones in database.
func saveXCorlTrn(xs ...*model.XCorlTrn) (err error) {
	if len(xs) == 0 {
		return nil
	}
	code := xs[0].Code
	valueStrings := make([]string, 0, len(xs))
	valueArgs := make([]interface{}, 0, len(xs)*8)
	for _, e := range xs {
		valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?)")
		valueArgs = append(valueArgs, e.UUID)
		valueArgs = append(valueArgs, e.Code)
		valueArgs = append(valueArgs, e.Klid)
		valueArgs = append(valueArgs, e.Date)
		valueArgs = append(valueArgs, e.Rcode)
		valueArgs = append(valueArgs, e.Corl)
		valueArgs = append(valueArgs, e.Udate)
		valueArgs = append(valueArgs, e.Utime)
	}
	stmt := fmt.Sprintf("INSERT INTO xcorl_trn (uuid,code,klid,date,rcode,corl,"+
		"udate,utime) VALUES %s "+
		"on duplicate key update corl=values(corl),"+
		"udate=values(udate),utime=values(utime)",
		strings.Join(valueStrings, ","))
	retry := conf.Args.DeadlockRetry
	rt := 0
	for ; rt < retry; rt++ {
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
