package sampler

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"math/rand"
	"runtime"
	"strings"
	"sync"

	"github.com/carusyte/stock/getd"
	"github.com/carusyte/stock/util"
	uuid "github.com/satori/go.uuid"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/model"
	"github.com/pkg/errors"
)

var (
	wccMaxLr = math.NaN()
	lock     sync.Mutex
)

type wccTrnDBJob struct {
	stock *model.Stock
	fin   int //-1:abort, 0:unfinished, 1:finished
	wccs  []*model.WccTrn
}

//CalWcc calculates Warping Correlation Coefficient for stocks
func CalWcc(stocks *model.Stocks) {
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
	chwcc := make(chan *wccTrnDBJob, conf.Args.DBQueueCapacity)
	wgdb := goSaveWccTrn(chwcc, suc)
	log.Printf("calculating warping correlation coefficients for training, parallel level:%d", pl)
	for _, stk := range stocks.List {
		wg.Add(1)
		wf <- 1
		go sampWccTrn(stk, &wg, &wf, chwcc)
	}
	wg.Wait()
	close(wf)

	close(chwcc)
	wgdb.Wait()

	close(suc)
	wgr.Wait()

	updateWcc()

	log.Printf("wcc_trn data saved. sampled stocks: %d / %d", len(rstks), stocks.Size())
	if stocks.Size() != len(rstks) {
		codes := make([]string, stocks.Size())
		for i, s := range stocks.List {
			codes[i] = s.Code
		}
		eq, fs, _ := util.DiffStrings(codes, rstks)
		if !eq {
			log.Printf("Unsampled: %+v", fs)
		}
	}
}

//updates corl column in the wcc_trn table based on sampled min_diff and max_diff
func updateWcc() {
	//remap [0, x] to [1, -1] (in opposite direction)
	//formula: -1 * ((x-f1)/(t1-f1) * (t2-f2) + f2)
	//simplified: (f1-x)/(t1-f1)*(t2-f2)-f2
	log.Printf("querying max(max_diff) + min(min_diff)...")
	max, e := dbmap.SelectFloat("select max(max_diff) + min(min_diff) from wcc_trn")
	if e != nil {
		log.Printf("failed to update corl: %+v", errors.WithStack(e))
		return
	}
	log.Printf("max: %f, updating corl value...", max)
	_, e = dbmap.Exec(`
		UPDATE wcc_trn  
		SET 
			corl = CASE
				WHEN min_diff < :mx - max_diff THEN - min_diff / :mx * 2 + 1
				ELSE  - max_diff / :mx * 2 + 1
			END
	`, map[string]interface{}{"mx": max})
	if e != nil {
		log.Printf("failed to update corl: %+v", errors.WithStack(e))
		return
	}
	log.Printf("collecting corl stats...")
	_, e = dbmap.Exec(`
		INSERT INTO fs_stats (method, tab, fields, mean, std, udate, utime)
		SELECT 
			'standardization', 'wcc_trn', 'corl', AVG(corl), STD(corl), DATE_FORMAT(now(), '%%Y-%%m-%%d'), DATE_FORMAT(now(), '%%H:%%i:%%S')
		FROM
			wcc_trn
	`,)
	if e != nil {
		log.Printf("failed to collect corl stats: %+v", errors.WithStack(e))
		return
	}
	log.Printf("standardizing...")
	_, e = dbmap.Exec(`
		UPDATE wcc_trn w
				JOIN
			(SELECT 
				mean m, std s
			FROM
				fs_stats
			WHERE
				method = 'standardization'
					AND tab = 'wcc_trn'
					AND fields = 'corl') f 
		SET 
			corl = (corl - f.m) / f.s
	`,)
	if e != nil {
		log.Printf("failed to standardize wcc corl: %+v", errors.WithStack(e))
		return
	}
}

func sampWccTrn(stock *model.Stock, wg *sync.WaitGroup, wf *chan int, out chan *wccTrnDBJob) {
	defer func() {
		wg.Done()
		<-*wf
	}()
	code := stock.Code
	var err error
	prior := conf.Args.Sampler.PriorLength
	shift := conf.Args.Sampler.WccMaxShift
	span := conf.Args.Sampler.XCorlSpan
	syear := conf.Args.Sampler.XCorlStartYear
	portion := conf.Args.Sampler.XCorlPortion
	maxKlid, err := dbmap.SelectInt(`select max(klid) from kline_d_b where code = ?`, code)
	if err != nil {
		log.Printf(`%s failed to query max klid, %+v`, code, err)
		return
	}
	maxk := int(maxKlid)
	if maxk+1 < prior {
		log.Printf("%s insufficient data for wcc_trn sampling: got %d, prior of %d required",
			code, maxk+1, prior)
		return
	}
	start := 0
	if len(syear) > 0 {
		sklid, err := dbmap.SelectInt(`select min(klid) from kline_d_b where code = ? and date >= ?`, code, syear)
		if err != nil {
			log.Printf(`%s failed to query min klid, %+v`, code, err)
			return
		}
		if int(sklid)+1 < prior {
			log.Printf("%s insufficient data for wcc_trn sampling: got %d, prior of %d required",
				code, int(sklid)+1, prior)
			return
		}
		start = int(sklid)
	} else if prior > 0 {
		start = prior - shift
	}
	retry := false
	var e error
	var wccs []*model.WccTrn
	for klid := start; klid <= maxk-span+1; klid++ {
		if rand.Float64() > portion {
			continue
		}
		for rt := 0; rt < 3; rt++ {
			retry, wccs, e = sampWccTrnAt(stock, klid)
			if !retry && e == nil {
				break
			}
			log.Printf("%s klid(%d) retrying %d...", stock.Code, klid, rt+1)
		}
		if e != nil {
			out <- &wccTrnDBJob{
				stock: stock,
				fin:   -1,
			}
			break
		}
		if len(wccs) > 0 {
			out <- &wccTrnDBJob{
				stock: stock,
				fin:   0,
				wccs:  wccs,
			}
		}
	}
	out <- &wccTrnDBJob{
		stock: stock,
		fin:   1,
	}
}

//klid is not included in target code span
func sampWccTrnAt(stock *model.Stock, klid int) (retry bool, wccs []*model.WccTrn, e error) {
	span := conf.Args.Sampler.XCorlSpan
	shift := conf.Args.Sampler.WccMaxShift
	minReq := conf.Args.Sampler.PriorLength
	prior := conf.Args.Sampler.XCorlPrior
	code := stock.Code
	qryKlid := ""
	offset := prior + shift - 1
	if klid > 0 {
		qryKlid = fmt.Sprintf(" and klid >= %d", klid-offset)
	}
	qryKlid += fmt.Sprintf(" and klid <= %d", klid+span)
	// use backward reinstated kline
	query, err := global.Dot.Raw("QUERY_BWR_DAILY")
	if err != nil {
		log.Printf(`%s failed to load sql QUERY_BWR_DAILY, %+v`, code, err)
		return true, wccs, err
	}
	query = fmt.Sprintf(query, qryKlid)
	var klhist []*model.Quote
	_, err = dbmap.Select(&klhist, query, code)
	if err != nil {
		if sql.ErrNoRows != err {
			log.Printf(`%s failed to load kline hist data, %+v`, code, err)
			return true, wccs, err
		}
		log.Printf(`%s no data in kline_d_b %s`, code, qryKlid)
		return
	}
	if len(klhist) < prior+shift+span {
		log.Printf("%s insufficient data for wcc_trn sampling at klid %d: %d, %d required",
			code, klid, len(klhist), prior+shift+span)
		return
	}

	//query reference security kline_d_b with shifted matching dates & calculate correlation
	skl := klhist[offset]
	log.Printf("%s sampling wcc at %d, %s", skl.Code, skl.Klid, skl.Date)
	// ref code dates
	dates := make([]string, len(klhist)-1)
	// target code lrs
	lrs := make([]float64, span)
	for i, k := range klhist {
		if i < len(dates) {
			dates[i] = k.Date
		}
		if i >= shift+prior {
			if !k.Lr.Valid {
				log.Printf(`%s %s log return is null, skipping`, code, k.Date)
				return
			}
			lrs[i-shift-prior] = k.Lr.Float64
		}
	}
	var codes []string
	dateStr := util.Join(dates, ",", true)
	query = fmt.Sprintf(`select code from kline_d_b where code <> ? and date in (%s) `+
		`group by code having count(*) = ? and min(klid) >= ?`, dateStr)
	_, err = dbmap.Select(&codes, query, code, len(dates), minReq-1)
	if err != nil {
		if sql.ErrNoRows != err {
			log.Printf(`%s failed to load reference kline data, %+v`, code, err)
			return true, wccs, err
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
		return true, wccs, err
	}
	codeStr := util.Join(codes, ",", true)
	dateStr = util.Join(dates[prior:], ",", true)
	query = fmt.Sprintf(query, codeStr, dateStr)
	var rhist []*model.Quote
	_, err = dbmap.Select(&rhist, query)
	if err != nil {
		if sql.ErrNoRows != err {
			log.Printf(`%s failed to load reference kline data, %+v`, code, err)
			return true, wccs, err
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
		if len(bucket) != len(lrs)+shift-1 {
			log.Printf(`%s reference %s data unmatched: %d+%d != %d, skipping`, code, lcode, len(lrs), shift, len(bucket))
			bucket = make([]float64, 0, 16)
			if k.Lr.Valid {
				bucket = append(bucket, k.Lr.Float64)
			} else {
				log.Printf(`%s reference %s %s log return is null`, code, k.Code, k.Date)
			}
			lcode = k.Code
			continue
		}
		//calculate mindiff and maxdiff
		minDiff, maxDiff, err := warpingCorl(lrs, bucket)
		if err != nil {
			log.Printf(`%s failed calculate wcc at klid %d, %+v`, code, klid, err)
			return false, wccs, err
		}
		dt, tm := util.TimeStr()
		w := &model.WccTrn{
			UUID:    fmt.Sprintf("%s", uuid.NewV1()),
			Code:    code,
			Klid:    skl.Klid,
			Date:    skl.Date,
			Rcode:   lcode,
			MinDiff: minDiff,
			MaxDiff: maxDiff,
			Udate:   sql.NullString{Valid: true, String: dt},
			Utime:   sql.NullString{Valid: true, String: tm},
		}
		wccs = append(wccs, w)
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

func goSaveWccTrn(chwcc chan *wccTrnDBJob, suc chan string) (wg *sync.WaitGroup) {
	wg = new(sync.WaitGroup)
	wg.Add(1)
	go func(wg *sync.WaitGroup, ch chan *wccTrnDBJob, suc chan string) {
		defer wg.Done()
		counter := make(map[string]int)
		for w := range ch {
			code := w.stock.Code
			if w.fin < 0 {
				log.Printf("%s failed samping wcc_trn", code)
			} else if w.fin == 0 && len(w.wccs) > 0 {
				w1 := w.wccs[0]
				e := saveWccTrn(w.wccs...)
				if e == nil {
					counter[code] += len(w.wccs)
					log.Printf("%s %d wcc_trn saved, start date:%s", code, len(w.wccs), w1.Date)
				} else {
					log.Panicf("%s %s db operation error:%+v", code, w1.Date, e)
				}
			} else {
				log.Printf("%s finished wccs_trn sampling, total: %d", code, counter[code])
				suc <- w.stock.Code
			}
		}
	}(wg, chwcc, suc)
	return
}

// saveWccTrn update existing wcc_trn data or insert new ones in database.
func saveWccTrn(ws ...*model.WccTrn) (err error) {
	if len(ws) == 0 {
		return nil
	}
	code := ws[0].Code
	valueStrings := make([]string, 0, len(ws))
	valueArgs := make([]interface{}, 0, len(ws)*10)
	for _, e := range ws {
		valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		valueArgs = append(valueArgs, e.UUID)
		valueArgs = append(valueArgs, e.Code)
		valueArgs = append(valueArgs, e.Klid)
		valueArgs = append(valueArgs, e.Date)
		valueArgs = append(valueArgs, e.Rcode)
		valueArgs = append(valueArgs, e.Corl)
		valueArgs = append(valueArgs, e.MinDiff)
		valueArgs = append(valueArgs, e.MaxDiff)
		valueArgs = append(valueArgs, e.Udate)
		valueArgs = append(valueArgs, e.Utime)
	}
	stmt := fmt.Sprintf("INSERT INTO wcc_trn (uuid,code,klid,date,rcode,corl,"+
		"min_diff,max_diff,udate,utime) VALUES %s "+
		"on duplicate key update corl=values(corl), min_diff=values(min_diff),"+
		"max_diff=values(max_diff),udate=values(udate),utime=values(utime)",
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
				return errors.Wrap(errors.WithStack(err), code+": failed to bulk update wcc_trn")
			}
		}
		break
	}
	if rt >= retry {
		return errors.Wrap(err, code+": failed to bulk update wcc_trn")
	}
	return nil
}

//warpingCorl calculates warping correlation coefficients and absolute difference.
//Actually summing over minimum/maximum absolute distance of each paired elements within shifted prior of bucket,
//and divide by len(lrs) to get average. Final correlation coefficient is chosen by max absolute average.
func warpingCorl(lrs, bucket []float64) (minDiff, maxDiff float64, e error) {
	lenLrs := len(lrs)
	if len(bucket) < lenLrs {
		return minDiff, maxDiff, errors.WithStack(errors.Errorf("len(bucket)(%d) must be greater than len(lrs)(%s)", len(bucket), len(lrs)))
	}
	shift := len(bucket) - lenLrs
	sumMin, sumMax := 0., 0.
	for i := 0; i < lenLrs; i++ {
		lr := lrs[i]
		min := math.Inf(1)
		max := math.Inf(-1)
		for j := 0; j <= shift; j++ {
			b := bucket[j]
			diff := math.Abs(lr - b)
			if diff < min {
				min = diff
			}
			if diff > max {
				max = diff
			}
		}
		sumMin += min
		sumMax += max
	}
	if e != nil {
		return minDiff, maxDiff, e
	}
	flen := float64(lenLrs)
	minDiff = sumMin / flen
	maxDiff = sumMax / flen
	return
}
