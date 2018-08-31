package sampler

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"

	"github.com/carusyte/stock/getd"
	"github.com/carusyte/stock/util"
	"github.com/ssgreg/repeat"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/model"
	"github.com/pkg/errors"
)

var (
	wccMaxLr          = math.NaN()
	curVolPath        string
	curVolSize        int
	curVolNo          = -1
	volLock           sync.RWMutex
	ftQryInit         sync.Once
	statsQryInit      sync.Once
	qryKline, qryDate string
	wccStats          *model.FsStats
	gcsClient         *util.GCSClient
)

type wccTrnDBJob struct {
	stock *model.Stock
	fin   int //-1:abort, 0:unfinished, 1:finished
	wccs  []*model.WccTrn
}

type stockrelDBJob struct {
	code     string
	stockrel *model.StockRel
}

type pcaljob struct {
	Code string
	Date string
	Klid int
}

//ExpJob stores wcc inference file export job information.
type ExpJob struct {
	Code   string
	Klid   int
	Rcodes []string
}

type fileUploadJob struct {
	localFile string
	dest      string
}

//CorlInferFile represents the structure of json formatted corl inference file
type CorlInferFile struct {
	Code     string
	Klid     int
	Refs     []string
	Features [][][]float64
	SeqLens  []int
}

//PcalWcc pre-calculates future wcc value using non-reinstated daily kline data and updates stockrel table.
func PcalWcc(expInferFile, upload, nocache bool, localPath, rbase string) {
	log.Println("starting wcc pre-calculation...")
	jobs, e := getPcalJobs()
	if e != nil || len(jobs) <= 0 {
		return
	}
	log.Printf("#jobs: %d", len(jobs))
	var expch chan<- *ExpJob
	var expwg *sync.WaitGroup
	if expInferFile {
		log.Println("inference file exportation enabled")
		expch, expwg = ExpInferFile(localPath, rbase, upload, nocache)
	}
	// make db job channel & waitgroup, start db goroutine
	dbch := make(chan *stockrelDBJob, conf.Args.DBQueueCapacity)
	dbwg := new(sync.WaitGroup)
	dbwg.Add(1)
	go collectStockRels(dbwg, dbch)
	// make job channel & waitgroup, start calculation goroutine
	pcch := make(chan *pcaljob, conf.Args.Concurrency)
	pcwg := new(sync.WaitGroup)
	pl := int(float64(runtime.NumCPU()) * 0.8)
	for i := 0; i < pl; i++ {
		pcwg.Add(1)
		go pcalWccWorker(pcch, expch, dbch, pcwg)
	}
	// iterate through qualified kline data, create wcc calculation job instance and push it to job channel
	for _, j := range jobs {
		pcch <- j
	}
	// close job channel, wait for job completion
	close(pcch)
	pcwg.Wait()
	// close db job channel wait for db job completion
	close(dbch)
	dbwg.Wait()
	// close exp channel wait for exp job completion, if the channel is not nil
	if expch != nil {
		close(expch)
		expwg.Wait()
	}
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

	UpdateWcc()

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

//UpdateWcc updates corl and corl_stz column in the wcc_trn table based on sampled min_diff and max_diff
func UpdateWcc() {
	//remap [0, x] to [1, -1] (in opposite direction)
	//formula: -1 * ((x-f1)/(t1-f1) * (t2-f2) + f2)
	//simplified: (f1-x)/(t1-f1)*(t2-f2)-f2
	log.Printf("querying max(max_diff) + min(min_diff)...")
	max, e := dbmap.SelectFloat("select max(max_diff) + min(min_diff) from wcc_trn")
	if e != nil {
		log.Printf("failed to query max(max_diff) + min(min_diff) for wcc: %+v", errors.WithStack(e))
		return
	}
	//update corl stock by stock to avoid undo file explosion
	var codes []string
	_, e = dbmap.Select(&codes, `select distinct code from wcc_trn order by code`)
	if e != nil {
		log.Printf("failed to query codes in wcc_trn: %+v", errors.WithStack(e))
		return
	}
	log.Printf("max: %f, updating corl value for %d stocks...", max, len(codes))
	for i, c := range codes {
		prog := float32(i+1) / float32(len(codes)) * 100.
		log.Printf("updating corl for %s, progress: %.3f%%", c, prog)
		_, e = dbmap.Exec(`
			UPDATE wcc_trn  
			SET
				corl = CASE
					WHEN min_diff < :mx - max_diff THEN - min_diff / :mx * 2 + 1
					ELSE  - max_diff / :mx * 2 + 1
				END,
				udate=DATE_FORMAT(now(), '%Y-%m-%d'), 
				utime=DATE_FORMAT(now(), '%H:%i:%S')
			WHERE code = :code
	`, map[string]interface{}{"mx": max, "code": c})
		if e != nil {
			log.Printf("failed to update corl for %s: %+v", c, errors.WithStack(e))
			return
		}
	}
	log.Printf("collecting corl stats...")
	// _, e = dbmap.Exec(`delete from fs_stats where method = ? and tab = ? and fields = ?`,
	// 	"standardization", "wcc_trn", "corl")
	// if e != nil {
	// 	log.Printf("failed to delete existing corl stats: %+v", errors.WithStack(e))
	// 	return
	// }
	_, e = dbmap.Exec(`
		INSERT INTO fs_stats (method, tab, fields, mean, std, vmax, udate, utime)
		SELECT 'standardization', 'wcc_trn', 'corl', AVG(corl), STD(corl), ?, DATE_FORMAT(now(), '%Y-%m-%d'), DATE_FORMAT(now(), '%H:%i:%S') FROM wcc_trn
		ON DUPLICATE KEY UPDATE mean=values(mean),std=values(std),vmax=values(vmax),udate=values(udate),utime=values(utime)
	`, max)
	if e != nil {
		log.Printf("failed to collect corl stats: %+v", errors.WithStack(e))
		return
	}

	StzWcc(codes...)
}

//StzWcc standardizes wcc_trn corl value and updates corl_stz field in the table.
func StzWcc(codes ...string) (e error) {
	log.Printf("standardizing...")
	if codes == nil {
		log.Printf("querying codes in wcc_trn table...")
		_, e = dbmap.Select(&codes, `select distinct code from wcc_trn order by code`)
		if e != nil {
			log.Printf("failed to query codes in wcc_trn: %+v", errors.WithStack(e))
			return
		}
	}
	var cstat struct {
		Mean, Std, Vmax float32
	}
	e = dbmap.SelectOne(&cstat, `select mean, std, vmax from fs_stats where method = ? and tab = ? and fields = ?`,
		`standardization`, `wcc_trn`, `corl`)
	if e != nil {
		log.Printf("failed to query corl stats: %+v", errors.WithStack(e))
		return
	}
	log.Printf("%d codes, mean: %f std: %f, vmax: %f", len(codes), cstat.Mean, cstat.Std, cstat.Vmax)
	//update stock by stock to avoid undo file explosion
	for i, c := range codes {
		prog := float32(i+1) / float32(len(codes)) * 100.
		log.Printf("standardizing %s, progress: %.3f%%", c, prog)
		_, e = dbmap.Exec(`
			UPDATE wcc_trn w
			SET 
				corl_stz = (corl - ?) / ?,
				udate=DATE_FORMAT(now(), '%Y-%m-%d'), 
				utime=DATE_FORMAT(now(), '%H:%i:%S')
			WHERE code = ?
		`, cstat.Mean, cstat.Std, c)
		if e != nil {
			log.Printf("failed to standardize wcc corl for %s: %+v", c, errors.WithStack(e))
			return
		}
	}
	return nil
}

//ExpInferFile exports inference file to local disk, and optionally uploads to google cloud storage.
func ExpInferFile(localPath, rbase string, upload, nocache bool) (fec chan<- *ExpJob, fewg *sync.WaitGroup) {
	var fuc chan *fileUploadJob
	var fuwg *sync.WaitGroup
	if upload {
		log.Println("GCS uploading enabled")
		gcsClient = util.NewGCSClient(conf.Args.GCS.UseProxy)
		fuc = make(chan *fileUploadJob, conf.Args.GCS.UploadQueue)
		fuwg = new(sync.WaitGroup)
		for i := 0; i < conf.Args.GCS.Connection; i++ {
			fuwg.Add(1)
			go uploadToGCS(fuc, fuwg, nocache)
		}
	}
	fileExpCh := make(chan *ExpJob, conf.Args.Concurrency)
	fec = fileExpCh
	fewg = new(sync.WaitGroup)
	fewg.Add(1)
	go fileExporter(localPath, rbase, fileExpCh, fuc, fewg, fuwg)
	return
}

func pcalWccWorker(pcch <-chan *pcaljob, expch chan<- *ExpJob, dbch chan<- *stockrelDBJob, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Println("pcal worker started")
	stats := getWccFeatStats()
	for pcjob := range pcch {
		code := pcjob.Code
		klid := pcjob.Klid
		date := pcjob.Date
		log.Printf("processing %s@%d, %s...", code, klid, date)
		rcodes, e := getRcodes4WccInfer(code, klid)
		if e != nil || len(rcodes) < 2 {
			continue
		}
		log.Printf("%s@%s has %d eligible reference codes for inference", code, date, len(rcodes))
		if expch != nil {
			expch <- &ExpJob{
				Code:   code,
				Klid:   klid,
				Rcodes: rcodes,
			}
		}
		lrs, reflrs, e := getKlines4WccPreCalculation(code, klid, rcodes...)
		var minc, maxc sql.NullString
		minv := sql.NullFloat64{Float64: math.Inf(0)}
		maxv := sql.NullFloat64{Float64: math.Inf(-1)}
		if len(lrs) > 0 && len(reflrs) > 0 && e == nil {
			log.Printf("%s has %d eligible reference codes for pre-calculation", code, len(reflrs))
			for rc, rlrs := range reflrs {
				minDiff, maxDiff, e := warpingCorl(lrs, rlrs)
				if e != nil {
					log.Printf(`%s@%d failed to pre-calculate wcc with %s, skipping: %+v`, code, klid, rc, e)
					continue
				}
				corl := 0.
				vmax := stats.Vmax.Float64
				if maxDiff > vmax {
					maxDiff = vmax //clipping
				}
				if minDiff < vmax-maxDiff {
					corl = -minDiff/vmax*2. + 1.
				} else {
					corl = -maxDiff/vmax*2. + 1.
				}
				mean, std := stats.Mean.Float64, stats.Std.Float64
				corl = (corl - mean) / std
				if corl < minv.Float64 {
					minv = sql.NullFloat64{Float64: corl, Valid: true}
					minc = sql.NullString{String: rc, Valid: true}
				}
				if corl > maxv.Float64 {
					maxv = sql.NullFloat64{Float64: corl, Valid: true}
					maxc = sql.NullString{String: rc, Valid: true}
				}
			}
		} else if e != nil {
			continue
		}
		log.Printf("%s: {pcode:%s, pos:%.5f, ncode:%s, neg:%.5f}",
			code, maxc.String, maxv.Float64, minc.String, minv.Float64)
		ud, ut := util.TimeStr()
		dbch <- &stockrelDBJob{
			code: code,
			stockrel: &model.StockRel{
				Code:       code,
				Date:       sql.NullString{String: date, Valid: true},
				Klid:       klid,
				RcodePosHs: maxc,
				RcodeNegHs: minc,
				PosCorlHs:  maxv,
				NegCorlHs:  minv,
				Udate:      sql.NullString{String: ud, Valid: true},
				Utime:      sql.NullString{String: ut, Valid: true},
			},
		}
	}
}

func getWccFeatStats() (stats *model.FsStats) {
	if wccStats != nil {
		return wccStats
	}
	query := func() {
		op := func(c int) (e error) {
			if c > 0 {
				log.Printf("#%d retrying to query fs_stats...", c)
			}
			e = dbmap.SelectOne(&wccStats, `select * from fs_stats where method = ? and fields = ? and tab = ?`,
				"standardization", "corl", "wcc_trn")
			if e != nil {
				if sql.ErrNoRows != e {
					log.Printf(`failed to query fs_stats: %+v`, e)
					return repeat.HintTemporary(e)
				}
				return repeat.HintStop(errors.New(`wcc stats not ready`))
			}
			return nil
		}
		e := repeat.Repeat(
			repeat.FnWithCounter(op),
			repeat.StopOnSuccess(),
			repeat.LimitMaxTries(conf.Args.DefaultRetry),
			repeat.WithDelay(
				repeat.FullJitterBackoff(500*time.Millisecond).WithMaxDelay(15*time.Second).Set(),
			),
		)
		if e != nil {
			log.Panicf("give up querying wcc stats: %+v", e)
		}
	}
	statsQryInit.Do(query)
	return wccStats
}

func getKlines4WccPreCalculation(code string, klid int, rcodes ...string) (lrs []float64, reflrs map[string][]float64, e error) {
	span := conf.Args.Sampler.CorlSpan
	shift := conf.Args.Sampler.WccMaxShift
	start := klid - shift + 1
	end := klid + span
	qryKlid := " and klid >= ? and klid <= ?"
	op := func(c int) error {
		lrs = make([]float64, 0, span)
		reflrs = make(map[string][]float64)
		maxKlid, e := dbmap.SelectInt(`select max(klid) from kline_d_b where code = ?`, code)
		if e != nil {
			if sql.ErrNoRows != e {
				log.Printf(`#%d %s failed to query max klid, %+v`, c, code, e)
				return repeat.HintTemporary(e)
			}
			log.Printf(`%s no data in kline_d_b`, code)
			return repeat.HintStop(e)
		}
		maxk := int(maxKlid)

		if maxk < end {
			return repeat.HintStop(fmt.Errorf("%s ineligible for wcc pre-calculation: %d < %d", code, maxk, klid+span))
		}
		query, e := global.Dot.Raw("QUERY_BWR_DAILY")
		if e != nil {
			log.Printf(`#%d %s@%d-%d failed to load sql QUERY_BWR_DAILY, %+v`, c, code, start, end, e)
			return repeat.HintTemporary(e)
		}
		query = fmt.Sprintf(query, qryKlid)
		var klhist []*model.Quote
		_, e = dbmap.Select(&klhist, query, code, start, end)
		if e != nil {
			if sql.ErrNoRows != e {
				log.Printf(`#%d %s@%d-%d failed to load kline hist data: %+v`, c, code, start, end, e)
				return repeat.HintTemporary(e)
			}
			log.Printf(`%s@%d-%d no data in kline_d_b`, code, start, end)
			return repeat.HintStop(e)
		}
		if len(klhist) < span {
			e = fmt.Errorf("%s [severe]: some kline data between %d(exclusive) and %d may be missing. skipping",
				code, start, end)
			return repeat.HintStop(e)
		}
		// search for reference codes by matching dates
		var args []interface{}
		var dates []string
		for _, rc := range rcodes {
			args = append(args, rc)
		}
		for i, k := range klhist {
			if i >= shift {
				if k.Lr.Valid {
					lrs = append(lrs, k.Lr.Float64)
				} else {
					e = fmt.Errorf(`%s [severe] reference %s@%d %s log return is null. skipping`, code, k.Code, k.Klid, k.Date)
					repeat.HintStop(e)
				}
			}
			if i < len(klhist)-1 {
				dates = append(dates, k.Date)
				args = append(args, k.Date)
			}
		}
		args = append(args, len(dates))
		qry := fmt.Sprintf(`select code from kline_d_b where code in (?%s) and date in (?%s) `+
			`group by code having count(*) = ?`, strings.Repeat(",?", len(rcodes)-1), strings.Repeat(",?", len(klhist)-2))
		var frcodes []string
		_, e = dbmap.Select(&frcodes, qry, args...)
		if e != nil {
			if sql.ErrNoRows != e {
				log.Printf(`#%d %s@%d-%d failed to query reference codes: %+v`, c, code, start, end, e)
				return repeat.HintTemporary(e)
			}
			log.Printf(`%s@%d-%d no matching reference code`, code, start, end)
			return repeat.HintStop(e)
		}
		if len(frcodes) == 0 {
			log.Printf(`%s no available reference data between %d and %d`,
				code, start, end)
			return repeat.HintStop(e)
		}
		//query klines for frcode
		args = make([]interface{}, len(dates)+1)
		for i, d := range dates {
			args[i+1] = d
		}
		qry = fmt.Sprintf(`
			SELECT 
				t.code,
				t.klid,
				t.date,
				t.lr
			FROM
				kline_d_b t
			WHERE
				t.code = ? AND t.date IN (?%s)
			ORDER BY code , klid
		`, strings.Repeat(",?", len(args)-2))
	LOOPRCODES:
		for _, rc := range frcodes {
			args[0] = rc //replace code argument
			var rhist []*model.Quote
			_, e = dbmap.Select(&rhist, qry, args...)
			if e != nil {
				if sql.ErrNoRows != e {
					log.Printf(`#%d %s@%d-%d failed to load reference kline of %s: %+v`, c, code, start, end, rc, e)
					return repeat.HintTemporary(e)
				}
				log.Printf(`%s reference code %s has no available data between %s and %s, skipping this one`,
					code, rc, args[1], args[len(args)-1])
				continue
			}
			if len(rhist) != len(args)-1 {
				log.Printf(`%s reference code %s has missing data between %s and %s, skipping this one`,
					code, rc, args[1], args[len(args)-1])
				continue
			}
			rlrs := make([]float64, len(rhist))
			for i, k := range rhist {
				if k.Lr.Valid {
					rlrs[i] = k.Lr.Float64
				} else {
					log.Printf(`%s [severe] reference %s@%d %s log return is null. skipping`, code, k.Code, k.Klid, k.Date)
					continue LOOPRCODES
				}
			}
			reflrs[rc] = rlrs
		}
		return nil
	}

	e = repeat.Repeat(
		repeat.FnWithCounter(op),
		repeat.StopOnSuccess(),
		repeat.LimitMaxTries(conf.Args.DefaultRetry),
		repeat.WithDelay(
			repeat.FullJitterBackoff(500*time.Millisecond).WithMaxDelay(15*time.Second).Set(),
		),
	)

	if e != nil {
		log.Printf("%s@%d give up querying klines for wcc pre-calculation: %+v", code, klid, e)
	}

	return
}

//getRcodes4WccInfer fetches eligible reference codes based on prior data.
//the returned rcodes array may have 0 elements if no eligible data can be found.
func getRcodes4WccInfer(code string, klid int) (rcodes []string, e error) {
	shift := conf.Args.Sampler.CorlTimeShift
	steps := conf.Args.Sampler.CorlTimeSteps
	start := klid - steps - shift + 1
	qryKlid := " and klid >= ? and klid <= ?"
	op := func(c int) error {
		log.Printf("#%d getting rcodes for %s@%d", c, code, klid)
		rcodes = make([]string, 0, 64)
		query, e := global.Dot.Raw("QUERY_BWR_DAILY")
		if e != nil {
			log.Printf(`#%d %s@%d failed to load sql QUERY_BWR_DAILY, %+v`, c, code, klid, e)
			return repeat.HintTemporary(e)
		}
		query = fmt.Sprintf(query, qryKlid)
		var klhist []*model.Quote
		_, e = dbmap.Select(&klhist, query, code, start, klid)
		if e != nil {
			if sql.ErrNoRows != e {
				log.Printf(`#%d %s@%d-%d failed to load kline hist data: %+v`, c, code, start, klid, e)
				return repeat.HintTemporary(e)
			}
			log.Printf(`%s@%d-%d no data in kline_d_b`, code, start, klid)
			return repeat.HintStop(e)
		}
		if len(klhist) < steps+shift {
			e = fmt.Errorf("%s [severe]: some kline data between %d and %d may be missing. skipping",
				code, start, klid)
			return repeat.HintStop(e)
		}
		// search for reference codes by matching dates
		args := []interface{}{code}
		for _, k := range klhist {
			args = append(args, k.Date)
		}
		args = append(args, len(klhist))
		qry := fmt.Sprintf(`select code from kline_d_b where code <> ? and date in (%s%s) `+
			`group by code having count(*) = ?`, "?", strings.Repeat(",?", len(klhist)-1))
		_, e = dbmap.Select(&rcodes, qry, args...)
		if e != nil {
			if sql.ErrNoRows != e {
				log.Printf(`#%d %s@%d-%d failed to query reference codes, %+v`, c, code, start, klid, e)
				return repeat.HintTemporary(e)
			}
			log.Printf(`%s@%d-%d no matching reference code`, code, start, klid)
			return repeat.HintStop(e)
		}
		if len(rcodes) < 2 {
			log.Printf(`%s insufficient reference code between %d and %d: %d`,
				code, start, klid, len(rcodes))
			return repeat.HintStop(e)
		}
		return nil
	}

	e = repeat.Repeat(
		repeat.FnWithCounter(op),
		repeat.StopOnSuccess(),
		repeat.LimitMaxTries(conf.Args.DefaultRetry),
		repeat.WithDelay(
			repeat.FullJitterBackoff(500*time.Millisecond).WithMaxDelay(10*time.Second).Set(),
		),
	)

	if e != nil {
		log.Printf("%s %d failed to get wcc reference codes for inference: %+v", code, klid, e)
		return nil, e
	}

	return rcodes, nil
}

func fileExporter(localPath, rbase string, fec <-chan *ExpJob, fuc chan<- *fileUploadJob, fewg, fuwg *sync.WaitGroup) {
	defer fewg.Done()
	fwwg := new(sync.WaitGroup)
	pl := int(float64(runtime.NumCPU()) * 0.8)
	for i := 0; i < pl; i++ {
		fwwg.Add(1)
		go fileExpWorker(localPath, rbase, fec, fuc, fwwg)
	}
	fwwg.Wait()
	if fuc != nil {
		close(fuc)
		fuwg.Wait()
		if e := gcsClient.Close(); e != nil {
			log.Printf("failed to close gcs client: %+v", e)
		}
		// clean empty volume sub-folders
		dirs, err := ioutil.ReadDir(localPath)
		if err != nil {
			log.Printf("failed to read local path %s, unable to clean sub-folders: %+v", localPath, err)
			return
		}
		for _, d := range dirs {
			if d.IsDir() && strings.HasPrefix(d.Name(), "vol_") {
				p := filepath.Join(localPath, d.Name())
				files, err := ioutil.ReadDir(p)
				if err != nil {
					log.Printf("failed to read local path %s, unable to clean this sub-folder: %+v", p, err)
					continue
				}
				removable := true
				for _, f := range files {
					if !f.IsDir() && strings.HasSuffix(f.Name(), ".json.gz") {
						removable = false
						break
					}
				}
				if removable {
					log.Printf("removing empty volume folder: %s", p)
					os.Remove(p)
				}
			}
		}
	}
}

func fileExpWorker(localPath, rbase string, fec <-chan *ExpJob, fuc chan<- *fileUploadJob, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Println("file export worker started")
	step := conf.Args.Sampler.CorlTimeSteps
	shift := conf.Args.Sampler.CorlTimeShift
	limit := step + shift
	for job := range fec {
		code := job.Code
		klid := job.Klid
		ex, p, e := util.FileExists(localPath, fmt.Sprintf("%s_%d.json.gz", code, klid), true, true)
		if e != nil {
			panic(e)
		}
		if ex {
			log.Printf("%s already exists.", p)
			continue
		}
		rcodes := job.Rcodes
		frcodes := make([]string, 0, len(rcodes))
		s := int(math.Max(0., float64(klid-step+1-shift)))
		feats := make([][][]float64, 0, len(rcodes))
		seqlens := make([]int, 0, len(rcodes))
		for _, rc := range rcodes {
			batch, seqlen, e := getSeries(code, rc, s, klid, limit)
			if e != nil {
				log.Panicf("failed to get series for %s and %s, exiting program", code, rc)
			}
			if len(batch) == 0 {
				log.Printf("no inference data for %s and %s", code, rc)
				continue
			}
			frcodes = append(frcodes, rc)
			feats = append(feats, batch)
			seqlens = append(seqlens, seqlen)
		}
		if len(feats) == 0 {
			log.Printf("no inference data for %s", code)
			continue
		}
		// write lv9 gzipped json file, send it to fuc if the channel is not nil
		dir, e := syncVolDir(localPath)
		if e != nil {
			log.Panicf("%s failed to read volume directory, exiting program", code)
		}
		cif := &CorlInferFile{
			Code:     code,
			Klid:     klid,
			Refs:     frcodes,
			Features: feats,
			SeqLens:  seqlens,
		}
		path := filepath.Join(dir, fmt.Sprintf("%s_%d", code, klid))
		path, e = util.WriteJSONFile(cif, path, true)
		if e != nil {
			log.Panicf("%s failed to export json file %s, exiting program: %+v", code, path, e)
		}
		log.Printf("json file exported: %s", path)
		if fuc != nil {
			sep := os.PathSeparator
			pattern := fmt.Sprintf(`.*(vol_\d*%[1]c[^%[1]c]*)`, sep)
			r := regexp.MustCompile(pattern).FindStringSubmatch(path)
			var gcsDest string
			if len(r) > 0 {
				gcsDest = filepath.Join(rbase, r[len(r)-1])
			} else {
				gcsDest = filepath.Join(rbase, filepath.Base(path))
			}
			fuc <- &fileUploadJob{
				localFile: path,
				dest:      gcsDest,
			}
		}
	}
}

func syncVolDir(localPath string) (dir string, e error) {
	volLock.Lock()
	defer volLock.Unlock()
	volSize := conf.Args.Sampler.VolSize
	if curVolPath == "" || curVolSize >= volSize {
		newPath := ""
		c := 0
		for {
			curVolNo++
			volDir := fmt.Sprintf("vol_%d", curVolNo)
			ex := false
			ex, newPath, e = util.FileExists(localPath, volDir, false, true)
			if e != nil {
				return
			}
			if ex {
				c, e = util.NumOfFiles(newPath, ".*\\.json\\.gz", false)
				if e != nil {
					return
				}
				if c < volSize {
					break
				}
			} else {
				newPath = filepath.Join(localPath, volDir)
				if e = util.MkDirAll(newPath, 0666); e != nil {
					return
				}
				break
			}
		}
		curVolPath = newPath
		curVolSize = c
	}
	curVolSize++
	return curVolPath, nil
}

func getSeries(code, rcode string, start, end, limit int) (series [][]float64, seqlen int, err error) {
	qk, qd := getFeatQuery()
	step := conf.Args.Sampler.CorlTimeSteps
	shift := conf.Args.Sampler.CorlTimeShift
	op := func(c int) error {
		if c > 0 {
			series = make([][]float64, 0, step)
			seqlen = 0
			log.Printf("retry #%d getting feature batch [%s, %s, %d, %d]", c, code, rcode, start, end)
		}
		rows, e := dbmap.Query(qk, code, code, start, end, limit)
		defer rows.Close()
		if e != nil {
			log.Printf("failed to query by klid [%s,%d,%d]: %+v", code, start, end, e)
			return repeat.HintTemporary(e)
		}
		cols, e := rows.Columns()
		unitFeatLen := len(cols) - 1
		featSize := unitFeatLen * 2
		shiftFeatSize := featSize * (shift + 1)
		count, rcount := 0, 0
		dates := make([]string, 0, 16)
		table, rtable := make([][]float64, 0, 16), make([][]float64, 0, 16)
		for ; rows.Next(); count++ {
			row := make([]float64, unitFeatLen)
			table = append(table, row)
			vals := make([]interface{}, len(cols))
			for i := range vals {
				vals[i] = new(interface{})
			}
			if e := rows.Scan(vals...); e != nil {
				log.Printf("failed to scan result set [%s,%d,%d]: %+v", code, start, end, e)
				return repeat.HintTemporary(e)
			}
			if d, ok := vals[0].(*interface{}); ok {
				dates = append(dates, string((*d).([]uint8)))
			} else {
				return repeat.HintStop(
					fmt.Errorf("[%s,%d,%d] column type conversion error, unable to parse date string", code, start, end),
				)
			}
			for i := 0; i < unitFeatLen; i++ {
				if f, ok := vals[i+1].(*interface{}); ok {
					row[i] = (*f).(float64)
				} else {
					return repeat.HintStop(
						fmt.Errorf("[%s,%d,%d] column type conversion error, unable to parse float64", code, start, end),
					)
				}
			}
		}
		if e := rows.Err(); e != nil {
			log.Printf("found error scanning result set [%s,%d,%d]: %+v", code, start, end, e)
			return repeat.HintTemporary(e)
		}
		qdates := util.Join(dates, ",", true)
		rRows, e := dbmap.Query(fmt.Sprintf(qd, qdates), rcode, rcode, limit)
		defer rRows.Close()
		if e != nil {
			log.Printf("failed to query by dates [%s,%s]: %+v", code, rcode, e)
			return repeat.HintTemporary(e)
		}
		for ; rRows.Next(); rcount++ {
			row := make([]float64, unitFeatLen)
			rtable = append(rtable, row)
			vals := make([]interface{}, len(cols))
			for i := range vals {
				vals[i] = new(interface{})
			}
			if e := rRows.Scan(vals...); e != nil {
				log.Printf("failed to scan rcode result set [%s]: %+v", rcode, e)
				return repeat.HintTemporary(e)
			}
			for i := 0; i < unitFeatLen; i++ {
				if f, ok := vals[i+1].(*interface{}); ok {
					row[i] = (*f).(float64)
				} else {
					return repeat.HintStop(
						fmt.Errorf("[%s,%d,%d] column type conversion error, unable to parse float64", code, start, end),
					)
				}
			}
		}
		if e := rRows.Err(); e != nil {
			log.Printf("found error scanning rcode result set [%s]: %+v", rcode, e)
			return repeat.HintTemporary(e)
		}
		if count != rcount {
			e = errors.New(fmt.Sprintf("rcode[%s] prior data size %d != code[%s]: %d", rcode, rcount, code, count))
			return repeat.HintStop(e)
		}
		if count < limit {
			e = errors.New(fmt.Sprintf("[%s,%s,%d,%d] insufficient data. get %d, %d required",
				code, rcode, start, end, count, limit))
			return repeat.HintStop(e)
		}
		series = make([][]float64, step)
		for st := shift; st < count; st++ {
			feats := make([]float64, 0, shiftFeatSize)
			for sf := shift; sf >= 0; sf-- {
				i := st - sf
				for j := 0; j < unitFeatLen; j++ {
					feats = append(feats, table[i][j])
				}
				for j := 0; j < unitFeatLen; j++ {
					feats = append(feats, rtable[i][j])
				}
			}
			series[st-shift] = feats
		}
		seqlen = count - shift
		return nil
	}

	err = repeat.Repeat(
		repeat.FnWithCounter(op),
		repeat.StopOnSuccess(),
		repeat.LimitMaxTries(conf.Args.DefaultRetry),
		repeat.WithDelay(
			repeat.FullJitterBackoff(500*time.Millisecond).WithMaxDelay(10*time.Second).Set(),
		),
	)

	if err != nil {
		log.Printf("failed to get series [%s, %s, %d, %d]: %+v", code, rcode, start, end, err)
	}

	return
}

func getFeatQuery() (qk, qd string) {
	if qryKline != "" && qryDate != "" {
		return qryKline, qryDate
	}
	ftQryInit.Do(func() {
		tmpl, e := dot.Raw("CORL_FEAT_QUERY_TMPL")
		if e != nil {
			log.Panicf("failed to load sql CORL_FEAT_QUERY_TMPL:%+v", e)
		}
		var strs []string
		cols := conf.Args.Sampler.FeatureCols
		for _, c := range cols {
			strs = append(strs, fmt.Sprintf("(d.%[1]s-s.%[1]s_mean)/s.%[1]s_std %[1]s,", c))
		}
		pkline := strings.Join(strs, " ")
		pkline = pkline[:len(pkline)-1] // strip last comma

		strs = make([]string, 0, 8)
		statsTmpl := `
			MAX(CASE
				WHEN t.fields = '%[1]s' THEN t.mean
				ELSE NULL 
			END) AS %[1]s_mean, 
			MAX(CASE
				WHEN t.fields = '%[1]s' THEN t.std
				ELSE NULL 
			END) AS %[1]s_std,`
		for _, c := range cols {
			strs = append(strs, fmt.Sprintf(statsTmpl, c))
		}
		stats := strings.Join(strs, " ")
		stats = stats[:len(stats)-1] // strip last comma

		qryKline = fmt.Sprintf(tmpl, pkline, stats, " AND d.klid BETWEEN ? AND ? ")
		qryDate = fmt.Sprintf(tmpl, pkline, stats, " AND d.date in (%s)")
	})
	return qryKline, qryDate
}

func uploadToGCS(ch <-chan *fileUploadJob, wg *sync.WaitGroup, nocache bool) {
	defer wg.Done()
	log.Println("gcs upload worker started")
	for job := range ch {
		// gcs api may have utilized retry mechanism already.
		// see https://godoc.org/cloud.google.com/go/storage
		op := func(c int) error {
			log.Printf("#%d uploading %s to %s", c, job.localFile, job.dest)
			ctx := context.Background()
			client, err := gcsClient.Get()
			if err != nil {
				log.Printf("failed to create gcs client when uploading %s: %+v", job.localFile, err)
				return repeat.HintTemporary(err)
			}
			timeout := time.Duration(conf.Args.GCS.Timeout) * time.Second
			// check if target object exists
			obj := client.Bucket(conf.Args.GCS.Bucket).Object(job.dest)
			tctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			rc, err := obj.NewReader(tctx)
			defer func() {
				if rc != nil {
					rc.Close()
				}
			}()
			if err != nil {
				if err != storage.ErrObjectNotExist {
					log.Printf("failed to check existence for %s: %+v", job.dest, err)
					return repeat.HintTemporary(err)
				}
			} else {
				// file already exists
				return nil
			}
			file, err := os.Open(job.localFile)
			if err != nil {
				log.Printf("failed to open %s: %+v", job.localFile, err)
				return repeat.HintTemporary(err)
			}
			wc := obj.NewWriter(tctx)
			wc.ContentType = "application/json"
			// wc.ACL = []storage.ACLRule{{Entity: storage.AllUsers, Role: storage.RoleReader}}
			if _, err := io.Copy(wc, bufio.NewReader(file)); err != nil {
				log.Printf("failed to upload %s: %+v", job.localFile, err)
				return repeat.HintTemporary(err)
			}
			if err := wc.Close(); err != nil {
				log.Printf("failed to upload %s: %+v", job.localFile, err)
				return repeat.HintTemporary(err)
			}
			log.Printf("%s uploaded", job.dest)
			if nocache {
				err = os.Remove(job.localFile)
				if err != nil {
					log.Printf("failed to remove %s: %+v", job.localFile, err)
				}
			}
			return nil
		}

		err := repeat.Repeat(
			repeat.FnWithCounter(op),
			repeat.StopOnSuccess(),
			repeat.LimitMaxTries(conf.Args.DefaultRetry),
			repeat.WithDelay(
				repeat.FullJitterBackoff(500*time.Millisecond).WithMaxDelay(15*time.Second).Set(),
			),
		)

		if err != nil {
			log.Printf("failed to upload file %s to gcs: %+v", job.localFile, err)
		}
	}
}

//getPcalJobs fetchs kline data not in stockrel with non-blank value
func getPcalJobs() (jobs []*pcaljob, e error) {
	log.Println("querying klines for candidate...")
	sklid := conf.Args.Sampler.CorlPrior
	_, e = dbmap.Select(&jobs, `
		SELECT 
			t.code code, t.date date, t.klid klid
		FROM
			(SELECT 
				code, date, klid
			FROM
				kline_d_b
			WHERE
				klid >= ?
			ORDER BY code , klid) t
		WHERE
			(code , klid) NOT IN (SELECT 
					code, klid
				FROM
					stockrel
				WHERE
					rcode_pos_hs IS NOT NULL)
	`, sklid)
	if e != nil {
		log.Printf("failed to query pcal jobs: %+v", e)
		e = errors.WithStack(e)
	}
	return
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
	span := conf.Args.Sampler.CorlSpan
	syear := conf.Args.Sampler.CorlStartYear
	portion := conf.Args.Sampler.CorlPortion
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
	start, end := 0, maxk-span+1
	if len(syear) > 0 {
		sklid, err := dbmap.SelectInt(`select min(klid) from kline_d_b where code = ? and date >= ?`, code, syear)
		if err != nil {
			log.Printf(`%s failed to query min klid, %+v`, code, err)
			return
		}
		if int(sklid)+1 < prior {
			start = prior - shift
		} else {
			start = int(sklid)
		}
	} else if prior > 0 {
		start = prior - shift
	}
	var klids []int
	dbmap.Select(&klids, `SELECT 
								klid
							FROM
								kline_d_b
							WHERE
								code = ?
								AND klid BETWEEN ? AND ?
								AND klid NOT IN (SELECT DISTINCT
									klid
								FROM
									wcc_trn
								WHERE
									code = ?)`,
		code, start, end, code,
	)
	num := int(float64(len(klids)) * portion)
	if num == 0 {
		log.Printf("%s insufficient data for wcc_trn sampling", code)
		return
	}
	sidx := rand.Perm(len(klids))[:num]
	log.Printf("%s selected %d/%d klids from kline_d_b", code, num, len(klids))
	retry := false
	var e error
	var wccs []*model.WccTrn
	for _, idx := range sidx {
		klid := klids[idx]
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
	span := conf.Args.Sampler.CorlSpan
	shift := conf.Args.Sampler.WccMaxShift
	minReq := conf.Args.Sampler.PriorLength
	prior := conf.Args.Sampler.CorlPrior
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
	valueArgs := make([]interface{}, 0, len(ws)*9)
	for _, e := range ws {
		valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?)")
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
	stmt := fmt.Sprintf("INSERT INTO wcc_trn (code,klid,date,rcode,corl,"+
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

func collectStockRels(wg *sync.WaitGroup, ch <-chan *stockrelDBJob) {
	defer wg.Done()
	log.Println("db worker started")
	size := 64
	wait := 15 * time.Second
	bucket := make([]*model.StockRel, 0, size)
	ticker := time.NewTicker(time.Second * 5)
	var lastSaved time.Time
	for {
		select {
		case <-ticker.C:
			if len(bucket) > 0 && time.Since(lastSaved) >= wait {
				saveStockRel(bucket...)
				bucket = make([]*model.StockRel, 0, size)
			}
		case job, ok := <-ch:
			if ok {
				bucket = append(bucket, job.stockrel)
				if len(bucket) >= size {
					saveStockRel(bucket...)
					bucket = make([]*model.StockRel, 0, size)
					lastSaved = time.Now()
				}
			} else {
				//channel has been closed
				ticker.Stop()
				if len(bucket) > 0 {
					saveStockRel(bucket...)
					bucket = nil
				}
				break
			}
		}
	}
}

func saveStockRel(rels ...*model.StockRel) {
	if len(rels) == 0 {
		return
	}
	log.Printf("saving stockrel data, size: %d", len(rels))
	valueHolders := make([]string, 0, len(rels))
	valueArgs := make([]interface{}, 0, len(rels)*16)
	cols := []string{"code", "klid"}
	valueUpdates := make([]string, 0, 16)
	addcol := func(i int, cn string, f interface{}, num *int) {
		valid := false
		switch f.(type) {
		case sql.NullString:
			valid = f.(sql.NullString).Valid
		case sql.NullFloat64:
			valid = f.(sql.NullFloat64).Valid
		default:
			log.Panicf("unsupported sql type: %+v", reflect.TypeOf(f))
		}
		if valid {
			valueArgs = append(valueArgs, f)
			if i == 0 {
				cols = append(cols, cn)
				valueUpdates = append(valueUpdates, fmt.Sprintf("%[1]s=values(%[1]s)", cn))
			}
			*num++
		}
	}
	for i, r := range rels {
		numFields := 2
		valueArgs = append(valueArgs, r.Code)
		valueArgs = append(valueArgs, r.Klid)
		addcol(i, "date", r.Date, &numFields)
		addcol(i, "neg_corl", r.NegCorl, &numFields)
		addcol(i, "neg_corl_hs", r.NegCorlHs, &numFields)
		addcol(i, "pos_corl", r.PosCorl, &numFields)
		addcol(i, "pos_corl_hs", r.PosCorlHs, &numFields)
		addcol(i, "rcode_neg", r.RcodeNeg, &numFields)
		addcol(i, "rcode_neg_hs", r.RcodeNegHs, &numFields)
		addcol(i, "rcode_pos", r.RcodePos, &numFields)
		addcol(i, "rcode_pos_hs", r.RcodePosHs, &numFields)
		addcol(i, "udate", r.Udate, &numFields)
		addcol(i, "utime", r.Utime, &numFields)
		holders := make([]string, numFields)
		for i := range holders {
			holders[i] = "?"
		}
		holderString := fmt.Sprintf("(%s)", strings.Join(holders, ","))
		valueHolders = append(valueHolders, holderString)
	}
	stmt := fmt.Sprintf("INSERT INTO stockrel (%s) VALUES %s on duplicate key update %s",
		strings.Join(cols, ","),
		strings.Join(valueHolders, ","),
		strings.Join(valueUpdates, ","))
	code := rels[0].Code
	klid := rels[0].Klid
	var e error
	op := func(c int) error {
		if c > 0 {
			log.Printf("retry #%d saving stockrel for %s@%d, size %d", c, code, klid, len(rels))
		}
		_, e = dbmap.Exec(stmt, valueArgs...)
		if e != nil {
			log.Printf("failed to save stockrel for %s@%d: %+v", code, klid, e)
			return repeat.HintTemporary(e)
		}
		return nil
	}

	e = repeat.Repeat(
		repeat.FnWithCounter(op),
		repeat.StopOnSuccess(),
		repeat.LimitMaxTries(conf.Args.DefaultRetry),
		repeat.WithDelay(
			repeat.FullJitterBackoff(500*time.Millisecond).WithMaxDelay(15*time.Second).Set(),
		),
	)

	if e != nil {
		log.Printf("give up saving stockrel for %s@%d size %d: %+v", code, klid, len(rels), e)
	}
}
