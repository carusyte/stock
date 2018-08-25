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
	volLock           sync.RWMutex
	ftQryInit         sync.Once
	qryKline, qryDate string
)

type wccTrnDBJob struct {
	stock *model.Stock
	fin   int //-1:abort, 0:unfinished, 1:finished
	wccs  []*model.WccTrn
}

type pcaljob struct {
	Code string
	Klid int
}

//ExpJob stores wcc inference file export job information.
type ExpJob struct {
	Code   string
	Klid   int
	Date   string
	Rcodes []string
}

type fileUploadJob struct {
	localFile string
	dest      string
}

//Pcal pre-calculates future wcc value using non-reinstated daily kline data and updates stockrel table.
func Pcal(expInferFile, upload, nocache bool, localPath string) {
	// TODO: realize Pcal()
	jobs, e := getPcalJobs()
	if e != nil || len(jobs) <= 0 {
		return
	}
	var expch chan<- *ExpJob
	var expwg *sync.WaitGroup
	if expInferFile {
		expch, expwg = ExpInferFile(localPath, upload, nocache)
	}
	// make db job channel & waitgroup, start db goroutine

	// make job channel & waitgroup, start calculation goroutine
	// iterate through qualified kline data, create wcc calculation job instance and push it to job channel
	// close job channel, wait for job completion
	// close db job channel wait for db job completion
	// close exp channel wait for exp job completion
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
func ExpInferFile(localPath string, upload, nocache bool) (fec chan<- *ExpJob, fewg *sync.WaitGroup) {
	var fuc chan *fileUploadJob
	var fuwg *sync.WaitGroup
	if upload {
		fuc = make(chan *fileUploadJob, math.MaxInt32)
		fuwg = new(sync.WaitGroup)
		for i := 0; i < conf.Args.GCS.Connection; i++ {
			fuwg.Add(1)
			go procInferFileUpload(fuc, fuwg, nocache)
		}
	}
	// TODO realize me
	fileExpCh := make(chan *ExpJob, conf.Args.Concurrency)
	fec = fileExpCh
	fewg = new(sync.WaitGroup)
	fewg.Add(1)
	go fileExporter(localPath, fileExpCh, fuc, fewg, fuwg)
	return
}

func fileExporter(localPath string, fec <-chan *ExpJob, fuc chan<- *fileUploadJob, fewg, fuwg *sync.WaitGroup) {
	defer fewg.Done()
	fwwg := new(sync.WaitGroup)
	pl := int(float64(runtime.NumCPU()) * 0.8)
	for i := 0; i < pl; i++ {
		fwwg.Add(1)
		go fileExpWorker(localPath, fec, fuc, fwwg)
	}
	fwwg.Wait()
	if fuc != nil {
		close(fuc)
		fuwg.Wait()
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
					os.Remove(p)
				}
			}
		}
	}
}

func fileExpWorker(localPath string, fec <-chan *ExpJob, fuc chan<- *fileUploadJob, wg *sync.WaitGroup) {
	//TODO realize me
	defer wg.Done()
	step := conf.Args.Sampler.CorlTimeSteps
	shift := conf.Args.Sampler.CorlTimeShift
	limit := step + shift
	for job := range fec {
		name := fmt.Sprintf("%s_%d.json.gz")
		exists, e := fileExists(localPath, name)
		if e != nil {
			panic(e)
		}
		if exists {
			continue
		}
		//TODO query inference klines
		code := job.Code
		klid := job.Klid
		rcodes := job.Rcodes
		volSize := conf.Args.Sampler.VolSize
		s := int(math.Max(0., float64(klid-step+1-shift)))
		for _, rc := range rcodes {
			batch, seqlen, e := getSeries(code, rc, s, klid, limit)
		}
		tmp := fmt.Sprintf("%s_%d.json.tmp")
	}
}

func getSeries(code, rcode string, start, end, limit int) (series [][]float64, seqlen int, err error) {
	qk, qd := getFeatQuery()
	step := conf.Args.Sampler.CorlTimeSteps
	shift := conf.Args.Sampler.CorlTimeShift
	op := func(c int) error {
		if c > 0 {
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
			frow := make([]float64, unitFeatLen)
			table[count] = frow
			vals := make([]interface{}, len(cols))
			// for i := range vals{
			// 	vals[i] = new(interface{})
			// }
			if e := rows.Scan(vals...); e != nil {
				log.Printf("failed to scan result set [%s,%d,%d]: %+v", code, start, end, e)
				return repeat.HintTemporary(e)
			}
			dates = append(dates, vals[0].(string))
			for i := 0; i < unitFeatLen; i++ {
				frow[i] = vals[i+1].(float64)
			}
		}
		if e := rows.Err(); e != nil {
			log.Printf("found error scanning result set [%s,%d,%d]: %+v", code, start, end, e)
			return repeat.HintTemporary(e)
		}
		qdates := util.Join(dates, ",", true)
		qd = fmt.Sprintf(qd, qdates)
		rRows, e := dbmap.Query(qd, rcode, rcode, limit)
		defer rRows.Close()
		if e != nil {
			log.Printf("failed to query by dates [%s,%s]: %+v", code, rcode, e)
			return repeat.HintTemporary(e)
		}
		for ; rRows.Next(); rcount++ {
			frow := make([]float64, unitFeatLen)
			rtable[rcount] = frow
			vals := make([]interface{}, len(cols))
			// for i := range vals{
			// 	vals[i] = new(interface{})
			// }
			if e := rows.Scan(vals...); e != nil {
				log.Printf("failed to scan rcode result set [%s]: %+v", rcode, e)
				return repeat.HintTemporary(e)
			}
			for i := 0; i < unitFeatLen; i++ {
				frow[i] = vals[i+1].(float64)
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
				for j := 0; j <= unitFeatLen; j++ {
					feats = append(feats, table[i][j])
				}
				for j := 0; j <= unitFeatLen; j++ {
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
			repeat.FullJitterBackoff(500*time.Millisecond).Set(),
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
	ftQryInit.Do(initFeatQuery)
	return qryKline, qryDate
}

func initFeatQuery() {
	tmpl, e := dot.Raw("CORL_FEAT_QUERY_TMPL")
	if e != nil {
		log.Panicf("failed to load sql CORL_FEAT_QUERY_TMPL:%+v", e)
	}
	var strs []string
	cols := conf.Args.Sampler.FeatureCols
	for _, c := range cols {
		strs = append(strs, fmt.Sprintf("(d.%[0]s-s.%[0]s_mean)/s.%[0]s_std %[0]s,", c))
	}
	pkline := strings.Join(strs, " ")
	pkline = pkline[:len(pkline)-1] // strip last comma

	strs = make([]string, 0, 8)
	statsTmpl := `
        MAX(CASE
            WHEN t.fields = '%[0]s' THEN t.mean
            ELSE NULL 
        END) AS %[0]s_mean, 
        MAX(CASE
            WHEN t.fields = '%[0]s' THEN t.std
            ELSE NULL 
        END) AS %[0]s_std,
	`
	for _, c := range cols {
		strs = append(strs, fmt.Sprintf(statsTmpl, c))
	}
	stats := strings.Join(strs, " ")
	stats = stats[:len(stats)-1] // strip last comma

	qryKline = fmt.Sprintf(tmpl, pkline, stats, " AND d.klid BETWEEN ? AND ? ")
	qryDate = fmt.Sprintf(tmpl, pkline, stats, " AND d.date in (%s)")
}

func fileExists(path, name string) (bool, error) {
	paths := []string{filepath.Join(path, name)}
	dirs, err := ioutil.ReadDir(path)
	if err != nil {
		log.Printf("failed to read content from %s: %+v", path, err)
		return false, errors.WithStack(err)
	}
	for _, d := range dirs {
		if d.IsDir() {
			paths = append(paths, filepath.Join(path, d.Name(), name))
		}
	}
	for _, p := range paths {
		_, err = os.Stat(p)
		if err != nil {
			if !os.IsNotExist(err) {
				log.Printf("failed to check existence of %s : %+v", p, err)
				return false, errors.WithStack(err)
			}
		} else {
			log.Printf("%s already exists.", p)
			return true, nil
		}
	}
	return false, nil
}

func procInferFileUpload(ch <-chan *fileUploadJob, wg *sync.WaitGroup, nocache bool) {
	defer wg.Done()
	for job := range ch {
		op := func(c int) error {
			if c > 0 {
				log.Printf("retry #%d uploading %s to %s", c, job.localFile, job.dest)
			}
			ctx := context.Background()
			client, err := storage.NewClient(ctx)
			if err != nil {
				log.Printf("failed to create gcs client when uploading %s: %+v", job.localFile, err)
				return repeat.HintTemporary(err)
			}
			defer func() {
				if err := client.Close(); err != nil {
					log.Printf("failed to close client after uploading %s: %+v", job.localFile, err)
				}
			}()
			// check if target object exists
			obj := client.Bucket(conf.Args.GCS.Bucket).Object(job.dest)
			rc, err := obj.NewReader(ctx)
			defer rc.Close()
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
			wc := obj.NewWriter(ctx)
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
				repeat.FullJitterBackoff(500*time.Millisecond).Set(),
			),
		)

		if err != nil {
			log.Printf("failed to upload file %s to gcs: %+v", job.localFile, err)
		}
	}
}

//getPcalJobs fetchs kline data not in stockrel with non-blank value
func getPcalJobs() (jobs []*pcaljob, e error) {
	sklid := conf.Args.Sampler.CorlPrior
	_, e = dbmap.Select(&jobs, `
		SELECT 
			t.code code, t.klid klid
		FROM
			(SELECT 
				code, klid
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
	//skip existing records
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
