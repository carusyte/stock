package sampler

import (
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/carusyte/stock/conf"

	"github.com/pkg/errors"
)

//CorlTab type, such as XcorlTrn, WccTrn, etc.
type CorlTab string
type tagJob struct {
	flag  string
	bno   int
	uuids []int
	done  bool
}

const (
	//XcorlTrn Cross Correlation Training
	XcorlTrn CorlTab = "xcorl_trn"
	//WccTrn Warping Correlation Coefficient Training
	WccTrn CorlTab = "wcc_trn"
)

//TagCorlTrn tags the sampled correlation training table (such as xcorl_trn or wcc_trn) data
//with specified flag as prefix by randomly and evenly selecting untagged samples.
func TagCorlTrn(table CorlTab, flag string, erase bool) (e error) {
	log.Printf("tagging %v for dataset %s...", table, flag)
	startno := 0
	vflag := ""
	switch flag {
	case TrainFlag:
		vflag = "TR"
	case TestFlag:
		vflag = "TS"
	}
	if erase {
		// clear already tagged data
		log.Printf("cleansing existing %s tag...", vflag)
		usql := fmt.Sprintf(`update %v set flag = null, bno=null where flag = ?`, table)
		_, e = dbmap.Exec(usql, vflag)
		if e != nil {
			return errors.WithStack(e)
		}
	} else {
		// load existent max tag number
		q := "SELECT  " +
			"    MAX(distinct bno) AS max_bno " +
			"FROM " +
			"    wcc_trn " +
			"WHERE " +
			"    flag = ?"
		sno, e := dbmap.SelectNullInt(q, vflag)
		if e != nil {
			return errors.WithStack(e)
		}
		if sno.Valid {
			startno = int(sno.Int64)
			log.Printf("continue with batch number: %d", startno+1)
		} else {
			log.Printf("no existing data for %s set. batch no will be starting from %d", vflag, startno+1)
		}
	}
	// tag group * batch_size of target data from untagged records randomly and evenly
	log.Println("loading untagged records...")
	untagged, e := getUntaggedCorls(table)
	if e != nil {
		return e
	}
	total := len(untagged)
	log.Printf("total of untagged records: %d", total)
	bsize := conf.Args.Sampler.TestSetBatchSize
	if flag == TrainFlag {
		bsize = conf.Args.Sampler.TrainSetBatchSize
	}
	segment := int(float64(total) / float64(bsize))
	rem := int(total) % bsize
	//take care of remainder
	remOwn := make(map[int]bool)
	if rem > 0 {
		perm := rand.Perm(bsize)
		for i := 0; i < rem; i++ {
			remOwn[perm[i]] = true
		}
	}
	offset := 0
	var batches int
	if flag == TestFlag {
		batches = conf.Args.Sampler.TestSetGroups
	} else {
		batches = segment
	}
	grps := make([][]int, batches)
	for i := 0; i < bsize; i++ {
		limit := segment
		if _, ok := remOwn[i]; ok {
			limit++
		}
		var uuids []int
		if i < bsize-1 {
			uuids = untagged[offset : offset+limit]
		} else {
			uuids = untagged[offset:]
		}
		log.Printf("%d/%d size: %d", i+1, bsize, len(uuids))
		offset += limit
		log.Printf("generating permutations of size %d...", len(uuids))
		perm := rand.Perm(len(uuids))
		n := int(math.Min(float64(len(perm)), float64(batches)))
		for j := 0; j < n; j++ {
			grps[j] = append(grps[j], uuids[perm[j]])
		}
	}
	untagged = nil
	remOwn = nil
	var wg, wgr sync.WaitGroup
	chjob := make(chan *tagJob, conf.Args.DBQueueCapacity)
	chr := make(chan *tagJob, conf.Args.DBQueueCapacity)
	ngrps := len(grps)
	//start cpu_num/2 of goroutines
	pll := int(math.Max(float64(runtime.NumCPU())*0.5, 1.0))
	wgr.Add(1)
	go collectTagJob(ngrps, &wgr, chr)
	for i := 0; i < pll; i++ {
		wg.Add(1)
		go procTagJob(table, &wg, chjob, chr)
	}
	for i := 0; i < len(grps); i++ {
		chjob <- &tagJob{
			flag:  vflag,
			bno:   startno + i + 1,
			uuids: grps[i],
		}
	}
	close(chjob)
	wg.Wait()
	close(chr)
	wgr.Wait()

	log.Printf("%v %s set tagged: %d", table, flag, ngrps)
	return nil
}

func collectTagJob(ngrps int, wgr *sync.WaitGroup, chr chan *tagJob) {
	defer wgr.Done()
	i := 0
	f := 0
	for j := range chr {
		//report progres
		i++
		status := "done"
		if !j.done {
			f++
			status = "failed"
		}
		prog := float64(float64(i)/float64(ngrps)) * 100.
		log.Printf("job %s_%d %s, progress: %d/%d(%.3f%%), failed:%d", j.flag, j.bno, status, i, ngrps, prog, f)
	}
}

func procTagJob(table CorlTab, wg *sync.WaitGroup, chjob chan *tagJob, chr chan *tagJob) {
	defer wg.Done()
	var e error
	for j := range chjob {
		var strg []string
		for _, i := range j.uuids {
			strg = append(strg, strconv.Itoa(i))
		}
		uuids := strings.Join(strg, ",")
		flag, bno := j.flag, j.bno
		log.Printf("tagging %s,%d size: %d", flag, bno, len(strg))
		rt := 0
		for ; rt < 3; rt++ {
			_, e = dbmap.Exec(fmt.Sprintf(`update %v set flag = ?, bno = ? where uuid in (%s)`, table, uuids), flag, bno)
			if e != nil {
				log.Printf("failed to update flag %s,%d: %+v, retrying %d...", flag, bno, e, rt+1)
			} else {
				break
			}
		}
		if rt >= 3 {
			if e != nil {
				chr <- j
			}
		}
		j.done = true
		chr <- j
	}
}

func getUntaggedCorls(table CorlTab) (uuids []int, e error) {
	stmt, e := dbmap.Prepare(fmt.Sprintf(`select uuid from %v where flag is null order by corl`, table))
	if e != nil {
		return nil, errors.WithStack(e)
	}
	defer stmt.Close()
	rows, e := stmt.Query()
	if e != nil {
		return nil, errors.WithStack(e)
	}
	defer rows.Close()
	var uuid int
	for rows.Next() {
		rows.Scan(&uuid)
		uuids = append(uuids, uuid)
	}
	e = rows.Err()
	return
}
