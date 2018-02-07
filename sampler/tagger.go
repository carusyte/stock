package sampler

import (
	"fmt"
	"log"
	"math"
	"math/rand"

	"github.com/carusyte/stock/util"

	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/model"
	"github.com/pkg/errors"
)

const (
	//TestFlag indicates a sample data as member of test set.
	TestFlag = "TEST"
)

var (
	dot = global.Dot
)

//TagTestSetByIndustry tags the sampled keypoint data in kpts table with "TEST" flag.
// tagging is randomly selected amongst stocks of various industry in which
// the number of member stocks is no less than 10. In each industry, about
// 10% of the stocks will be selected. Only the records with the lowest klid
// will be tagged. Retagging will erase those already tagged as "TEST" and
// reselect target stocks without any flag, trying to find as much as 10%
// out of each industry.
func TagTestSetByIndustry(batchSize int) (e error) {
	// query number of stocks for each industry
	type stat struct {
		Industry string `db:"ind_lv3"`
		Count    int    `db:"cnt"`
	}
	var stats []*stat
	_, e = dbmap.Select(&stats, `select ind_lv3, count(*) cnt from basics `+
		`group by ind_lv3 having cnt > 9 order by cnt`)
	if e != nil {
		return errors.WithStack(e)
	}
	if len(stats) == 0 {
		log.Printf("no available data in basics table. aborting")
		return nil
	}
	// multiply by 0.1 and floor each count
	for _, s := range stats {
		s.Count = int(float32(s.Count) * float32(0.1))
	}
	// clear already "TEST" tagged data
	_, e = dbmap.Exec(`update kpts set flag = null where flag = ?`, TestFlag)
	if e != nil {
		return errors.WithStack(e)
	}
	// select desired number of target data from untagged records randomly
	qry, e := dot.Raw("RAND_KPTS_BY_INDUSTRY")
	if e != nil {
		return errors.WithStack(e)
	}
	var toTag []*model.KeyPoint
	for _, s := range stats {
		var kpts []*model.KeyPoint
		_, e = dbmap.Select(&kpts, qry, s.Industry, s.Count)
		if e != nil {
			return errors.WithStack(e)
		}
		log.Printf("%s\t%d", s.Industry, len(kpts))
		toTag = append(toTag, kpts...)
	}
	// tag head record with 'TEST' flag
	for _, k := range toTag {
		_, e = dbmap.Exec("update kpts set flag = ? where code = ? and klid = ?",
			TestFlag, k.Code, k.Klid)
		if e != nil {
			return errors.WithStack(e)
		}
	}
	// tag with TEST_### flag amongst TEST stocks, evenly across each score
	var scores []int
	_, e = dbmap.Select(&scores, `select distinct score from kpts`)
	if e != nil {
		log.Println(e)
		return errors.WithStack(e)
	}
	smap := make(map[int][]string)
	for i, s := range scores {
		var uuids []string
		log.Printf("fetching sample data for score %v", s)
		_, e = dbmap.Select(&uuids, `select uuid from kpts where score = ? `+
			`and code in (select code from kpts where flag = ?) `+
			`and flag is null ORDER BY RAND()`, s, TestFlag)
		if e != nil {
			log.Println(e)
			return errors.WithStack(e)
		}
		log.Printf("score %v size: %d", s, len(uuids))
		if len(uuids) > 0 {
			smap[s] = uuids
		} else {
			if i == len(scores)-1 {
				scores = scores[:len(scores)-1]
			} else {
				scores = append(scores[:i], scores[i+1:len(scores)]...)
			}
		}
	}

	n := len(smap)
	portion := int(math.Floor(float64(batchSize) / float64(n)))
	for bno := 1; true; bno++ {
		_, minl := findMin(smap)
		if minl < portion {
			break
		}
		rmd := batchSize - portion*n
		rmds := make(map[int]bool)
		if rmd > 0 {
			perm := rand.Perm(n)
			i := 0
			for _, p := range perm {
				s := scores[p]
				if len(smap[s]) > portion {
					rmds[s] = true
					i++
				}
				if i >= rmd {
					break
				}
			}
		}
		var uuids []string
		qmap := make(map[int]string)
		for s, us := range smap {
			mquota := portion
			if _, ok := rmds[s]; ok {
				mquota = portion + 1
			}
			nus := us[:mquota]
			smap[s] = us[mquota:len(us)]
			qmap[s] = fmt.Sprintf("%d-%d", len(us), mquota)
			uuids = append(uuids, nus...)
		}
		flag := fmt.Sprintf("TEST_%v", bno)
		log.Printf("Tagging [%s]: %+v", flag, qmap)
		updSQL := fmt.Sprintf(`update kpts set flag = ? where uuid in (%s)`,
			util.Join(uuids, ",", true))
		_, e = dbmap.Exec(updSQL, flag)
		if e != nil {
			return errors.WithStack(e)
		}
		if minl <= portion {
			break
		}
	}

	qry, e = dot.Raw("COUNT_KPTS_BY_FLAG")
	if e != nil {
		return errors.WithStack(e)
	}
	nTest, e := dbmap.SelectFloat(qry, TestFlag)
	if e != nil {
		return errors.WithStack(e)
	}
	nTotal, e := dbmap.SelectFloat("select count(*) from kpts")
	if e != nil {
		return errors.WithStack(e)
	}
	log.Printf("Test Set Summary:\t%d stocks, %.2f%% sampled data",
		len(toTag), nTest/nTotal*100.)
	return nil
}

//TagTrainingSetByScore tags the key point sample data with batch number randomly,
//at the mean time trying to keep a balanced portion of each class (score).
func TagTrainingSetByScore(batchSize int) (e error) {
	var scores []int
	_, e = dbmap.Select(&scores, `select distinct score from kpts`)
	if e != nil {
		log.Println(e)
		return errors.WithStack(e)
	}
	smap := make(map[int][]string)
	for i, s := range scores {
		var uuids []string
		log.Printf("fetching sample data for score %v", s)
		_, e = dbmap.Select(&uuids, `select uuid from kpts where score = ? `+
			`and code not in (select code from kpts where flag = 'TEST') `+
			`and flag is null ORDER BY RAND()`, s)
		if e != nil {
			log.Println(e)
			return errors.WithStack(e)
		}
		log.Printf("score %v size: %d", s, len(uuids))
		if len(uuids) > 0 {
			smap[s] = uuids
		} else {
			if i == len(scores)-1 {
				scores = scores[:len(scores)-1]
			} else {
				scores = append(scores[:i], scores[i+1:len(scores)]...)
			}
		}
	}

	n := len(smap)
	portion := int(math.Floor(float64(batchSize) / float64(n)))
	for bno := 1; true; bno++ {
		_, minl := findMin(smap)
		quota := portion
		if minl < portion {
			quota = minl
		}
		rmd := batchSize - quota*n
		rmds := make(map[int]bool)
		if rmd > 0 && minl > portion {
			perm := rand.Perm(n)
			i := 0
			for _, p := range perm {
				s := scores[p]
				if len(smap[s]) > quota {
					rmds[s] = true
					i++
				}
				if i >= rmd {
					break
				}
			}
		}
		var uuids []string
		qmap := make(map[int]string)
		for s, us := range smap {
			mquota := quota
			if _, ok := rmds[s]; ok {
				mquota = quota + 1
			}
			nus := us[:mquota]
			smap[s] = us[mquota:len(us)]
			qmap[s] = fmt.Sprintf("%d-%d", len(us), mquota)
			uuids = append(uuids, nus...)
		}
		flag := fmt.Sprintf("TRN_%v", bno)
		log.Printf("Tagging [%s]: %+v", flag, qmap)
		updSQL := fmt.Sprintf(`update kpts set flag = ? where uuid in (%s)`,
			util.Join(uuids, ",", true))
		_, e = dbmap.Exec(updSQL, flag)
		if e != nil {
			return errors.WithStack(e)
		}
		if minl <= portion {
			break
		}
	}
	return nil
}

func findMin(smap map[int][]string) (mins, minl int) {
	minl = math.MaxInt32
	for s, us := range smap {
		if len(us) < minl {
			mins = s
			minl = len(us)
		}
	}
	return
}
