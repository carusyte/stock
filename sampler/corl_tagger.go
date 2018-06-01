package sampler

import (
	"fmt"
	"log"
	"math"
	"math/rand"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/util"

	"github.com/pkg/errors"
)

type CorlTab string

const (
	XcorlTrn CorlTab = "xcorl_trn"
	WccTrn   CorlTab = "wcc_trn"
)

//TagCorlTrn tags the sampled correlation training table (such as xcorl_trn or wcc_trn) data
//with specified flag as prefix by randomly and evenly selecting untagged samples.
func TagCorlTrn(table CorlTab, flag string) (e error) {
	log.Printf("tagging %v using %s as prefix...", table, flag)
	// clear already tagged data
	log.Println("cleansing existing tag...")
	usql := fmt.Sprintf(`update %v set flag = null where flag like '%s%%'`, table, flag)
	_, e = dbmap.Exec(usql)
	if e != nil {
		return errors.WithStack(e)
	}
	// tag group * batch_size of target data from untagged records randomly and evenly
	log.Println("loading untagged records...")
	var untagged []string
	_, e = dbmap.Select(&untagged, fmt.Sprintf(`select uuid from %v where flag is null order by corl`, table))
	if e != nil {
		return errors.WithStack(e)
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
	grps := make([][]string, batches)

	for i := 0; i < bsize; i++ {
		limit := segment
		if _, ok := remOwn[i]; ok {
			limit++
		}
		var uuids []string
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
	for i := 0; i < len(grps); i++ {
		g := grps[i]
		uuids := util.Join(g, ",", true)
		flag := fmt.Sprintf("%s_%d", flag, i+1)
		prog := float64(float64(i+1)/float64(len(grps))) * 100.
		log.Printf("step %d/%d(%.3f%%) tagging %s, size: %d", i+1, len(grps), prog, flag, len(g))
		rt := 0
		for ; rt < 3; rt++ {
			_, e = dbmap.Exec(fmt.Sprintf(`update %v set flag = ? where uuid in (%s)`, table, uuids), flag)
			if e != nil {
				log.Printf("failed to update flag: %+v, retrying %d...", e, rt+1)
			} else {
				break
			}
		}
		if rt >= 3 {
			if e != nil {
				return errors.WithStack(e)
			}
		}
		grps[i] = nil
	}
	log.Printf("%v %s set tagged: %d", table, flag, batches)
	return nil
}
