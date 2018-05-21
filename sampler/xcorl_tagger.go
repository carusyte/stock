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

//TagXcorlTrn tags the sampled xcorl_trn data with specified flag as prefix
//by randomly and evenly selecting untagged samples.
func TagXcorlTrn(flag string) (e error) {
	log.Printf("tagging xcorl_trn using %s as prefix...", flag)
	// clear already tagged data
	log.Println("cleansing existing tag...")
	usql := fmt.Sprintf(`update xcorl_trn set flag = null where flag like '%s%%'`, flag)
	_, e = dbmap.Exec(usql)
	if e != nil {
		return errors.WithStack(e)
	}
	// tag group * batch_size of target data from untagged records randomly and evenly
	log.Println("loading untagged records...")
	var untagged []string
	_, e = dbmap.Select(&untagged, `select uuid from xcorl_trn where flag is null order by corl`)
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
		prog := float64(float64(i+1) / float64(len(grps))) * 100.
		log.Printf("step %d/%d(%.3f%%) tagging %s, size: %d", i+1, len(grps), prog, flag, len(g))
		_, e = dbmap.Exec(fmt.Sprintf(`update xcorl_trn set flag = ? where uuid in (%s)`, uuids), flag)
		if e != nil {
			return errors.WithStack(e)
		}
		grps[i] = nil
	}
	log.Printf("xcorl_trn %s set tagged: %d", flag, batches)
	return nil
}
