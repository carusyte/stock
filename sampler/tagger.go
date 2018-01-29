package sampler

import (
	"log"

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
func TagTestSetByIndustry() (e error) {
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
	// tag them
	for _, k := range toTag {
		_, e = dbmap.Exec("update kpts set flag = ? where code = ? and klid = ?",
			TestFlag, k.Code, k.Klid)
		if e != nil {
			return errors.WithStack(e)
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
