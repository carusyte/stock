package getd

import (
	"database/sql"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
)

//GetIndicesV2 fetches index data from configured source.
func GetIndicesV2() (idxlst, suclst []*model.IdxLst) {
	src := conf.Args.DataSource.Index
	log.Infof("Querying index list for source: %s", src)
	_, e := dbmap.Select(&idxlst, `select * from idxlst where src = ?`, src)
	util.CheckErr(e, "failed to query idxlst")
	log.Infof("# indices: %d", len(idxlst))
	idxMap := make(map[string]*model.IdxLst)
	for _, idx := range idxlst {
		log.Infof("%v", idx)
		idxMap[idx.Code] = idx
	}
	stks := &model.Stocks{}
	for _, idx := range idxlst {
		stks.Add(&model.Stock{
			Market: sql.NullString{
				String: idx.Market,
				Valid:  true,
			},
			Code: idx.Code,
			Name: idx.Name})
	}
	fr := FetchRequest{
		RemoteSource: model.DataSource(src),
		LocalSource:  model.Index,
		Reinstate:    model.None,
	}
	cs := []model.CYTP{model.DAY, model.WEEK, model.MONTH}
	frs := make([]FetchRequest, len(cs))
	for i, c := range cs {
		fr.Cycle = c
		frs[i] = fr
	}
	rstks := GetKlinesV2(stks, frs...)
	for _, c := range rstks.Codes {
		suclst = append(suclst, idxMap[c])
	}
	return
}
