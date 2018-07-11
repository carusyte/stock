package getd

import (
	"github.com/carusyte/stock/model"
)

//UpdateStockRels updates stock relationship data in db.
func UpdateStockRels(stocks *model.Stocks) (rstks *model.Stocks) {
	// log.Printf("begin to collect stock relationship data")
	// var wg sync.WaitGroup
	// parallel := conf.Args.Concurrency
	// wf := make(chan int, parallel)
	// outstks := make(chan *model.Stock, JOB_CAPACITY)
	// rstks = new(model.Stocks)
	// wgr := collect(rstks, outstks)
	// chDbjob = createStockRelsDbJobQueues()
	// wgdb := saveStockRels(outstks)
	// for _, stk := range stocks.List {
	// 	wg.Add(1)
	// 	wf <- 1
	// 	go getStockRels(stk, &wg, &wf)
	// }
	// wg.Wait()
	// close(wf)
	// waitDbjob(wgdb)
	// close(outstks)
	// wgr.Wait()
	// log.Printf("%d stocks %s data updated.", rstks.Size(), strings.Join(kt2strs(kltype), ", "))
	// if stks.Size() != rstks.Size() {
	// 	same, skp := stks.Diff(rstks)
	// 	if !same {
	// 		log.Printf("Failed: %+v", skp)
	// 	}
	// }
	// return
	panic("implement me")
}
