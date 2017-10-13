package getd

import (
	"github.com/carusyte/stock/model"
	"time"
	"log"
)

func dKlineTc(stk *model.Stock, klt model.DBTab, incr bool) (kldy []*model.Quote, suc bool) {
	RETRIES := 20
	var (
		ldate string
		lklid int
		code  string = stk.Code
	)

	for rt := 0; rt < RETRIES; rt++ {
		kls, suc, retry := tryKlineTc(stk, klt, incr, &ldate, &lklid)
		if suc {
			kldy = kls
			break
		} else {
			if retry && rt+1 < RETRIES {
				log.Printf("%s retrying to get %s [%d]", code, klt, rt+1)
				time.Sleep(time.Millisecond * 500)
				continue
			} else {
				//FIXME sometimes 10jqk nginx server redirects to the same server and replies empty data no matter how many times you try
				log.Printf("%s failed to get %s", code, klt)
				return kldy, false
			}
		}
	}

	supplementMisc(kldy, lklid)
	if ldate != "" {
		//skip the first record which is for varate calculation
		kldy = kldy[1:]
	}
	binsert(kldy, string(klt), lklid)
	return kldy, true
}

func tryKlineTc(stock *model.Stock, tab model.DBTab, incr bool, ldate *string, lklid *int) (
	kldy []*model.Quote, ok, retry bool) {
	//TODO implement me
	return
}
