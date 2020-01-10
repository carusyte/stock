package getd

import (
	"context"
	"testing"

	"github.com/carusyte/stock/model"
	"github.com/sirupsen/logrus"

	"reflect"
	"time"

	"gopkg.in/chromedp/chromedp.v0"
)

func TestGet(t *testing.T) {
	Get()
}

func TestCollectFsStats(t *testing.T) {
	t.Fail()
	CollectFsStats()
}
func TestTimeoutContext(t *testing.T) {
	ctxt, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	select {
	case d := <-ctxt.Done():
		logrus.Printf("Done() returns: %+v\n", d)
		e := ctxt.Err()
		logrus.Printf("%+v\n%+v\n", reflect.TypeOf(e), e)
	}
	logrus.Printf("end")
}

func TestFinMark(t *testing.T) {
	s := &model.Stock{}
	s.Code = "601377"
	s.Name = "兴业证券"
	ss := new(model.Stocks)
	ss.Add(s)
	finMark(ss)
}

// func TestGetByCDP(t *testing.T) {
// 	var (
// 		err  error
// 		pool *chromedp.Pool
// 		wg   sync.WaitGroup
// 	)
// 	// create context
// 	ctxt, cancel := context.WithCancel(context.Background())
// 	defer cancel()

// 	//cdp.PoolLog(nil, nil, logrus.Printf)
// 	pool, err = chromedp.NewPool()
// 	if err != nil {
// 		logrus.Fatal(err)
// 	}
// 	defer func() {
// 		err = pool.Shutdown()
// 		if err != nil {
// 			logrus.Fatal(err)
// 		}
// 	}()

// 	// run task list
// 	codes := []string{"600022", "600765", "000766"}
// 	tab := model.KLINE_MONTH
// 	for _, code := range codes {
// 		wg.Add(1)
// 		go doGetData(code, tab, ctxt, pool, &wg)
// 	}
// 	wg.Wait()
// }

// func doGetData(code string, tab model.DBTab, ctxt context.Context, pool *chromedp.Pool, wg *sync.WaitGroup) {
// 	defer wg.Done()
// 	retry := 5
// 	for t := 0; t < retry; t++ {
// 		ok, retry := tryGetData(code, tab, ctxt, pool)
// 		if ok || !retry {
// 			break
// 		} else {
// 			logrus.Printf("=============== %s retry %d", code, t+1)
// 		}
// 	}
// }

// func tryGetData(code string, tab model.DBTab, ctxt context.Context, pool *chromedp.Pool) (ok, retry bool) {
// 	// get chrome runner from the pool
// 	pr, err := pool.Allocate(ctxt,
// 		runner.Flag("headless", true),
// 		runner.Flag("no-default-browser-check", true),
// 		runner.Flag("no-first-run", true),
// 		runner.ExecPath(`/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary`),
// 	)
// 	if err != nil {
// 		logrus.Fatal(err)
// 	}
// 	defer pr.Release()
// 	var today, all string
// 	chr := make(chan bool)
// 	go func(chr chan bool) {
// 		err := pr.Run(ctxt, buildActions4test(code, tab, &today, &all))
// 		if err != nil {
// 			fmt.Println(err)
// 			chr <- false
// 		} else {
// 			chr <- true
// 		}
// 	}(chr)
// 	select {
// 	case suc := <-chr:
// 		if suc {
// 			fmt.Printf("========= %s today: %s\n", code, today)
// 			fmt.Printf("========= %s all: %s\n", code, all)
// 			if len(today) > 0 && len(all) > 0 {
// 				return true, false
// 			} else {
// 				logrus.Printf("====== %s empty data returned", code)
// 				return false, true
// 			}
// 		} else {
// 			return false, true
// 		}
// 	case <-time.After(30 * time.Second):
// 		logrus.Printf("%s timeout waiting for network response", code)
// 		return false, true
// 	}
// }

// func buildActions4test(code string, tab model.DBTab, today, all *string) chromedp.Tasks {
// 	//url := fmt.Sprintf(`http://stockpage.10jqka.com.cn/HQ_v4.html#hs_%s`, code)
// 	url := fmt.Sprintf(`http://stockpage.10jqka.com.cn/HQ_v4.html#hs_%s`, code)
// 	fin := make(chan bool)
// 	sel := ``
// 	mcode := ""
// 	switch tab {
// 	case model.KLINE_DAY_NR:
// 		sel = `a[hxc3-data-type="hxc3KlineQfqDay"]`
// 		mcode = "00"
// 		return chromedp.Tasks{
// 			chromedp.Navigate(url),
// 			chromedp.WaitVisible(sel, chromedp.ByQuery),
// 			chromedp.Click(sel, chromedp.ByQuery),
// 			chromedp.WaitVisible(`#changeFq`, chromedp.ByID),
// 			chromedp.Click(`#changeFq`, chromedp.ByID),
// 			chromedp.WaitVisible(`a[data-type="Bfq"]`, chromedp.ByQuery),
// 			captureData4test(today, all, mcode, fin),
// 			chromedp.Click(`a[data-type="Bfq"]`, chromedp.ByQuery),
// 			wait4test(fin),
// 		}
// 	case model.KLINE_DAY:
// 		mcode = "01"
// 		sel = `a[hxc3-data-type="hxc3KlineQfqDay"]`
// 	case model.KLINE_WEEK:
// 		mcode = "11"
// 		sel = `a[hxc3-data-type="hxc3KlineQfqWeek"]`
// 	case model.KLINE_MONTH:
// 		mcode = "21"
// 		sel = `a[hxc3-data-type="hxc3KlineQfqMonth"]`
// 	}
// 	return chromedp.Tasks{
// 		chromedp.Navigate(url),
// 		chromedp.WaitVisible(sel, chromedp.ByQuery),
// 		captureData4test(today, all, mcode, fin),
// 		chromedp.Click(sel, chromedp.ByQuery),
// 		wait4test(fin),
// 	}
// }

func wait4test(fin chan bool) chromedp.Action {
	return chromedp.ActionFunc(func(ctxt context.Context) error {
		select {
		case <-time.After(100 * time.Second):
			return nil
		case <-ctxt.Done():
			return nil
		case <-fin:
			return nil
		}
	})
}

// func captureData4test(today, all *string, mcode string, fin chan bool) chromedp.Action {
// 	return chromedp.ActionFunc(func(ctxt context.Context, h cdp.Executor) error {
// 		th := h.(*chromedp.TargetHandler)
// 		echan := th.Listen(cdproto.EventNetworkRequestWillBeSent, cdproto.EventNetworkLoadingFinished,
// 			cdproto.EventNetworkLoadingFailed)
// 		go func(echan <-chan interface{}, ctxt context.Context, fin chan bool) {
// 			defer th.Release(echan)
// 			var (
// 				reqIdTd, reqIdAll network.RequestID
// 				urlTd, urlAll     string
// 				finTd, finAll     = false, false
// 			)
// 			for {
// 				select {
// 				case d := <-echan:
// 					switch d.(type) {
// 					case *network.EventLoadingFailed:
// 						lfail := d.(*network.EventLoadingFailed)
// 						if reqIdTd == lfail.RequestID {
// 							logrus.Printf("===== loading failed: %s, %+v", urlTd, lfail)
// 							return
// 						} else if reqIdAll == lfail.RequestID {
// 							logrus.Printf("===== loading failed: %s, %+v", urlAll, lfail)
// 							return
// 						}
// 					case *network.EventRequestWillBeSent:
// 						req := d.(*network.EventRequestWillBeSent)
// 						if strings.HasSuffix(req.Request.URL, mcode+"/today.js") {
// 							urlTd = req.Request.URL
// 							reqIdTd = req.RequestID
// 						} else if strings.HasSuffix(req.Request.URL, mcode+"/all.js") {
// 							urlAll = req.Request.URL
// 							reqIdAll = req.RequestID
// 						}
// 					case *network.EventLoadingFinished:
// 						res := d.(*network.EventLoadingFinished)
// 						if reqIdTd == res.RequestID {
// 							data, e := network.GetResponseBody(reqIdTd).Do(ctxt, h)
// 							if e != nil {
// 								panic(e)
// 							}
// 							*today = string(data)
// 							finTd = true
// 						} else if reqIdAll == res.RequestID {
// 							data, e := network.GetResponseBody(reqIdAll).Do(ctxt, h)
// 							if e != nil {
// 								panic(e)
// 							}
// 							*all = string(data)
// 							finAll = true
// 						}
// 					}
// 					if finTd && finAll {
// 						fin <- true
// 						return
// 					}
// 				case <-ctxt.Done():
// 					return
// 				}
// 			}
// 		}(echan, ctxt, fin)
// 		return nil
// 	})
// }
