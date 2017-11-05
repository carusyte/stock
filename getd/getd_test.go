package getd

import (
	"testing"
	"github.com/carusyte/stock/model"
	"context"
	"fmt"
	"log"

	cdp "github.com/knq/chromedp"
	cdptypes "github.com/knq/chromedp/cdp"
	"github.com/knq/chromedp/cdp/network"
	"strings"
)

func TestFinMark(t *testing.T) {
	s := &model.Stock{}
	s.Code = "601377"
	s.Name = "兴业证券"
	ss := new(model.Stocks)
	ss.Add(s)
	finMark(ss)
}

func TestGetByCDP(t *testing.T) {
	var err error

	// create context
	ctxt, cancel := context.WithCancel(context.Background())
	defer cancel()

	// create chrome instance
	c, err := cdp.New(ctxt, cdp.WithLog(log.Printf))
	if err != nil {
		log.Fatal(err)
	}

	// run task list
	var site, res string
	err = c.Run(ctxt, thsGet("600022"))
	if err != nil {
		log.Fatal(err)
	}

	// shutdown chrome
	err = c.Shutdown(ctxt)
	if err != nil {
		log.Fatal(err)
	}

	// wait for chrome to finish
	err = c.Wait()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("saved screenshot from search result listing `%s` (%s)", res, site)
}

func thsGet(code string) cdp.Tasks {
	url := fmt.Sprintf(`http://stockpage.10jqka.com.cn/HQ_v4.html#hs_%s`, code)
	var (
		treqId, areqId network.RequestID
	)
	return cdp.Tasks{
		cdp.ActionFunc(func(ctxt context.Context, h cdptypes.Handler) error {
			go func() {
				echan := h.Listen(cdptypes.EventNetworkRequestWillBeSent, cdptypes.EventNetworkLoadingFinished)
				for d := range echan {
					switch d.(type) {
					case *network.EventRequestWillBeSent:
						req := d.(*network.EventRequestWillBeSent)
						if strings.HasSuffix(req.Request.URL, "/today.js") {
							treqId = req.RequestID
						} else if strings.HasSuffix(req.Request.URL, "/all.js") {
							areqId = req.RequestID
						}
					case *network.EventLoadingFinished:
						res := d.(*network.EventLoadingFinished)
						var data []byte
						var e error
						if treqId == res.RequestID {
							data, e = network.GetResponseBody(treqId).Do(ctxt, h)
						} else if areqId == res.RequestID {
							data, e = network.GetResponseBody(areqId).Do(ctxt, h)
						}
						if e != nil {
							panic(e)
						}
						if len(data) > 0 {
							fmt.Printf("=========data: %+v\n", string(data))
						}
					}
				}
			}()
			return nil
		}),
		cdp.Navigate(url),
		cdp.WaitVisible(`body > ul > li:nth-child(2) > a`, cdp.ByQuery),
		cdp.Click(`body > ul > li:nth-child(2) > a`, cdp.ByQuery),
		cdp.WaitVisible(`#testcanvas > div.hxc3-hxc3KlinePricePane-hover`, cdp.ByQuery),
	}
}
