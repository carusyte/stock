package util

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/pkg/errors"
)

var (
	proxyPool []string
	luProxy   time.Time
	pxLock    = sync.RWMutex{}
)

const staleMax = 15

//PickHTTPProxy randomly chooses a HTTP proxy from the pool.
func PickHTTPProxy() (httpProxy string, e error) {
	pxLock.Lock()
	defer pxLock.Unlock()

	if len(proxyPool) > 0 && time.Since(luProxy).Minutes() < staleMax {
		return proxyPool[rand.Intn(len(proxyPool))], nil
	}

	log.Println("fetching free proxy list from remote server...")
	url := `https://free-proxy-list.net/`
	res, e := HTTPGetResponse(url, nil, true, false, false)
	if e != nil {
		log.Printf("failed to get free proxy list from %s, giving up %+v", url, e)
		return httpProxy, errors.WithStack(e)
	}
	defer res.Body.Close()
	// parse body using goquery
	doc, e := goquery.NewDocumentFromReader(res.Body)
	if e != nil {
		log.Printf("failed to read from response body: %+v", e)
		return httpProxy, errors.WithStack(e)
	}
	//parse free proxy item within 15 minutes check
	doc.Find("#proxylisttable tbody tr").Each(
		func(i int, s *goquery.Selection) {
			lchk := s.Find("td:nth-child(8)").Text()
			if strings.HasSuffix(lchk, "minute ago") {
				m := lchk[:strings.Index(lchk, " ")]
				if i, e := strconv.ParseInt(m, 10, 64); e == nil {
					if int(i) > staleMax {
						return
					}
				} else {
					log.Printf("failed to parse proxy last check string: %s, %+v", m, e)
					return
				}
			}
			proxyPool = append(proxyPool,
				fmt.Sprintf("http://%s:%s",
					strings.TrimSpace(s.Find("td:nth-child(1)").Text()),
					strings.TrimSpace(s.Find("td:nth-child(2)").Text()),
				),
			)
		})
	log.Printf("successfully fetched %d free proxy servers from remote.", len(proxyPool))
	luProxy = time.Now()
	return proxyPool[rand.Intn(len(proxyPool))], nil
}
