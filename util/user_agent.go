package util

import (
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"

	"github.com/PuerkitoBio/goquery"
	"github.com/pkg/errors"
)

var (
	agentPool []string
	uaLock    = sync.RWMutex{}
)

//PickUserAgent picks a user agent string from the pool randomly.
//if the pool is not populated, it will trigger the initialization process
//to fetch user agent lists from remote server.
func PickUserAgent() (ua string, e error) {
	uaLock.Lock()
	defer uaLock.Unlock()

	if len(agentPool) > 0 {
		return agentPool[rand.Intn(len(agentPool))], nil
	}
	log.Println("fetching user agent list from remote server...")
	urlTmpl := `https://developers.whatismybrowser.com/useragents/explore/hardware_type_specific/computer/%d`
	pages := 3
	for p := 1; p <= pages; p++ {
		url := fmt.Sprintf(urlTmpl, p)
		res, e := HTTPGetResponse(url, nil, false, false, false)
		if e != nil {
			log.Printf("failed to get user agent list from %s, giving up %+v", url, e)
			return ua, errors.WithStack(e)
		}
		defer res.Body.Close()
		// parse body using goquery
		doc, e := goquery.NewDocumentFromReader(res.Body)
		if e != nil {
			log.Printf("failed to read from response body: %+v", e)
			return ua, errors.WithStack(e)
		}
		//parse user agent
		doc.Find("body div.content-base section div table tbody tr").Each(
			func(i int, s *goquery.Selection) {
				agentPool = append(agentPool, strings.TrimSpace(s.Find("td.useragent a").Text()))
			})
	}
	log.Printf("successfully fetched %d user agents from remote server.", len(agentPool))
	return agentPool[rand.Intn(len(agentPool))], nil
}
