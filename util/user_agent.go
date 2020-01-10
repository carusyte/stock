package util

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"

	"github.com/PuerkitoBio/goquery"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
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
	logrus.Println("fetching user agent list from remote server...")
	urlTmpl := `https://developers.whatismybrowser.com/useragents/explore/hardware_type_specific/computer/%d`
	pages := 3
	for p := 1; p <= pages; p++ {
		url := fmt.Sprintf(urlTmpl, p)
		e = parseUaPage(url)
		if e != nil {
			return
		}
	}
	logrus.Printf("successfully fetched %d user agents from remote server.", len(agentPool))
	return agentPool[rand.Intn(len(agentPool))], nil
}

func parseUaPage(url string) (e error) {
	res, e := HTTPGetResponse(url, nil, false, false, false)
	if e != nil {
		logrus.Printf("failed to get user agent list from %s, giving up %+v", url, e)
		return errors.WithStack(e)
	}
	defer res.Body.Close()
	// parse body using goquery
	doc, e := goquery.NewDocumentFromReader(res.Body)
	if e != nil {
		logrus.Printf("failed to read from response body: %+v", e)
		return errors.WithStack(e)
	}
	//parse user agent
	doc.Find("body div.content-base section div table tbody tr").Each(
		func(i int, s *goquery.Selection) {
			agentPool = append(agentPool, strings.TrimSpace(s.Find("td.useragent a").Text()))
		})
	return nil
}
