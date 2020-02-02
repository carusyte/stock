package util

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/global"
	"github.com/pkg/errors"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

var (
	proxyPool map[string]bool
	proxyList []*Proxy
	luProxy   time.Time
	pxLock    = sync.RWMutex{}
)

//PickProxyDirect randomly chooses a proxy from the pool directly.
func PickProxyDirect() (httpProxy string, e error) {
	pxLock.Lock()
	defer pxLock.Unlock()

	if len(proxyPool) > 0 && time.Since(luProxy).Minutes() < conf.Args.Network.RotateProxyRefreshInterval {
		return pickFromProxyPool(), nil
	}

	proxyPool = make(map[string]bool)

	var wg, wgc sync.WaitGroup
	chpx := make(chan []string, 512)

	wgc.Add(1)
	go collectProxies(&wgc, chpx)

	luProxy = time.Now()
	wg.Add(1)
	go fetchProxyFromFreeProxyList(&wg, chpx)
	wg.Add(1)
	go fetchProxyFromKuaidaili(&wg, chpx)
	wg.Add(1)
	go fetchProxyFromHinkyDink(&wg, chpx)
	wg.Add(1)
	go fetchProxyFrom66IP(&wg, chpx)
	wg.Add(1)
	go fetchProxyFromData5u(&wg, chpx)
	wg.Add(1)
	go fetchProxyFromIP3366(&wg, chpx)
	wg.Add(1)
	go fetchProxyFromSocksProxy(&wg, chpx)

	wg.Wait()
	close(chpx)
	wgc.Wait()

	log.Printf("successfully fetched %d free proxy servers from remote.", len(proxyPool))
	return pickFromProxyPool(), nil
}

//PickProxy randomly chooses a proxy from database.
//The queried proxy will be cached for conf.Args.Network.RotateProxyRefreshInterval
func PickProxy() (proxy *Proxy, e error) {
	pxLock.Lock()
	defer pxLock.Unlock()
	if len(proxyList) > 0 && time.Since(luProxy).Minutes() < conf.Args.Network.RotateProxyRefreshInterval {
		return proxyList[rand.Intn(len(proxyList))], nil
	}
	proxyList = make([]*Proxy, 0, 64)
	query := `
		SELECT 
			*
		FROM
			proxy_list
		WHERE
			score >= ?`
	_, e = global.Dbmap.Select(&proxyList, query, conf.Args.Network.RotateProxyScoreThreshold)
	if e != nil {
		log.Println("failed to query proxy server from database", e)
		return proxy, errors.WithStack(e)
	}
	luProxy = time.Now()
	log.Printf("successfully fetched %d free proxy servers from database.", len(proxyList))
	return proxyList[rand.Intn(len(proxyList))], nil
}

//UpdateProxyScore for the specified proxy.
func UpdateProxyScore(p *Proxy, success bool) {
	if p == nil {
		return
	}
	var e error
	if success {
		_, e = global.Dbmap.Exec(`update proxy_list set suc = suc + 1, score = suc/(suc+fail)*100 `+
			`where host = ? and port = ?`, p.Host, p.Port)
	} else {
		_, e = global.Dbmap.Exec(`update proxy_list set fail = fail + 1, score = suc/(suc+fail)*100 `+
			`where host = ? and port = ?`, p.Host, p.Port)
	}
	if e != nil {
		log.Printf("failed to increase fail counter for proxy %+v", p)
	}
}

func checkProxy(host, port string) bool {
	timeout := 5 * time.Second
	addr := net.JoinHostPort(host, port)
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		log.Printf("%s failed: %+v", addr, err)
		return false
	}
	if conn != nil {
		log.Printf("%s success", addr)
		conn.Close()
		return true
	}
	log.Printf("%s failed", addr)
	return false
}

func pickFromProxyPool() string {
	proxies := make([]string, len(proxyPool))
	i := 0
	for p := range proxyPool {
		proxies[i] = p
		i++
	}
	return proxies[rand.Intn(len(proxies))]
}

func collectProxies(wgc *sync.WaitGroup, chpx chan []string) {
	defer wgc.Done()
	for px := range chpx {
		for _, p := range px {
			proxyPool[p] = true
		}
	}
}

func fetchProxyFromKuaidaili(wg *sync.WaitGroup, chpx chan []string) {
	defer wg.Done()
	url := `https://www.kuaidaili.com/ops/proxylist/1/`
	log.Printf("fetching free proxy list from %s", url)
	res, e := HTTPGetResponse(url, nil, false, false, false)
	if e != nil {
		log.Printf("failed to get free proxy list from %s, giving up %+v", url, e)
		return
	}
	defer res.Body.Close()
	// parse body using goquery
	doc, e := goquery.NewDocumentFromReader(res.Body)
	if e != nil {
		log.Printf("failed to read response body from %s: %+v", url, e)
		return
	}
	var pool []string
	//parse free proxy item within last check limit
	doc.Find("#freelist table tbody tr").Each(
		func(i int, s *goquery.Selection) {
			lchk := s.Find("td:nth-child(8)").Text()
			if strings.HasSuffix(lchk, "分钟前") {
				m := lchk[:strings.Index(lchk, "分")]
				if i, e := strconv.ParseInt(m, 10, 64); e == nil {
					if int(i) > conf.Args.Network.RotateProxyFreshnessMin {
						return
					}
				} else {
					log.Printf("failed to parse proxy last check string: %s, %+v", m, e)
					return
				}
			}
			host := strings.TrimSpace(s.Find("td:nth-child(1)").Text())
			port := strings.TrimSpace(s.Find("td:nth-child(2)").Text())
			if checkProxy(host, port) {
				pool = append(pool,
					fmt.Sprintf("http://%s:%s",
						host,
						port,
					),
				)
			}
		})
	log.Printf("%d proxies available from %s", len(pool), url)
	chpx <- pool
}

func fetchProxyFromFreeProxyList(wg *sync.WaitGroup, chpx chan []string) {
	defer wg.Done()
	url := `https://free-proxy-list.net/`
	log.Printf("fetching free proxy list from %s", url)
	res, e := HTTPGetResponse(url, nil, true, false, false)
	if e != nil {
		log.Printf("failed to get free proxy list from %s, giving up %+v", url, e)
		return
	}
	defer res.Body.Close()
	// parse body using goquery
	doc, e := goquery.NewDocumentFromReader(res.Body)
	if e != nil {
		log.Printf("failed to read response body from %s: %+v", url, e)
		return
	}
	var pool []string
	//parse free proxy item within last check limit
	doc.Find("#proxylisttable tbody tr").Each(
		func(i int, s *goquery.Selection) {
			lchk := s.Find("td:nth-child(8)").Text()
			if strings.HasSuffix(lchk, "minute ago") {
				m := lchk[:strings.Index(lchk, " ")]
				if i, e := strconv.ParseInt(m, 10, 64); e == nil {
					if int(i) > conf.Args.Network.RotateProxyFreshnessMin {
						return
					}
				} else {
					log.Printf("failed to parse proxy last check string: %s, %+v", m, e)
					return
				}
			}
			host := strings.TrimSpace(s.Find("td:nth-child(1)").Text())
			port := strings.TrimSpace(s.Find("td:nth-child(2)").Text())
			if checkProxy(host, port) {
				pool = append(pool,
					fmt.Sprintf("http://%s:%s",
						host,
						port,
					),
				)
			}
		})
	log.Printf("%d proxies available from %s", len(pool), url)
	chpx <- pool
}

func fetchProxyFromHinkyDink(wg *sync.WaitGroup, chpx chan []string) {
	defer wg.Done()
	url := `http://www.mrhinkydink.com/proxies.htm`
	log.Printf("fetching free proxy list from %s", url)
	res, e := HTTPGetResponse(url, nil, false, false, false)
	if e != nil {
		log.Printf("failed to get free proxy list from %s, giving up %+v", url, e)
		return
	}
	defer res.Body.Close()
	// parse body using goquery
	doc, e := goquery.NewDocumentFromReader(res.Body)
	if e != nil {
		log.Printf("failed to read response body from %s: %+v", url, e)
		return
	}
	var pool []string
	//parse free proxy item
	doc.Find(`body table:nth-child(2) tbody tr:nth-child(2) ` +
		`td:nth-child(3) table tbody tr td table tbody tr[bgcolor="#88ff88"],tr[bgcolor="#ffff88"]`).Each(
		func(i int, s *goquery.Selection) {
			host := strings.TrimSpace(s.Find("td:nth-child(1)").Text())
			host = strings.TrimRight(host, `*`)
			port := strings.TrimSpace(s.Find("td:nth-child(2)").Text())
			if checkProxy(host, port) {
				pool = append(pool,
					fmt.Sprintf("http://%s:%s",
						host,
						port,
					),
				)
			}
		})
	log.Printf("%d proxies available from %s", len(pool), url)
	chpx <- pool
}

func fetchProxyFrom66IP(wg *sync.WaitGroup, chpx chan []string) {
	defer wg.Done()
	url := `http://www.66ip.cn/1.html`
	log.Printf("fetching free proxy list from %s", url)
	res, e := HTTPGetResponse(url, nil, false, false, false)
	if e != nil {
		log.Printf("failed to get free proxy list from %s, giving up %+v", url, e)
		return
	}
	defer res.Body.Close()
	// Convert the designated charset HTML to utf-8 encoded HTML.
	utfBody := transform.NewReader(res.Body, simplifiedchinese.GBK.NewDecoder())
	// parse body using goquery
	doc, e := goquery.NewDocumentFromReader(utfBody)
	if e != nil {
		log.Printf("failed to read response body from %s: %+v", url, e)
		return
	}
	var pool []string
	//parse free proxy item
	doc.Find(`#main div div:nth-child(1) table tbody tr`).Each(
		func(i int, s *goquery.Selection) {
			if i == 0 {
				//skip header
				return
			}
			host := strings.TrimSpace(s.Find("td:nth-child(1)").Text())
			port := strings.TrimSpace(s.Find("td:nth-child(2)").Text())
			if "0" == port {
				//invalid port
				return
			}
			if checkProxy(host, port) {
				pool = append(pool,
					fmt.Sprintf("http://%s:%s",
						host,
						port,
					),
				)
			}
		})
	log.Printf("%d proxies available from %s", len(pool), url)
	chpx <- pool
}

func fetchProxyFromIP3366(wg *sync.WaitGroup, chpx chan []string) {
	defer wg.Done()
	url := `http://www.ip3366.net/free/?stype=1`
	log.Printf("fetching free proxy list from %s", url)
	res, e := HTTPGetResponse(url, nil, false, false, false)
	if e != nil {
		log.Printf("failed to get free proxy list from %s, giving up %+v", url, e)
		return
	}
	defer res.Body.Close()
	// Convert the designated charset HTML to utf-8 encoded HTML.
	utfBody := transform.NewReader(res.Body, simplifiedchinese.GBK.NewDecoder())
	// parse body using goquery
	doc, e := goquery.NewDocumentFromReader(utfBody)
	if e != nil {
		log.Printf("failed to read response body from %s: %+v", url, e)
		return
	}
	var pool []string
	//parse free proxy item
	doc.Find(`#list table tbody tr`).Each(
		func(i int, s *goquery.Selection) {
			host := strings.TrimSpace(s.Find("td:nth-child(1)").Text())
			port := strings.TrimSpace(s.Find("td:nth-child(2)").Text())
			if checkProxy(host, port) {
				pool = append(pool,
					fmt.Sprintf("http://%s:%s",
						host,
						port,
					),
				)
			}
		})
	log.Printf("%d proxies available from %s", len(pool), url)
	chpx <- pool
}

func fetchProxyFromData5u(wg *sync.WaitGroup, chpx chan []string) {
	defer wg.Done()
	urls := []string{
		`http://www.data5u.com/free/index.shtml`,
		`http://www.data5u.com/free/gngn/index.shtml`,
		`http://www.data5u.com/free/gwgn/index.shtml`,
	}
	for _, url := range urls {
		log.Printf("fetching free proxy list from %s", url)
		res, e := HTTPGetResponse(url, nil, false, false, false)
		if e != nil {
			log.Printf("failed to get free proxy list from %s, giving up %+v", url, e)
			return
		}
		defer res.Body.Close()
		// parse body using goquery
		doc, e := goquery.NewDocumentFromReader(res.Body)
		if e != nil {
			log.Printf("failed to read response body from %s: %+v", url, e)
			return
		}
		var pool []string
		//parse free proxy item
		doc.Find(`body div:nth-child(7) ul li:nth-child(2) ul`).Each(
			func(i int, s *goquery.Selection) {
				if i == 0 {
					//skip header
					return
				}
				host := strings.TrimSpace(s.Find("span:nth-child(1) li").Text())
				port := strings.TrimSpace(s.Find("span:nth-child(2) li").Text())
				if checkProxy(host, port) {
					pool = append(pool,
						fmt.Sprintf("http://%s:%s",
							host,
							port,
						),
					)
				}
			})
		log.Printf("%d proxies available from %s", len(pool), url)
		chpx <- pool
	}
}

func fetchProxyFromSocksProxy(wg *sync.WaitGroup, chpx chan []string) {
	defer wg.Done()
	url := `https://www.socks-proxy.net/`
	log.Printf("fetching free proxy list from %s", url)
	res, e := HTTPGetResponse(url, nil, true, false, false)
	if e != nil {
		log.Printf("failed to get free proxy list from %s, giving up %+v", url, e)
		return
	}
	defer res.Body.Close()
	// parse body using goquery
	doc, e := goquery.NewDocumentFromReader(res.Body)
	if e != nil {
		log.Printf("failed to read response body from %s: %+v", url, e)
		return
	}
	var pool []string
	//parse free proxy item within last check limit
	doc.Find("#proxylisttable tbody tr").Each(
		func(i int, s *goquery.Selection) {
			ptype := strings.TrimSpace(s.Find("td:nth-child(5)").Text())
			if "Socks5" != ptype {
				return
			}
			host := strings.TrimSpace(s.Find("td:nth-child(1)").Text())
			port := strings.TrimSpace(s.Find("td:nth-child(2)").Text())
			if checkProxy(host, port) {
				pool = append(pool,
					fmt.Sprintf("socks5://%s:%s",
						host,
						port,
					),
				)
			}
		})
	log.Printf("%d proxies available from %s", len(pool), url)
	chpx <- pool
}

//Proxy represents the table structure of proxy_list.
type Proxy struct {
	Source      string  `db:"source"`
	Host        string  `db:"host"`
	Port        string  `db:"port"`
	Type        string  `db:"type"`
	Status      string  `db:"status"`
	Suc         int     `db:"suc"`
	Fail        int     `db:"fail"`
	Score       float64 `db:"score"`
	LastCheck   string  `db:"last_check"`
	LastScanned string  `db:"last_scanned"`
}

func (x *Proxy) String() string {
	return toJSONString(x)
}

func toJSONString(i interface{}) string {
	j, e := json.Marshal(i)
	if e != nil {
		fmt.Println(e)
	}
	return fmt.Sprintf("%v", string(j))
}
