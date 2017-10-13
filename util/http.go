package util

import (
	"fmt"
	"golang.org/x/net/proxy"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"regexp"
	"time"
)

const RETRY int = 3

var PART_PROXY float64 = 0
var PROXY_ADDR string = ""

func HttpGetResp(url string) (res *http.Response, e error) {
	return HttpGetRespUsingHeaders(url, nil)
}

func HttpGetRespUsingHeaders(url string, headers map[string]string) (res *http.Response, e error) {
	host := ""
	r := regexp.MustCompile(`//([^/]*)/`).FindStringSubmatch(url)
	if len(r) > 0 {
		host = r[len(r)-1]
	}

	var client *http.Client
	//determine if we must use a proxy
	if PART_PROXY > 0 && rand.Float64() < PART_PROXY {
		// create a socks5 dialer
		dialer, err := proxy.SOCKS5("tcp", PROXY_ADDR, nil, proxy.Direct)
		if err != nil {
			fmt.Fprintln(os.Stderr, "can't connect to the proxy:", err)
			os.Exit(1)
		}
		// setup a http client
		httpTransport := &http.Transport{}
		client = &http.Client{Timeout: time.Second * 60, // Maximum of 60 secs
			Transport: httpTransport}
		// set our socks5 as the dialer
		httpTransport.Dial = dialer.Dial
	} else {
		client = &http.Client{Timeout: time.Second * 60, // Maximum of 60 secs
		}
	}

	for i := 0; true; i++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			log.Panic(err)
		}

		req.Header.Set("Accept", "*/*")
		req.Header.Set("Accept-Language", "en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4,zh-TW;q=0.2")
		req.Header.Set("Cache-Control", "no-cache")
		req.Header.Set("Connection", "keep-alive")
		//req.Header.Set("Cookie", "searchGuide=sg; "+
		//	"UM_distinctid=15d4e2ca50580-064a0c1f749ffa-30667808-232800-15d4e2ca506a9c; "+
		//	"Hm_lvt_78c58f01938e4d85eaf619eae71b4ed1=1502162404,1502164752,1504536800; "+
		//	"Hm_lpvt_78c58f01938e4d85eaf619eae71b4ed1=1504536800")
		if host != "" {
			req.Header.Set("Host", host)
		}
		req.Header.Set("Pragma", "no-cache")
		//req.Header.Set("Upgrade-Insecure-Requests", "1")
		req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6)"+
			" AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36")
		if headers != nil && len(headers) > 0 {
			for k := range headers {
				req.Header.Set(k, headers[k])
			}
		}

		res, err = client.Do(req)
		if err != nil {
			//handle "read: connection reset by peer" error by retrying
			if i >= RETRY {
				log.Printf("http communication failed. url=%s\n%+v", url, err)
				e = err
				return
			} else {
				log.Printf("http communication error. url=%s, retrying %d ...\n%+v", url, i+1, err)
				if res != nil {
					res.Body.Close()
				}
				time.Sleep(time.Millisecond * 500)
			}
		} else {
			return
		}
	}
	return
}

func HttpGetBytesUsingHeaders(url string, headers map[string]string) (body []byte, e error) {
	var resBody *io.ReadCloser
	defer func() {
		if resBody != nil {
			(*resBody).Close()
		}
	}()
	for i := 0; true; i++ {
		res, e := HttpGetRespUsingHeaders(url, headers)
		if e != nil {
			if i >= RETRY {
				log.Printf("http communication failed. url=%s\n%+v", url, e)
				return nil, e
			} else {
				log.Printf("http communication error. url=%s, retrying %d ...\n%+v", url, i+1, e)
				if res != nil {
					res.Body.Close()
				}
				time.Sleep(time.Millisecond * 500)
				continue
			}
		}
		resBody = &res.Body
		var err error
		body, err = ioutil.ReadAll(res.Body)
		if err != nil {
			//handle "read: connection reset by peer" error by retrying
			if i >= RETRY {
				log.Printf("http communication failed. url=%s\n%+v", url, err)
				return nil, e
			} else {
				log.Printf("http communication error. url=%s, retrying %d ...\n%+v", url, i+1, err)
				if resBody != nil {
					res.Body.Close()
				}
				time.Sleep(time.Millisecond * 500)
				continue
			}
		} else {
			return body, nil
		}
	}
	return
}

func HttpGetBytes(url string) (body []byte, e error) {
	return HttpGetBytesUsingHeaders(url, nil)
}

func Download(url, file string) (err error) {
	return DownloadUsingHeaders(url, file, nil)
}

func DownloadUsingHeaders(url, file string, headers map[string]string) (e error) {
	log.Printf("Downloading from %s", url)

	if _, e := os.Stat(file); e == nil {
		os.Remove(file)
	}

	output, err := os.Create(file)
	if err != nil {
		log.Printf("Error while creating %s\n%+v", file, err)
		return
	}
	defer output.Close()

	response, err := HttpGetRespUsingHeaders(url, headers)
	if err != nil {
		log.Printf("Error while downloading %s\n%+v", url, err)
		return
	}
	defer response.Body.Close()

	n, err := io.Copy(output, response.Body)
	if err != nil {
		log.Printf("Error while downloading %s\n%+v", url, err)
		return
	}

	log.Printf("%d bytes downloaded. file saved to %s.", n, file)

	return nil
}
