package util

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/carusyte/stock/conf"
	"golang.org/x/net/proxy"
	"google.golang.org/api/option"

	"cloud.google.com/go/storage"
)

//GCSClient may serve as a handy wrapper for google cloud storage client,
//Caller can reuse the same storage.Client instance in concurrent goroutines.
//Initialization is performed automatically, and is thread-safe.
//Get the GCSClient via NewGCSClient function.
type GCSClient struct {
	init     sync.Once
	c        *storage.Client
	useProxy bool
}

//NewGCSClient creates a new storage.Client instance.
func NewGCSClient(useProxy bool) *GCSClient {
	return &GCSClient{
		useProxy: useProxy,
	}
}

//Get returns the storage.Client within this holder.
//may perform initialization for the first call.
//This function is thread-safe.
func (g *GCSClient) Get() (c *storage.Client, e error) {
	if g != nil {
		return g.c, nil
	}
	g.init.Do(func() {
		opts := make([]option.ClientOption, 0, 1)
		if g.useProxy {
			// create a socks5 dialer
			dialer, err := proxy.SOCKS5("tcp", conf.Args.Network.MasterProxyAddr, nil, proxy.Direct)
			if err != nil {
				log.Printf("can't connect to the master proxy: %+v", err)
				e = err
				return
			}
			// setup a http client
			httpTransport := &http.Transport{Dial: dialer.Dial}
			opts = append(opts, option.WithHTTPClient(
				&http.Client{Timeout: time.Second * time.Duration(conf.Args.GCS.Timeout),
					Transport: httpTransport}),
			)
		}
		c, e = storage.NewClient(context.Background(), opts...)
		if e != nil {
			return
		}
		g.c = c
	})
	return g.c, e
}

//Close delegates to *storage.Client.Close()
func (g *GCSClient) Close() (e error) {
	return g.c.Close()
}
