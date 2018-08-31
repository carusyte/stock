package util

import (
	"context"
	"os"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/carusyte/stock/conf"
)

const userAgent = "gcloud-golang-storage/20151204"

//GCSClient may serve as a handy wrapper for google cloud storage client,
//Caller can reuse the same storage.Client instance in concurrent goroutines.
//Initialization is performed automatically, and is thread-safe.
//Get the GCSClient via NewGCSClient function.
type GCSClient struct {
	init                          sync.Once
	c                             *storage.Client
	useProxy                      bool
	origHTTPProxy, origHTTPsProxy string
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
	if g.c != nil {
		return g.c, nil
	}
	g.init.Do(func() {
		if g.useProxy {
			// gcs api doesn't support proxy setting very well for now,
			// setting the environment variables as a workaround
			proxy := conf.Args.Network.MasterHttpProxy
			if v, ok := os.LookupEnv("http_proxy"); ok {
				if v != proxy {
					g.origHTTPProxy = v
				}
			} else {
				e = os.Setenv("http_proxy", proxy)
			}
			if v, ok := os.LookupEnv("https_proxy"); ok {
				if v != proxy {
					g.origHTTPsProxy = v
				}
			} else {
				e = os.Setenv("https_proxy", proxy)
			}
			if e != nil {
				return
			}
		}
		c, e = storage.NewClient(context.Background())
		if e != nil {
			return
		}
		g.c = c
	})
	return g.c, e
}

//Close delegates to *storage.Client.Close()
func (g *GCSClient) Close() (e error) {
	if g.origHTTPProxy != "" {
		if e = os.Setenv("http_proxy", g.origHTTPProxy); e != nil {
			return
		}
	}
	if g.origHTTPsProxy != "" {
		if e = os.Setenv("https_proxy", g.origHTTPsProxy); e != nil {
			return
		}
	}
	return g.c.Close()
}
