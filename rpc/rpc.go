package rpc

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/bitly/go-hostpool"
	"github.com/carusyte/stock/conf"

	"github.com/pkg/errors"
	logr "github.com/sirupsen/logrus"
)

	// "github.com/felixge/tcpkeepalive"

var (
	hp = hostpool.New(conf.Args.RPCServers)
	//lock                   = sync.RWMutex{}
)

//Pub Publish data to all rpc servers
func Pub(service string, request interface{}, reply interface{}, retry int) (e []error) {
	var wg sync.WaitGroup
	cherr := make(chan error)
	for _, srv := range hp.Hosts() {
		wg.Add(1)
		go func(wg *sync.WaitGroup, cherr chan error) {
			defer wg.Done()
			for i := 0; i < retry; i++ {
				logr.Debugf("rpc call start, server: %s, service: %s", srv, service)
				err := tryRpcCall(srv, service, request, reply)
				if err == nil {
					return
				} else if i+1 < retry {
					logr.Warnf("retrying to call rpc service: %d\n, %s", i+1, fmt.Sprintln(err))
					time.Sleep(time.Millisecond * time.Duration(500+500*i))
				} else {
					logr.Errorf("failed to call rpc service\n%s", fmt.Sprintln(err))
					cherr <- err
				}
			}
		}(&wg, cherr)
	}
	wg.Wait()
	for err := range cherr {
		e = append(e, err)
	}
	close(cherr)
	return nil
}

func Call(service string, request interface{}, reply interface{}, retry int) (e error) {
	for i := 0; i < retry; i++ {
		hpr := hp.Get()
		serverAddress := hpr.Host()
		logr.Debugf("rpc call start, server: %s, service: %s", serverAddress, service)
		err := tryRpcCall(serverAddress, service, request, reply)
		if err == nil {
			hpr.Mark(nil)
			return nil
		} else if i+1 < retry {
			hpr.Mark(err)
			logr.Warnf("retrying to call rpc service: %d\n, %s", i+1, fmt.Sprintln(err))
		} else {
			hpr.Mark(err)
			logr.Errorf("failed to call rpc service\n%s", fmt.Sprintln(err))
			return err
		}
	}
	return nil
}

func tryRpcCall(serverAddress, service string, request interface{}, reply interface{}) (e error) {
	conn, err := net.Dial("tcp", serverAddress)
	if err != nil {
		return errors.Wrapf(err, "failed to connect rpc server: %s", serverAddress)
	}
	defer conn.Close()

	// err = tcpkeepalive.SetKeepAlive(conn, time.Second*60, 2048, time.Second*45)
	// if err != nil {
	// 	return errors.Wrapf(err, "failed to set tcp keep-alive for connection to %s", serverAddress)
	// }
	
	client := rpc.NewClient(conn)
	defer client.Close()
	err = client.Call(service, request, reply)
	if err != nil {
		return errors.Wrapf(err, "%s rpc service error: %s", serverAddress, service)
	}
	return nil
}

// Returns the number of available RPC servers configured in rpc_servers in stock.toml
// If filter is set to true, broken servers will be removed from the host pool.
func Available(filter bool) (c int, healthy float64) {
	srvs := hp.Hosts()
	if len(srvs) == 0 {
		return 0, 0
	}
	all := len(srvs)
	for i := 0; i < len(srvs); {
		srv := srvs[i]
		conn, err := net.Dial("tcp", srv)
		if err == nil {
			conn.Close()
			c++
			i++
		} else {
			logr.Warnf("rpc server %s is inaccessible", srv)
			if filter {
				logr.Printf("removing rpc server %s from the host pool", srv)
				if i+1 < len(srvs) {
					srvs = append(srvs[:i], srvs[i+1:]...)
				} else {
					srvs = srvs[:i]
				}
			} else {
				i++
			}
		}
	}
	if c < all {
		hp = hostpool.New(srvs)
	}
	healthy = float64(c) / float64(all)
	return
}
