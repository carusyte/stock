package proxy

import (
	"math/rand"
	"sync"
)

type proxy struct {
	ip   string
	port int
}

var (
	proxyPool []proxy
	pxLock    = sync.RWMutex{}
)

//PickProxy randomly chooses a proxy from the pool.
func PickProxy() (ip string, port int, e error) {
	pxLock.Lock()
	defer pxLock.Unlock()
	
	if len(proxyPool) > 0 {
		p := proxyPool[rand.Intn(len(proxyPool))]
		return p.ip, p.port, nil
	}
}
