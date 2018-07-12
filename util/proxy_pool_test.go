package util

import (
	"sync"
	"testing"
)

func TestFetchProxyFrom66IP(t *testing.T) {
	t.Fail()
	var wg sync.WaitGroup
	chpx := make(chan []string)
	wg.Add(1)
	fetchProxyFrom66IP(&wg, chpx)
	wg.Wait()
	close(chpx)
}

func TestPickProxy(t *testing.T) {
	PickProxy()
}
