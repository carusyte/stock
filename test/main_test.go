package main

import (
	"testing"
	"fmt"
	"time"
	"sync"
)

func TestChannel(t *testing.T) {
	rc := make(chan int, 5)
	rl := make([]int, 0, 16)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range rc {
			time.Sleep(time.Millisecond * 120)
			rl = append(rl, i)
		}
	}()
	for i := 0; i < 300; i++ {
		rc <- i
	}
	close(rc)
	wg.Wait()
	fmt.Printf("size: %d", len(rl))
}
