package main

import (
	"testing"
	"fmt"
	"time"
	"sync"
	"math"
	"github.com/mjanda/go-dtw"
	"log"
	"github.com/shirou/gopsutil/cpu"
)

func TestCpu(t *testing.T) {
	for x := 0; x < 10; x++ {
		s, e := cpu.Percent(0,false)
		if e != nil {
			panic(e)
		}
		for _, i := range s {
			log.Printf("%+v", i)
		}
		time.Sleep(time.Second * 2)
	}
}

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

func TestBlue(t *testing.T) {
	blue()
}

func TestDTW(t *testing.T) {
	// prepare arrays
	a := []float64{1, 1, 1, 2, 2, 2, 3, 3, 3, 2, 2, 4, 4, 4, 4}
	//b := []float64{1, 1, 2, 2, 3, 3, 2, 4, 4, 4}
	c := []float64{7, 9, 3, 1, 10, 20, 30, 3, 4, 6}

	dtw := dtw.Dtw{}

	// optionally set your own distance function
	dtw.DistanceFunction = func(x float64, y float64) float64 {
		difference := x - y
		return math.Sqrt(difference * difference)
	}
	dtw.ComputeOptimalPathWithWindow(a, c, 5) // 5 = window size
	path := dtw.RetrieveOptimalPath()
	log.Printf("Optimal Path: %+v", path)
}
