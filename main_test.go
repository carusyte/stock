package main

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/model"
	"github.com/mjanda/go-dtw"
	"github.com/shirou/gopsutil/cpu"
	"github.com/ssgreg/repeat"
)

func TestCpu(t *testing.T) {
	for x := 0; x < 10; x++ {
		s, e := cpu.Percent(0, false)
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

// func TestBlue(t *testing.T) {
// 	blue()
// }

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

func TestISOWeek(t *testing.T) {
	tToday, _ := time.Parse(global.DateFormat, "2017-09-04")
	y, w := tToday.ISOWeek()
	fmt.Println(y, w)
	tToday, _ = time.Parse(global.DateFormat, "2017-09-05")
	y, w = tToday.ISOWeek()
	fmt.Println(y, w)
}

func TestNilSlice(t *testing.T) {
	s := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	s1 := s[5:7]
	s[5] = 99
	s[6] = 144
	s = nil
	fmt.Println(s1)
}

func TestMap(t *testing.T) {
	m := make(map[int]byte)
	modMap(m)
	fmt.Printf("%+v", m)
}

func modMap(m map[int]byte) {
	m[1] = 0
	m[2] = 0
	m[3] = 0
}

func TestNilPointer(t *testing.T) {
	var s []*model.TradeDataBasic
	log.Printf("len: %d", len(s))
	t.Fail()
}

func TestMapNil(t *testing.T) {
	m := map[string]interface{}{
		"a": nil,
		"b": 0,
	}
	if x, ok := m["a"]; ok {
		log.Debugf("map to nil is ok: %+v", x)
	}
}

func TestRepeat(t *testing.T) {
	op := func(c int) error {
		log.Debugf("retrying: %d", c+1)
		return repeat.HintTemporary(errors.New("dumb error"))
	}
	repeat.Repeat(
		repeat.FnWithCounter(op),
		repeat.StopOnSuccess(),
		repeat.LimitMaxTries(conf.Args.DefaultRetry),
		repeat.WithDelay(
			repeat.FullJitterBackoff(500*time.Millisecond).WithMaxDelay(10*time.Second).Set(),
		),
	)
}

func TestUnixTime(t *testing.T) {
	log.Debugf("time.Now().Unix(): %+v", time.Now().Unix())
	log.Debugf("time.Now().Local().Unix(): %+v", time.Now().Local().Unix())
	log.Debugf("time.Now().UnixNano(): %+v", time.Now().UnixNano())
	log.Debugf("time.Now().Local().UnixNano(): %+v", time.Now().Local().UnixNano())
}
