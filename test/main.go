package main

import (
	"time"
	"github.com/carusyte/stock/score"
	"log"
	logr "github.com/sirupsen/logrus"
	"github.com/carusyte/stock/getd"
)

func main() {
	//getData()
	//kdjFirst()
	holistic()
	//BLUE
	//blue()
	//kdjOnly()
}

func blue() {
	r := new(score.BlueChip).Get(nil, -1, true)
	log.Printf("\n%+v", r)
}

func holistic() {
	logr.SetLevel(logr.DebugLevel)
	start := time.Now()
	//r1 := new(score.HiD).Geta()
	//r1.Weight = 0.1
	r2 := new(score.BlueChip).Geta().Sort().Shrink(500)
	r2.Weight = 0
	//r1r2 := score.Combine(r1, r2)
	//r1r2.Weight = 0
	r3 := new(score.KdjV).Get(r2.Stocks(), -1, false)
	r3.Weight = 1
	log.Printf("\n%+v", score.Combine(r2, r3).Sort())
	log.Printf("Time Cost: %v", time.Since(start).Seconds())
}

func kdjFirst() {
	logr.SetLevel(logr.DebugLevel)
	start := time.Now()
	r1 := new(score.KdjV).Geta().Sort().Shrink(50)
	r2 := new(score.HiD).Get(r1.Stocks(), -1, false)
	r2.Weight = 0.2
	r3 := new(score.BlueChip).Get(r1.Stocks(), -1, false)
	r3.Weight = 0.8
	log.Printf("\n%+v", score.Combine(r2, r3, r1).Sort())
	log.Printf("Time Cost: %v", time.Since(start).Seconds())
}

func kdjOnly(code... string) {
	logr.SetLevel(logr.DebugLevel)
	start := time.Now()
	r1 := new(score.KdjV).Get(code,-1,true)
	log.Printf("\n%+v", r1)
	log.Printf("Time Cost: %v", time.Since(start).Seconds())
}

func getData() {
	getd.Get()
}
