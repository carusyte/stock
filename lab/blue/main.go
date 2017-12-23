package main

import (
	"log"
	"time"

	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/score"
)

func main() {
	// hidblue()
	blue()
}

func hidblue() {
	start := time.Now()
	r1 := new(score.HiD).Geta()
	r1.Weight = 0.2
	r2 := new(score.BlueChip).Geta()
	r2.Weight = 0.8
	r1r2 := score.Combine(r1, r2).Sort()
	// .Shrink(int(c))
	log.Printf("%+v", r1r2)
	log.Printf("Time Cost: %v", time.Since(start).Seconds())
}

func blue() {
	s := &model.Stock{}
	s.Code = "000651"
	s.Name = "格力电器"
	ss := new(model.Stocks)
	ss.Add(s)
	b := new(score.BlueChip)
	b.Get([]string{"000651"}, -1, false)
}
