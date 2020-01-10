package main

import (
	"time"

	"github.com/carusyte/stock/score"
	"github.com/sirupsen/logrus"
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
	logrus.Printf("%+v", r1r2)
	logrus.Printf("Time Cost: %v", time.Since(start).Seconds())
}

func blue() {
	b := new(score.BlueChip)
	b.Get([]string{"600104"}, -1, false)
}
