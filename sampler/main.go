package sampler

import (
	"log"
	"math/rand"
	"time"

	"github.com/carusyte/stock/conf"

	"github.com/carusyte/stock/model"
)

var (
	grader Grader
)

func init() {
	rand.Seed(time.Now().UnixNano())

	switch conf.Args.Sampler.Grader {
	case graderLr:
		log.Println("Sampling key points using LrGrader")
		grader = new(lrGrader)
	case graderRemaLr:
		log.Println("Sampling key points using RemaLrGrader")
		grader = new(remaLrGrader)
	default:
		log.Println("Sampling key points using default grader")
		grader = new(dwGrader)
	}
}

//Grader gives scores according to specific standards based on various implementation.
type Grader interface {
	sample(code string, frame int, klhist []*model.Quote) (kpts []*model.KeyPoint, err error)
	stats(frame int) (e error)
}
