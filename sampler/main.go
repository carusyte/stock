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
		log.Println("Key point grader: LrGrader")
		grader = new(lrGrader)
	case graderRemaLr:
		log.Println("Key point grader: RemaLrGrader")
		grader = new(remaLrGrader)
	default:
		log.Println("Key point grader: default grader")
		grader = new(dwGrader)
	}
}

//Grader gives scores according to specific standards based on various implementation.
type Grader interface {
	sample(code string, frame int, klhist []*model.Quote) (kpts []*model.KeyPoint, err error)
	stats(frame int) (e error)
}
