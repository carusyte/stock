package score

import (
	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/model"
	"log"
	"os"
)

const JOB_CAPACITY = global.JOB_CAPACITY
const MAX_CONCURRENCY = global.MAX_CONCURRENCY

var (
	dbmap = global.Dbmap
	dot   = global.Dot
)

//TODO implement scoring
func main() {
	if len(os.Args) < 2 {
		log.Println("scorer is required")
		os.Exit(1)
	}
}

type Aspect struct {
	Score       float64
	Weight      float64
	Description string
	Comment     string
}

type Item struct {
	Code    string
	Field   interface{}
	Score   float64
	Aspects []*Aspect
}

type Result struct {
	Items map[string]*Item
}

type Scorer interface {
	Get(stock []*model.Stock) (r Result)
}
