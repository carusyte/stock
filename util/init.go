package util

import (
	"math/rand"
	"time"

	"github.com/carusyte/stock/global"
)

var log = global.Log

func init() {
	rand.Seed(time.Now().UnixNano())
}
