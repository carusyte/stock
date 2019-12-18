package getd

import (
	"math/rand"
	"time"

	"github.com/carusyte/stock/global"
)

var (
	dbmap = global.Dbmap
	dot   = global.Dot
)

func init() {
	rand.Seed(time.Now().UnixNano())
}
