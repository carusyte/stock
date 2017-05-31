package score

import "github.com/carusyte/stock/model"

// Medium to Long term model.
// Search stocks for:
// 1. Latest high yearly dividend yield ratio, which is >= 4.5%
// 2. DYR with progressive increase or constantly at high level
// 3. Nearer registration date.
// Get warnings if:
// 1. Dividend Payout Ratio is greater than 90%
type HiD struct {
	Code        string
	Name        string
	Year        string
	RegDate     string
	XdxrDate    string
	Progress    string
	Divi        float64
	SharesAllot float64
	SharesCvt   float64
	Dyr         float64
	Dpr         float64
	DyrGrYoy    string
}

const DYR_THRESHOLD = 0.045

func (h *HiD) Get(s []*model.Stock) (r Result) {
	var hid HiD
	if s == nil || len(s) == 0 {
		dbmap.Select(&hid, "", DYR_THRESHOLD)
	} else {

	}
}
