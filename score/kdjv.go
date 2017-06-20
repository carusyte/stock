package score

import "github.com/carusyte/stock/model"

// Medium to Long term model.
// Search for stocks whose J value in KDJ indicator is near valley, considering all periods
type KdjV struct{}

func (k *KdjV) Get(s []*model.Stock) (r Result){
	return
}