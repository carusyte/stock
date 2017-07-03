package score

import "github.com/carusyte/stock/model"

// Medium to Long term model.
// Search for stocks whose J value in KDJ indicator is near valley, considering all periods
// Golden cross death cross theory?
type KdjV struct{
	model.Indicator
	Name string
}

func (k *KdjV) GetFieldStr(name string) string {
	panic("implement me")
}

func (k *KdjV) Get(stock []string, limit int, ranked bool) (r *Result) {
	panic("implement me")
}

func (k *KdjV) Id() string {
	panic("implement me")
}

func (k *KdjV) Fields() []string {
	panic("implement me")
}

func (k *KdjV) Description() string {
	panic("implement me")
}

func (k *KdjV) Geta() (r *Result){
	return
}
