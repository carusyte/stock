package getd

import (
	"testing"

	"github.com/carusyte/stock/model"
)

func Test_sampXCorlTrnAt(t *testing.T) {
	type args struct {
		stock *model.Stock
		klid  int
	}
	tests := []struct {
		name     string
		args     args
		wantStop bool
		wantXt   []*model.XCorlTrn
	}{
		{
			name: "base",
			args: args{
				stock: &model.Stock{
					Code: "600104",
					Name: "上汽集团",
				},
				klid: 4730,
			},
			wantStop: false,
			wantXt:   []*model.XCorlTrn{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, xts := sampXCorlTrnAt(tt.args.stock, tt.args.klid)
			saveXCorlTrn(xts...)
		})
	}
}
