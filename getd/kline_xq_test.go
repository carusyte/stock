package getd

import (
	"testing"

	"github.com/carusyte/stock/model"
)

func Test_getKlineXQ(t *testing.T) {
	type args struct {
		stk    *model.Stock
		kltype []model.DBTab
	}
	tests := []struct {
		name      string
		args      args
		wantTdmap map[model.DBTab]*model.TradeData
		wantLkmap map[model.DBTab]int
		wantSuc   bool
	}{
		{
			name: "basic test",
			args: args{
				stk:    &model.Stock{Code: "000585", Name: "东北电气"},
				kltype: []model.DBTab{model.KLINE_DAY_B, model.KLINE_DAY_F, model.KLINE_DAY_NR},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// gotTdmap, gotLkmap, gotSuc := getKlineXQ(tt.args.stk, tt.args.kltype)
			getKlineXQ(tt.args.stk, tt.args.kltype)
		})
	}
}
