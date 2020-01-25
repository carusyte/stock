package getd

import (
	"database/sql"
	"testing"

	"github.com/carusyte/stock/model"
)

func Test_getKlineEM(t *testing.T) {
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
				stk: &model.Stock{
					Market: sql.NullString{String: "SZ", Valid: true},
					Code:   "000585",
					Name:   "东北电气"},
				kltype: []model.DBTab{model.KLINE_DAY_VLD, model.KLINE_WEEK_VLD, model.KLINE_MONTH_VLD},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// gotTdmap, gotLkmap, gotSuc := getKlineXQ(tt.args.stk, tt.args.kltype)
			getKlineEM(tt.args.stk, tt.args.kltype)
		})
	}
}
