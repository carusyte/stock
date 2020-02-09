package getd

import (
	"database/sql"
	"testing"

	"github.com/carusyte/stock/model"
)

func Test_getKlineEM(t *testing.T) {
	type args struct {
		stk  *model.Stock
		freq FetchRequest
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
				freq: FetchRequest{
					RemoteSource: model.EM,
					LocalSource:  model.EM,
					Cycle:        model.DAY,
					Reinstate:    model.None,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &EmKlineFetcher{}
			// gotTdmap, gotLkmap, gotSuc := getKlineXQ(tt.args.stk, tt.args.kltype)
			f.fetchKline(
				tt.args.stk,
				tt.args.freq,
				false)
		})
	}
}
