package getd

import (
	"database/sql"
	"testing"
	"time"

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
				stk: &model.Stock{
					Market: sql.NullString{String: "SZ", Valid: true},
					Code:   "000585",
					Name:   "东北电气"},
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

func TestUnixMilliSec(t *testing.T) {
	begin := float64(time.Now().AddDate(0, 0, 1).UnixNano()) * float64(time.Nanosecond) / float64(time.Millisecond)
	log.Debugf("got: %d", int64(begin))
}
