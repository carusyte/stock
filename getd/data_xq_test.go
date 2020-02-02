package getd

import (
	"database/sql"
	"testing"
	"time"

	"github.com/carusyte/stock/model"
)

func Test_getKlineXQ(t *testing.T) {
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
					RemoteSource: model.XQ,
					LocalSource:  model.XQ,
					Cycle:        model.DAY,
					Reinstate:    model.Forward,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &XqKlineFetcher{}
			// gotTdmap, gotLkmap, gotSuc := getKlineXQ(tt.args.stk, tt.args.kltype)
			f.FetchKline(tt.args.stk, tt.args.freq, false)
		})
	}
}

func TestUnixMilliSec(t *testing.T) {
	begin := float64(time.Now().AddDate(0, 0, 1).UnixNano()) * float64(time.Nanosecond) / float64(time.Millisecond)
	log.Debugf("got: %d", int64(begin))
}
