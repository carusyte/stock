package getd

import (
	"fmt"
	"sync"
)

//CollectFsStats updates feature scaling stats.
func CollectFsStats() {
	// _, e := dbmap.Exec("delete from fs_stats where method = ?", "standardization")
	// if e != nil {
	// 	log.Printf("failed to clean up fs_stats %+v", e)
	// 	return
	// }

	var wg sync.WaitGroup

	// basic log returns
	sqlt, e := dot.Raw("COLLECT_STANDARDIZATION_STATS")
	if e != nil {
		log.Printf("failed to get fs_stats sql %+v", e)
		return
	}
	tabs := []string{"kline_d_b_lr", "kline_w_b_lr", "kline_m_b_lr", "index_d_n_lr", "index_w_n_lr", "index_m_n_lr"}
	fields := []string{"close", "low", "low_close", "high", "high_close", "open", "open_close", "volume"}
	upfn := func(tabs, fields []string, w *sync.WaitGroup) {
		defer wg.Done()
		for _, t := range tabs {
			for _, f := range fields {
				usql := fmt.Sprintf(sqlt, t, f)
				_, e = dbmap.Exec(usql)
				if e != nil {
					log.Printf("failed to update fs_stats for [%s.%s]: %+v", t, f, e)
					continue
				}
				log.Printf("fs_stats for [%s.%s] updated.", t, f)
			}
		}
	}
	wg.Add(1)
	go upfn(tabs, fields, &wg)

	// moving average log returns
	tabs = []string{
		"kline_d_b_ma_lr", "kline_w_b_ma_lr", "kline_m_b_ma_lr",
		"index_d_n_ma_lr", "index_w_n_ma_lr", "index_m_n_ma_lr",
	}
	fields = []string{
		"ma5", "ma5_l", "ma5_h", "ma5_o",
		"ma10", "ma10_l", "ma10_h", "ma10_o",
		"ma20", "ma20_l", "ma20_h", "ma20_o",
		"ma30", "ma30_l", "ma30_h", "ma30_o",
		"ma60", "ma60_l", "ma60_h", "ma60_o",
		"ma120", "ma120_l", "ma120_h", "ma120_o",
		"ma200", "ma200_l", "ma200_h", "ma200_o",
		"ma250", "ma250_l", "ma250_h", "ma250_o",
		"vol5", "vol10", "vol20", "vol30", "vol60", "vol120", "vol200", "vol250",
	}
	wg.Add(1)
	go upfn(tabs, fields, &wg)

	// indicators
	tabs = []string{"indicator_d", "indicator_w", "indicator_m"}
	fields = []string{
		"KDJ_K", "KDJ_D", "KDJ_J",
		"MACD", "MACD_diff", "MACD_dea",
		"RSI1", "RSI2", "RSI3",
		"BIAS1", "BIAS2", "BIAS3",
		"BOLL_lower", "BOLL_lower_o", "BOLL_lower_h", "BOLL_lower_l", "BOLL_lower_c",
		"BOLL_mid", "BOLL_mid_o", "BOLL_mid_h", "BOLL_mid_l", "BOLL_mid_c",
		"BOLL_upper", "BOLL_upper_o", "BOLL_upper_h", "BOLL_upper_l", "BOLL_upper_c",
	}
	wg.Add(1)
	go upfn(tabs, fields, &wg)

	wg.Wait()
}
