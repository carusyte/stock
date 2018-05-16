package getd

import (
	"fmt"
	"log"
)

//CollectFsStats updates feature scaling stats.
func CollectFsStats() {
	_, e := dbmap.Exec("delete from fs_stats where method = ?", "standardization")
	if e != nil {
		log.Printf("failed to clean up fs_stats %+v", e)
		return
	}
	// klines
	sqlt, e := dot.Raw("COLLECT_STANDARDIZATION_STATS")
	if e != nil {
		log.Printf("failed to get fs_stats sql %+v", e)
		return
	}
	fields := []string{
		"lr", "lr_l", "lr_l_c", "lr_h", "lr_h_c", "lr_o", "lr_o_c", "lr_vol",
		"lr_ma5", "lr_ma5_l", "lr_ma5_h", "lr_ma5_o",
		"lr_ma10", "lr_ma10_l", "lr_ma10_h", "lr_ma10_o",
		"lr_ma20", "lr_ma20_l", "lr_ma20_h", "lr_ma20_o",
		"lr_ma30", "lr_ma30_l", "lr_ma30_h", "lr_ma30_o",
		"lr_ma60", "lr_ma60_l", "lr_ma60_h", "lr_ma60_o",
		"lr_ma120", "lr_ma120_l", "lr_ma120_h", "lr_ma120_o",
		"lr_ma200", "lr_ma200_l", "lr_ma200_h", "lr_ma200_o",
		"lr_ma250", "lr_ma250_l", "lr_ma250_h", "lr_ma250_o",
		"lr_vol5", "lr_vol10", "lr_vol20", "lr_vol30", "lr_vol60", "lr_vol120", "lr_vol200", "lr_vol250",
	}
	for _, f := range fields {
		usql := fmt.Sprintf(sqlt, f)
		_, e = dbmap.Exec(usql)
		if e != nil {
			log.Printf("failed to update fs_stats for field %s: %+v", f, e)
			continue
		}
		log.Printf("fs_stats for field %s updated.", f)
	}
	// indicators
	sqlt, e = dot.Raw("COLLECT_INDICATOR_STANDARDIZATION_STATS")
	if e != nil {
		log.Printf("failed to get fs_stats sql %+v", e)
		return
	}
	tabs := []string{"indicator_d"}
	fields = []string{
		"KDJ_K", "KDJ_D", "KDJ_J",
		"MACD", "MACD_diff", "MACD_dea",
		"RSI1", "RSI2", "RSI3",
		"BIAS1", "BIAS2", "BIAS3",
		"BOLL_lower", "BOLL_lower_o", "BOLL_lower_h", "BOLL_lower_l", "BOLL_lower_c",
		"BOLL_mid", "BOLL_mid_o", "BOLL_mid_h", "BOLL_mid_l", "BOLL_mid_c",
		"BOLL_upper", "BOLL_upper_o", "BOLL_upper_h", "BOLL_upper_l", "BOLL_upper_c",
	}
	for _, t := range tabs {
		for _, f := range fields {
			usql := fmt.Sprintf(sqlt, t, f)
			_, e = dbmap.Exec(usql)
			if e != nil {
				log.Printf("failed to update fs_stats for field %s: %+v", f, e)
				continue
			}
			log.Printf("fs_stats for table %s field %s updated.", t, f)
		}
	}
}
