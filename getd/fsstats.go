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
		"lr", "lr_l", "lr_h", "lr_o", "lr_vol",
		"lr_ma5", "lr_ma10", "lr_ma20", "lr_ma30", "lr_ma60", "lr_ma120", "lr_ma200", "lr_ma250",
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
		"MACD", "MACD_DIFF", "MACD_DEA",
		"RSI1", "RSI2", "RSI3",
		"BIAS1", "BIAS2", "BIAS3",
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
