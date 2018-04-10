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
	fields := []string{"lr", "lr_l", "lr_h", "lr_o", "lr_vol"}
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
	fields = []string{"KDJ_K", "KDJ_D", "KDJ_J"}
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
