package getd

import "log"

//CollectFsStats updates feature scaling stats.
func CollectFsStats() {
	_, e := dbmap.Exec("delete from fs_stats where method = ?", "standardization")
	if e != nil {
		log.Printf("failed to clean up fs_stats %+v", e)
	}
	usql, e := dot.Raw("COLLECT_STANDARDIZATION_STATS")
	_, e = dbmap.Exec(usql)
	if e != nil {
		log.Printf("failed to update fs_stats %+v", e)
	}
}
