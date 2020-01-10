package getd

import (
	"fmt"
	"testing"

	"github.com/sirupsen/logrus"
)

func TestCollectIndicatorStats(t *testing.T) {
	t.Fail()
	sqlt, e := dot.Raw("COLLECT_INDICATOR_STANDARDIZATION_STATS")
	if e != nil {
		logrus.Printf("failed to get fs_stats sql %+v", e)
		return
	}
	tabs := []string{"indicator_d"}
	fields := []string{
		"BOLL_lower", "BOLL_lower_o", "BOLL_lower_h", "BOLL_lower_l", "BOLL_lower_c",
		"BOLL_mid", "BOLL_mid_o", "BOLL_mid_h", "BOLL_mid_l", "BOLL_mid_c",
		"BOLL_upper", "BOLL_upper_o", "BOLL_upper_h", "BOLL_upper_l", "BOLL_upper_c",
	}
	for _, t := range tabs {
		for _, f := range fields {
			usql := fmt.Sprintf(sqlt, t, f)
			_, e = dbmap.Exec(usql)
			if e != nil {
				logrus.Printf("failed to update fs_stats for field %s: %+v", f, e)
				continue
			}
			logrus.Printf("fs_stats for table %s field %s updated.", t, f)
		}
	}
}
