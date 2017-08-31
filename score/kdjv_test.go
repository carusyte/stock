package score

import (
	"testing"
	"github.com/sirupsen/logrus"
	"log"
)

func TestKdjv_SyncRemoteKdjFd(t *testing.T) {
	kdjv := new(KdjV)
	suc := kdjv.SyncKdjFeatDat()
	if !suc {
		log.Printf("KDJ FeatDat Sync Failed.")
	}
}

func TestKdjV_RenewStats(t *testing.T) {
	//new(KdjV).RenewStats([]string{"600104"})
	kdjv := new(KdjV)
	kdjv.RenewStats(false)
	//kdjv.RenewStats(false)
}

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}
