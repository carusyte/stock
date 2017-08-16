package score

import (
	"testing"
	"github.com/sirupsen/logrus"
)

func TestKdjV_RenewStats(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	//new(KdjV).RenewStats([]string{"600104"})
	new(KdjV).RenewStats(false,"000006")
}
