package score

import (
	"testing"
	"log"
	"github.com/montanaflynn/stats"
	"math"
	"time"
	logr "github.com/sirupsen/logrus"
)

func TestCombineResults(t *testing.T) {
	r1 := new(HiD).Geta()
	r1.Weight = 0.5
	r2 := new(BlueChip).Geta()
	r2.Weight = 0.5
	log.Printf("\n%+v", Combine(r1, r2).Sort())
}

func TestKdjV(t *testing.T) {
	initLog()
	start := time.Now()
	r1 := new(KdjV).Get([]string{"000429"}, -1, false)
	log.Printf("\n%+v", r1)
	log.Printf("time cost: %v", time.Since(start).Seconds())
}

func initLog() {
	//logr.SetOutput(os.Stdout)
	logr.SetLevel(logr.DebugLevel)
}

func TestCorrelation(t *testing.T) {
	a := []float64{8, 1, 7, 3}

	b := []float64{100, 30, 70, 3}
	c := []float64{1.2, 1.3, 1.4, 1.5}
	d := []float64{2.3, 2.7, 2.8, 9}
	e := []float64{1.1, 1.2, 1.3, 1.4}
	f := []float64{9, 0, 8, 5}

	g := []float64{9, 0, 8, 5, 10, 30, 66, 21, 4}
	h := []float64{10, 1, 9, 6, 11, 32, 69, 26, 7}

	z, _ := stats.Correlation(a, b)
	log.Println(z)
	z, _ = stats.Covariance(a, b)
	log.Println(z)
	z, _ = stats.Correlation(a, c)
	log.Println(z)
	z, _ = stats.Correlation(a, d)
	log.Println(z)
	z, _ = stats.Correlation(a, e)
	log.Println(z)
	z, _ = stats.Covariance(a, e)
	log.Println(z)
	z = diff(a, f)
	log.Println(z)
	z, _ = stats.Correlation(a, f)
	log.Println(z)

	z = diff(g, h)
	log.Println(z)
}

func diff(a, b []float64) float64 {
	s := .0
	for i := 0; i < len(a); i++ {
		s += math.Pow(a[i]-b[i], 2)
	}
	return math.Pow(s/float64(len(a)), 0.5)
}
