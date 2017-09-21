package getd

import (
	"testing"
	"github.com/carusyte/stock/model"
	"log"
	"time"
	"sync"
	"github.com/satori/go.uuid"
	"fmt"
	"github.com/sirupsen/logrus"
	"runtime"
	"github.com/montanaflynn/stats"
	"math"
)

func TestConcurrentModifySlice(t *testing.T) {
	ofs := make([]float64, 150000)
	cfs := new(FloatSlice)
	cfs2 := new(FloatSlice)
	log.Printf("initializing slice of size %d", len(ofs))
	for i := 0; i < len(ofs); i++ {
		ofs[i] = float64(i)
	}
	log.Println("complete")
	cfs.Set(ofs)
	cfs2.Set(ofs)
	st := time.Now()
	for i := 0; i < len(ofs); {
		v := ofs[i]
		if int(v)%2 == 0 {
			if i < len(ofs)-1 {
				ofs = append(ofs[:i], ofs[i+1:]...)
			} else {
				ofs = ofs[:i]
			}
		} else {
			i++
		}
	}
	log.Printf("single threaded: %d,   time: %.2f", len(ofs), time.Since(st).Seconds())

	st = time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(f *FloatSlice, wg *sync.WaitGroup) {
			defer wg.Done()
			for i := 0; i < f.Len(); {
				if int(f.Get(i))%2 == 0 {
					f.Remove(i)
				} else {
					i++
				}
			}
		}(cfs, &wg)
	}
	wg.Wait()
	log.Printf("4 threads: %d,   time: %.2f", cfs.Len(), time.Since(st).Seconds())
	st = time.Now()
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(f *FloatSlice, wg *sync.WaitGroup) {
			defer wg.Done()
			for i := 0; i < f.Len(); {
				if int(f.Get(i))%2 == 0 {
					f.Remove(i)
				} else {
					i++
				}
			}
		}(cfs2, &wg)
	}
	wg.Wait()
	log.Printf("8 threads: %d,   time: %.2f", cfs.Len(), time.Since(st).Seconds())
}

func TestModifySlice(t *testing.T) {
	s := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	//for i, v := range s {
	//	log.Printf("i: %d, v: %d, len: %d", i, v, len(s))
	//	if i%2 == 0 {
	//		s = append(s[:i], s[i+1:]...)
	//		log.Printf("remove: %d, len: %d", v, len(s))
	//	}
	//}
	for i := 0; i < len(s); {
		v := s[i]
		log.Printf("i: %d, v: %d, len: %d", i, v, len(s))
		if v%2 == 0 {
			if i < len(s)-2 {
				s = append(s[:i], s[i+1:]...)
			} else {
				s = s[:i]
			}
			log.Printf("remove: %d, len: %d", v, len(s))
		} else {
			i++
		}
	}
	log.Printf("finished: %+v", s)
}

func TestCompactKdjFeatDat(t *testing.T) {
	fdvs := GetKdjFeatDatRaw(model.DAY, true, 3)
	for i := 0; i < 3; i++ {
		log.Printf("LOOP: %d", i+1)
		fdvs = prune(fdvs)
	}
}

func prune(fdvs []*model.KDJfdrView) (r []*model.KDJfdrView) {
	st := time.Now()
	ol := len(fdvs)
	log.Printf("LEN: %d", len(fdvs))
	for i := 0; i < len(fdvs); i++ {
		f1 := fdvs[i]
		pend := make([]*model.KDJfdrView, 1, 16)
		pend[0] = f1
		for j := 0; j < len(fdvs); {
			f2 := fdvs[j]
			if f1 == f2 {
				j++
				continue
			}
			d := CalcKdjDevi(f1.K, f1.D, f1.J, f2.K, f2.D, f2.J)
			if d >= 0.99 {
				pend = append(pend, f2)
				if j < len(fdvs)-1 {
					fdvs = append(fdvs[:j], fdvs[j+1:]...)
				} else {
					fdvs = fdvs[:j]
				}
			} else {
				j++
			}
		}
		if len(pend) < 2 {
			continue
		}
		log.Printf("found %d similar", len(pend))
		nk := make([]float64, len(f1.K))
		nd := make([]float64, len(f1.D))
		nj := make([]float64, len(f1.J))
		for j := 0; j < len(f1.K); j++ {
			sk := 0.
			sd := 0.
			sj := 0.
			for _, f := range pend {
				sk += f.K[j]
				sd += f.D[j]
				sj += f.J[j]
			}
			deno := float64(len(pend))
			nk[j] = sk / deno
			nd[j] = sd / deno
			nj[j] = sj / deno
		}
		f1.K = nk
		f1.D = nd
		f1.J = nj
	}
	nl := len(fdvs)
	log.Printf("old: %d, new: %d,   time: %.2f", ol, nl, time.Since(st).Seconds())
	return fdvs
}

type FloatSlice struct {
	fs   []float64
	lock sync.RWMutex
}

func (s *FloatSlice) Set(f []float64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.fs = make([]float64, len(f))
	copy(s.fs, f)
}

func (s *FloatSlice) Get(i int) float64 {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.fs[i]
}

func (s *FloatSlice) Len() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return len(s.fs)
}

func (s *FloatSlice) Remove(i int) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if i < len(s.fs)-1 {
		s.fs = append(s.fs[:i], s.fs[i+1:]...)
	} else {
		s.fs = s.fs[:i]
	}
}

func TestUUID(t *testing.T) {
	for i := 0; i < 50; i++ {
		uuid := fmt.Sprintf("%s", uuid.NewV1())
		log.Printf("%s,   len:%d", uuid, len(uuid))
	}
}

func TestPruneKdjFeatDat(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	PruneKdjFeatDat(KDJ_FD_PRUNE_PREC, KDJ_PRUNE_RATE, true)
}

func TestConcurrentLoop(t *testing.T) {
	s := time.Now()
	for i := 0; i < 90000; i++ {
		_ = make([]int, 10000)
	}
	log.Printf("single: %.2f", time.Since(s).Seconds())
	var wg sync.WaitGroup
	s = time.Now()
	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 30000; i++ {
				_ = make([]int, 10000)
			}
		}()
	}
	wg.Wait()
	log.Printf("3 gr: %.2f", time.Since(s).Seconds())
}

func TestFloat2Int(t *testing.T) {
	p, _ := stats.Round(float64(runtime.NumCPU())*0.7, 0)
	log.Println(p)
}

func TestIntFloatCalculation(t *testing.T) {
	is := make([]int, 100000)
	fs := make([]float64, 100000)
	for i:=0;i<len(is);i++{
		is[i] = i
	}
	for i:=0;i<len(fs);i++{
		fs[i] = math.Sqrt(float64(i))
	}
	st := time.Now()
	sf := .0
	for i:=0;i<len(fs);i++{
		sf += fs[i]
	}
	log.Printf("float: %d", time.Since(st).Nanoseconds())
	st = time.Now()
	s := 0
	for i:=0;i<len(is);i++{
		s += is[i]
	}
	log.Printf("int: %d", time.Since(st).Nanoseconds())
}