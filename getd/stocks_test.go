package main

import (
	"github.com/carusyte/stock/model"
	"log"
	"sync"
	"testing"
)

func TestParseBonusPage(t *testing.T) {
	var (
		wg    sync.WaitGroup
		wgget sync.WaitGroup
		//xdxrs []*model.Xdxr
	)
	chxdxr := make(chan []*model.Xdxr, 16)
	wgget.Add(1)
	go func() {
		defer wgget.Done()
		c := 1
		for xdxr := range chxdxr {
			if len(xdxr) > 0 {
				log.Printf("%d : %s[%s] : %d", c, xdxr[0].Code, xdxr[0].Name, len(xdxr))
				for _, x := range xdxr {
					log.Printf("%+v", x)
				}
			} else {
				log.Printf("%d : %d", c, len(xdxr))
			}
			c++
		}
	}()
	s := &model.Stock{}
	s.Code = "000876"
	s.Name = "新 希 望"
	wg.Add(1)
	parseBonusPage(chxdxr, s, &wg)
	wg.Wait()
	close(chxdxr)
	wgget.Wait()
}
