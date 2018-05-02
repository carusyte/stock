package getd

import (
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"

	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/model"
	"github.com/pkg/errors"
)

type xCorlTrnDBJob struct {
	stock  *model.Stock
	xcorls []*model.XCorlTrn
}

//CalXCorl calculates cross correlation for stocks
func CalXCorl(stocks *model.Stocks) (rstks *model.Stocks) {
	var wg sync.WaitGroup
	pl := int(float64(runtime.NumCPU()) * 0.8)
	wf := make(chan int, pl)
	suc := make(chan *model.Stock, global.JOB_CAPACITY)
	rstks = &model.Stocks{}
	wgr := collect(rstks, suc)
	chxcorl := make(chan *xCorlTrnDBJob, conf.Args.DBQueueCapacity)
	wgdb := goSaveXCorlTrn(chxcorl, suc)
	for _, stk := range stocks.List {
		wg.Add(1)
		wf <- 1
		go sampXCorlTrn(stk.Code, &wg, &wf)
	}
	wg.Wait()
	close(wf)

	close(chxcorl)
	wgdb.Wait()

	close(suc)
	wgr.Wait()

	log.Printf("xcorl_trn data saved. %d / %d", rstks.Size(), stocks.Size())
	if stocks.Size() != rstks.Size() {
		same, skp := stocks.Diff(rstks)
		if !same {
			log.Printf("Failed: %+v", skp)
		}
	}

	return
}

func sampXCorlTrn(code string, wg *sync.WaitGroup, wf *chan int) {
	defer func() {
		wg.Done()
		<-*wf
	}()
	//TODO realize me
	prior := conf.Args.Sampler.PriorLength
}

func goSaveXCorlTrn(chxcorl chan *xCorlTrnDBJob, suc chan *model.Stock) (wg *sync.WaitGroup) {
	wg = new(sync.WaitGroup)
	wg.Add(1)
	go func(wg *sync.WaitGroup, ch chan *xCorlTrnDBJob, suc chan *model.Stock) {
		defer wg.Done()
		for x := range ch {
			e := SaveXCorlTrn(x.xcorls...)
			if e == nil {
				suc <- x.stock
				log.Printf("%s %d %s saved", x.stock.Code, len(x.xcorls), "xcorl_trn")
			}
		}
	}(wg, chxcorl, suc)
	return
}

// SaveXCorlTrn update existing xcorl_trn data or insert new ones in database.
func SaveXCorlTrn(xs ...*model.XCorlTrn) (err error) {
	if len(xs) == 0 {
		return nil
	}
	retry := 10
	rt := 0
	code := ""
	for ; rt < retry; rt++ {
		code = xs[0].Code
		valueStrings := make([]string, 0, len(xs))
		valueArgs := make([]interface{}, 0, len(xs)*7)
		for _, e := range xs {
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, e.Code)
			valueArgs = append(valueArgs, e.Klid)
			valueArgs = append(valueArgs, e.Date)
			valueArgs = append(valueArgs, e.Rcode)
			valueArgs = append(valueArgs, e.Corl)
			valueArgs = append(valueArgs, e.Udate)
			valueArgs = append(valueArgs, e.Utime)
		}
		stmt := fmt.Sprintf("INSERT INTO xcorl_trn (code,klid,date,rcode,corl,"+
			"udate,utime) VALUES %s "+
			"on duplicate key update corl=values(corl),"+
			"udate=values(udate),utime=values(utime)",
			strings.Join(valueStrings, ","))
		_, err := dbmap.Exec(stmt, valueArgs...)
		if err != nil {
			fmt.Println(err)
			if strings.Contains(err.Error(), "Deadlock") {
				continue
			} else {
				return errors.Wrap(errors.WithStack(err), code+": failed to bulk update xcorl_trn")
			}
		}
		break
	}
	if rt >= retry {
		return errors.Wrap(err, code+": failed to bulk update xcorl_trn")
	}
	return nil
}
