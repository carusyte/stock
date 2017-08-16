package getd

import (
	"fmt"
	"strings"
	"github.com/carusyte/stock/model"
	"math"
	"log"
	"github.com/carusyte/stock/util"
	logr "github.com/sirupsen/logrus"
	"time"
	"sync"
	"github.com/carusyte/stock/indc"
	"github.com/satori/go.uuid"
	"runtime"
)

var kdjFdMap map[string][]*model.KDJfdrView = make(map[string][]*model.KDJfdrView)
var lock = sync.RWMutex{}

func GetKdjHist(code string, tab model.DBTab, retro int, toDate string) (indcs []*model.Indicator) {
	defer func() {
		if r := recover(); r != nil {
			if er, hasError := r.(error); hasError {
				log.Panicf("%s, %s, %d, %s, error:\n%+v", code, tab, retro, toDate, er)
			}
		}
	}()
	var (
		e   error
		sql string
	)
	if toDate == "" {
		if retro > 0 {
			sql = fmt.Sprintf("SELECT * FROM (SELECT * FROM %s WHERE code = ? ORDER BY klid DESC LIMIT ?) t"+
				" ORDER BY t.klid", tab)
			_, e = dbmap.Select(&indcs, sql, code, retro)
		} else {
			sql = fmt.Sprintf("SELECT * FROM %s WHERE code = ? ORDER BY klid", tab)
			_, e = dbmap.Select(&indcs, sql, code)
		}
		if e != nil {
			if "sql: no rows in result set" != e.Error() {
				return
			} else {
				log.Panicf("%s failed to query kdj hist, sql: %s, \n%+v", code, sql, e)
			}
		}
	} else {
		if retro > 0 {
			sql := fmt.Sprintf("SELECT * FROM (SELECT * FROM %s WHERE code = ? and date <= ? ORDER BY klid "+
				"DESC LIMIT ?) t ORDER BY t.klid", tab)
			_, e = dbmap.Select(&indcs, sql, code, toDate, retro)
		} else {
			sql := fmt.Sprintf("SELECT * FROM %s WHERE code = ? and date <= ? ORDER BY klid", tab)
			_, e = dbmap.Select(&indcs, sql, code, toDate)
		}
		if e != nil {
			if "sql: no rows in result set" != e.Error() {
				return
			} else {
				log.Panicf("%s failed to query kdj hist, sql: %s, \n%+v", code, sql, e)
			}
		}
		if indcs[len(indcs)-1].Date == toDate {
			return
		}
		var oqs []*model.Quote
		switch tab {
		case model.INDICATOR_DAY:
			return
		case model.INDICATOR_WEEK:
			sql = "select * from kline_w where code = ? and date < ? order by klid"
		case model.INDICATOR_MONTH:
			sql = "select * from kline_m where code = ? and date < ? order by klid"
		}
		_, e = dbmap.Select(&oqs, sql, code, toDate)
		if e != nil {
			if "sql: no rows in result set" != e.Error() {
				return
			} else {
				log.Panicf("%s failed to query kline, sql: %s, \n%+v", code, sql, e)
			}
		}
		qsdy := GetKlBtwn(code, model.KLINE_DAY, "["+indcs[len(indcs)-1].Date, toDate+"]", false)
		//FIXME 000006, indicator_w, 100, 2017-01-16, error: nil pointer
		nq := ToOne(qsdy[1:], qsdy[0].Close, oqs[len(oqs)-1].Klid)
		nidcs := indc.DeftKDJ(append(oqs, nq))
		return append(indcs, nidcs[len(nidcs)-1])
	}
	return
}

func SmpKdjFeat(code string, cytp model.CYTP, expvr, mxrt float64, mxhold int) {
	var itab, ktab model.DBTab
	switch cytp {
	case model.DAY:
		itab = model.INDICATOR_DAY
		ktab = model.KLINE_DAY
	case model.WEEK:
		itab = model.INDICATOR_WEEK
		ktab = model.KLINE_WEEK
	case model.MONTH:
		itab = model.INDICATOR_MONTH
		ktab = model.KLINE_MONTH
	default:
		log.Panicf("not supported cycle type: %+v", cytp)
	}
	hist := GetKdjHist(code, itab, 0, "")
	klhist := GetKlineDb(code, ktab, 0, false)
	if len(hist) != len(klhist) {
		log.Panicf("%s %s and %s does not match: %d:%d", code, itab, ktab, len(hist),
			len(klhist))
	}
	if len(hist) < 3 {
		log.Printf("%s %s insufficient data for sampling", code, cytp)
		return
	}
	indf, kfds := smpKdjBY(code, cytp, hist, klhist, expvr, mxrt, mxhold)
	indfSl, kfdsSl := smpKdjSL(code, cytp, hist, klhist, expvr, mxrt, mxhold)
	indf = append(indf, indfSl...)
	kfds = append(kfds, kfdsSl...)
	saveIndcFt(code, cytp, indf, kfds)
}

// sample KDJ sell point features
func smpKdjSL(code string, cytp model.CYTP, hist []*model.Indicator, klhist []*model.Quote,
	expvr float64, mxrt float64, mxhold int) (indf []*model.IndcFeatRaw, kfds []*model.KDJfdRaw) {
	dt, tm := util.TimeStr()
	kfds = make([]*model.KDJfdRaw, 0, 16)
	indf = make([]*model.IndcFeatRaw, 0, 16)
	for i := 1; i < len(hist)-1; i++ {
		kl := klhist[i]
		sc := kl.Close
		if sc < klhist[i+1].Close {
			continue
		}
		lc := math.Inf(0)
		tspan := 0
		pc := klhist[i-1].Close
		for w, j := 0, 0; i+j < len(hist); j++ {
			nc := klhist[i+j].Close
			if nc < lc {
				lc = nc
				tspan = j
			}
			if pc <= nc {
				rt := (nc - lc) / math.Abs(lc) * 100
				if rt >= mxrt || w > mxhold {
					break
				}
				if j > 0 {
					w++
				}
			} else {
				w = 0
			}
			pc = nc
		}
		if sc == 0 {
			sc = -0.01
			lc -= 0.01
		}
		mark := (lc - sc) / math.Abs(sc) * 100
		if mark <= -expvr {
			//sample backward and find the last J, D cross point
			samp := len(ToLstJDCross(hist[:i+1]))
			kft := new(model.IndcFeatRaw)
			kft.Code = code
			kft.Udate = dt;
			kft.Utime = tm;
			kft.Bysl = "SL"
			kft.Cytp = string(cytp)
			kft.Indc = "KDJ"
			kft.Mark = mark
			kft.SmpDate = hist[i-samp+1].Date
			kft.SmpNum = samp
			kft.Tspan = tspan
			kft.Mpt = mark / float64(tspan)
			fid := kft.GenFid()
			indf = append(indf, kft)
			for j := i - samp + 1; j <= i; j++ {
				kfd := new(model.KDJfdRaw)
				kfd.Fid = fid
				kfd.Code = code
				kfd.K = hist[j].KDJ_K
				kfd.D = hist[j].KDJ_D
				kfd.J = hist[j].KDJ_J
				kfd.Klid = hist[j].Klid
				kfd.Udate = dt
				kfd.Utime = tm
				kfds = append(kfds, kfd)
			}
		}
		i += tspan
	}
	return
}

//sample KDJ buy point features
func smpKdjBY(code string, cytp model.CYTP, hist []*model.Indicator, klhist []*model.Quote,
	expvr, mxrt float64, mxhold int) (indf []*model.IndcFeatRaw, kfds []*model.KDJfdRaw) {
	dt, tm := util.TimeStr()
	kfds = make([]*model.KDJfdRaw, 0, 16)
	indf = make([]*model.IndcFeatRaw, 0, 16)
	for i := 1; i < len(hist)-1; i++ {
		kl := klhist[i]
		sc := kl.Close
		if sc >= klhist[i+1].Close {
			continue
		}
		hc := math.Inf(-1)
		tspan := 0
		pc := klhist[i-1].Close
		for w, j := 0, 0; i+j < len(hist); j++ {
			nc := klhist[i+j].Close
			if nc > hc {
				hc = nc
				tspan = j
			}
			if pc >= nc {
				rt := (hc - nc) / math.Abs(hc) * 100
				if rt >= mxrt || w > mxhold {
					break
				}
				if j > 0 {
					w++
				}
			} else {
				w = 0
			}
			pc = nc
		}
		if sc == 0 {
			sc = 0.01
			hc += 0.01
		}
		mark := (hc - sc) / math.Abs(sc) * 100
		if mark >= expvr {
			//sample backward and find the last J, D cross point
			samp := len(ToLstJDCross(hist[:i+1]))
			kft := new(model.IndcFeatRaw)
			kft.Code = code
			kft.Udate = dt;
			kft.Utime = tm;
			kft.Bysl = "BY"
			kft.Cytp = string(cytp)
			kft.Indc = "KDJ"
			kft.Mark = mark
			kft.SmpDate = hist[i-samp+1].Date
			kft.SmpNum = samp
			kft.Tspan = tspan
			kft.Mpt = mark / float64(tspan)
			fid := kft.GenFid()
			indf = append(indf, kft)
			for j := i - samp + 1; j <= i; j++ {
				kfd := new(model.KDJfdRaw)
				kfd.Fid = fid
				kfd.Code = code
				kfd.K = hist[j].KDJ_K
				kfd.D = hist[j].KDJ_D
				kfd.J = hist[j].KDJ_J
				kfd.Klid = hist[j].Klid
				kfd.Udate = dt
				kfd.Utime = tm
				kfds = append(kfds, kfd)
			}
		}
		i += tspan
	}
	return
}

func ToLstJDCross(kdjs []*model.Indicator) (cross []*model.Indicator) {
	c := 1
	for i := len(kdjs) - 1; i > 0; i-- {
		j := kdjs[i].KDJ_J
		d := kdjs[i].KDJ_D
		if j == d {
			if c < 3 {
				c = int(math.Min(3.0, float64(len(kdjs))))
			}
			return kdjs[len(kdjs)-c:]
		}
		pj := kdjs[i-1].KDJ_J
		pd := kdjs[i-1].KDJ_D
		c++
		if pj == pd {
			if c < 3 {
				c = int(math.Min(3.0, float64(len(kdjs))))
			}
			return kdjs[len(kdjs)-c:]
		}
		if (j < d && pj < pd) || (j > d && pj > pd) {
			continue
		}
		if c < 3 {
			c = int(math.Min(3.0, float64(len(kdjs))))
		}
		return kdjs[len(kdjs)-c:]
	}
	return kdjs;
}

func GetKdjFeatDat(cytp model.CYTP, buy bool, num int) []*model.KDJfdrView {
	bysl := "BY"
	if !buy {
		bysl = "SL"
	}
	mk := fmt.Sprintf("%s-%s-%d", cytp, bysl, num)
	lock.Lock()
	defer lock.Unlock()
	if fdvs, exists := kdjFdMap[mk]; exists {
		return fdvs
	}
	start := time.Now()
	sql, e := dot.Raw("KDJ_FEAT_DAT_RAW")
	util.CheckErr(e, "failed to get KDJ_FEAT_DAT_RAW sql")
	rows, e := dbmap.Query(sql, string(cytp)+bysl+"%", cytp, bysl, num)
	if e != nil {
		if "sql: no rows in result set" != e.Error() {
			fdvs := make([]*model.KDJfdrView, 0)
			kdjFdMap[mk] = fdvs
			return fdvs
		} else {
			log.Panicf("failed to query kdj feat dat, sql:\n%s\n%+v", sql, e)
		}
	}
	defer rows.Close()
	var (
		code, fid, smpDate string
		pcode, pfid        string
		smpNum, klid       int
		k, d, j            float64
		kfv                *model.KDJfdrView
	)
	fdvs := make([]*model.KDJfdrView, 0, 16)
	for rows.Next() {
		rows.Scan(&code, &fid, &smpDate, &smpNum, &klid, &k, &d, &j)
		if code != pcode || fid != pfid {
			kfv = newKDJfdView(code, smpDate, smpNum)
			fdvs = append(fdvs, kfv)
		}
		kfv.Add(klid, k, d, j)
		pcode = code
		pfid = fid
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	kdjFdMap[mk] = fdvs
	logr.Debugf("query kdj_feat_dat_raw(%s,%s,%d): %.2f", cytp, bysl, num, time.Since(start).Seconds())
	return fdvs
}

func newKDJfdView(code, date string, num int) *model.KDJfdrView {
	return &model.KDJfdrView{code, date, num, make([]int, 0, 16), make([]float64, 0, 16),
		make([]float64, 0, 16), make([]float64, 0, 16)}
}

func purgeKdjFeatDat(code string) {
	tran, e := dbmap.Begin()
	util.CheckErr(e, "failed to begin new transaction")
	//purge data of this code before insertion
	_, e = tran.Exec("delete from indc_feat_raw where code = ?", code)
	if e != nil {
		log.Printf("failed to purge indc_feat_raw, %s", code)
		tran.Rollback()
		log.Panicln(e)
	}
	_, e = tran.Exec("delete from kdj_feat_dat_raw where code = ?", code)
	if e != nil {
		log.Printf("failed to purge indc_feat_raw, %s", code)
		tran.Rollback()
		log.Panicln(e)
	}
	tran.Commit()
}

func saveIndcFt(code string, cytp model.CYTP, feats []*model.IndcFeatRaw, kfds []*model.KDJfdRaw) {
	tran, e := dbmap.Begin()
	util.CheckErr(e, "failed to begin new transaction")
	if len(feats) > 0 && len(kfds) > 0 {
		valueStrings := make([]string, 0, len(feats))
		valueArgs := make([]interface{}, 0, len(feats)*13)
		var code string
		for _, f := range feats {
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, f.Code)
			valueArgs = append(valueArgs, f.Indc)
			valueArgs = append(valueArgs, f.Cytp)
			valueArgs = append(valueArgs, f.Bysl)
			valueArgs = append(valueArgs, f.SmpDate)
			valueArgs = append(valueArgs, f.SmpNum)
			valueArgs = append(valueArgs, f.Fid)
			valueArgs = append(valueArgs, f.Mark)
			valueArgs = append(valueArgs, f.Tspan)
			valueArgs = append(valueArgs, f.Mpt)
			valueArgs = append(valueArgs, f.Remarks)
			valueArgs = append(valueArgs, f.Udate)
			valueArgs = append(valueArgs, f.Utime)
			code = f.Code
		}
		stmt := fmt.Sprintf("INSERT INTO indc_feat_raw (code,indc,cytp,bysl,smp_date,smp_num,fid,mark,tspan,mpt,"+
			"remarks,"+
			"udate,utime) VALUES %s on duplicate key update smp_num=values(smp_num),mark=values(mark),tspan=values"+
			"(tspan),mpt=values(mpt),remarks=values(remarks),udate=values(udate),utime=values(utime)",
			strings.Join(valueStrings, ","))
		_, err := tran.Exec(stmt, valueArgs...)
		if err != nil {
			log.Printf("%s failed to bulk insert indc_feat_raw", code)
			tran.Rollback()
			log.Panicln(err)
		}

		valueStrings = make([]string, 0, len(kfds))
		valueArgs = make([]interface{}, 0, len(kfds)*8)
		for _, k := range kfds {
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, k.Code)
			valueArgs = append(valueArgs, k.Fid)
			valueArgs = append(valueArgs, k.Klid)
			valueArgs = append(valueArgs, k.K)
			valueArgs = append(valueArgs, k.D)
			valueArgs = append(valueArgs, k.J)
			valueArgs = append(valueArgs, k.Udate)
			valueArgs = append(valueArgs, k.Utime)
		}
		stmt = fmt.Sprintf("INSERT INTO kdj_feat_dat_raw (code,fid,klid,k,d,j,"+
			"udate,utime) VALUES %s on duplicate key update k=values(k),d=values(d),"+
			"j=values(j),udate=values(udate),utime=values(utime)",
			strings.Join(valueStrings, ","))
		_, err = tran.Exec(stmt, valueArgs...)
		if err != nil {
			log.Printf("%s failed to bulk insert kdj_feat_dat_raw", code)
			tran.Rollback()
			log.Panicln(err)
		}

		tran.Commit()
	}
}

func PruneKdjFeatDat(prec float64, pass int) {
	logr.Debugf("Pruning KDJ feature data. precision:%f, pass:%d", prec, pass)
	var fdks []*fdKey
	_, e := dbmap.Select(&fdks, "select cytp, bysl, smp_num, count(*) count from indc_feat_raw group by cytp, bysl, "+
		"smp_num")
	if e != nil {
		if "sql: no rows in result set" == e.Error() {
			return
		} else {
			log.Panicln("failed to query indc_feat_dat_raw", e)
		}
	}
	_, e = dbmap.Exec("truncate table indc_feat")
	util.CheckErr(e, "failed to truncate indc_feat")
	_, e = dbmap.Exec("truncate table kdj_feat_dat")
	util.CheckErr(e, "failed to truncate kdj_feat_dat")
	var wg sync.WaitGroup
	chfdk := make(chan *fdKey, JOB_CAPACITY)
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go doPruneKdjFeatDat(chfdk, &wg, prec, pass)
	}
	sumbf := 0
	for _, k := range fdks {
		sumbf += k.Count
		chfdk <- k
	}
	close(chfdk)
	wg.Wait()
	sumaf, e := dbmap.SelectInt("select count(*) from indc_feat")
	util.CheckErr(e, "failed to count indc_feat")
	log.Printf("raw kdj feature data pruned. before: %d, after: %d, rate: %.2f%%", sumbf, sumaf,
		float64(sumaf)/float64(sumbf))
}

func doPruneKdjFeatDat(chfdk chan *fdKey, wg *sync.WaitGroup, prec float64, pass int) {
	defer wg.Done()
	for fdk := range chfdk {
		st := time.Now()
		fdrvs := GetKdjFeatDat(model.CYTP(fdk.Cytp), fdk.Bysl == "BY", fdk.SmpNum)
		logr.Debugf("pruning: %s-%s-%d size: %d", fdk.Cytp, fdk.Bysl, fdk.SmpNum, len(fdrvs))
		fdvs := convert2Fdvs(fdk, fdrvs)
		for p := 0; p < pass; p++ {
			fdvs = passKdjFeatDatPrune(fdk, fdvs, prec)
		}
		for _, fdv := range fdvs {
			fdv.Weight = float64(fdv.SmpNum) / float64(len(fdrvs))
		}
		saveKdjFd(fdvs)
		logr.Debugf("%s-%s-%d pruned, before: %d, after: %d,  time: %.2f", fdk.Cytp, fdk.Bysl, fdk.SmpNum, fdk.Count,
			len(fdvs), time.Since(st).Seconds())
	}
}

func saveKdjFd(fdvs []*model.KDJfdView) {
	tran, e := dbmap.Begin()
	util.CheckErr(e, "failed to begin new transaction")
	if len(fdvs) > 0 {
		valueStrings := make([]string, 0, len(fdvs))
		valueArgs := make([]interface{}, 0, len(fdvs)*10)
		dt, tm := util.TimeStr()
		for _, f := range fdvs {
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, f.Indc)
			valueArgs = append(valueArgs, f.Fid)
			valueArgs = append(valueArgs, f.Cytp)
			valueArgs = append(valueArgs, f.Bysl)
			valueArgs = append(valueArgs, f.SmpNum)
			valueArgs = append(valueArgs, f.FdNum)
			valueArgs = append(valueArgs, f.Weight)
			valueArgs = append(valueArgs, f.Remarks)
			valueArgs = append(valueArgs, dt)
			valueArgs = append(valueArgs, tm)
		}
		stmt := fmt.Sprintf("INSERT INTO indc_feat (indc,fid,cytp,bysl,smp_num,fd_num,weight,remarks,"+
			"udate,utime) VALUES %s on duplicate key update fid=values(fid),fd_num=values(fd_num),weight=values"+
			"(weight),remarks=values(remarks),udate=values(udate),utime=values(utime)",
			strings.Join(valueStrings, ","))
		_, err := tran.Exec(stmt, valueArgs...)
		if err != nil {
			tran.Rollback()
			log.Panicln("failed to bulk insert indc_feat", err)
		}

		for _, f := range fdvs {
			valueStrings = make([]string, 0, f.SmpNum)
			valueArgs = make([]interface{}, 0, f.SmpNum*7)
			for i := 0; i < f.SmpNum; i++ {
				valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?)")
				valueArgs = append(valueArgs, f.Fid)
				valueArgs = append(valueArgs, i)
				valueArgs = append(valueArgs, f.K[i])
				valueArgs = append(valueArgs, f.D[i])
				valueArgs = append(valueArgs, f.J[i])
				valueArgs = append(valueArgs, dt)
				valueArgs = append(valueArgs, tm)
			}
			stmt = fmt.Sprintf("INSERT INTO kdj_feat_dat (fid,seq,k,d,j,"+
				"udate,utime) VALUES %s on duplicate key update k=values(k),d=values(d),"+
				"j=values(j),udate=values(udate),utime=values(utime)",
				strings.Join(valueStrings, ","))
			_, err = tran.Exec(stmt, valueArgs...)
			if err != nil {
				tran.Rollback()
				log.Panicln("failed to bulk insert kdj_feat_dat", err)
			}
		}
		tran.Commit()
	}
}

func passKdjFeatDatPrune(fdk *fdKey, fdvs []*model.KDJfdView, prec float64) ([]*model.KDJfdView) {
	for i := 0; i < len(fdvs); i++ {
		f1 := fdvs[i]
		pend := make([]*model.KDJfdView, 1, 16)
		pend[0] = f1
		for j := 0; j < len(fdvs); {
			f2 := fdvs[j]
			if f1 == f2 {
				j++
				continue
			}
			d := CalcKdjDevi(f1.K, f1.D, f1.J, f2.K, f2.D, f2.J)
			if d >= prec {
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
		logr.Debugf("%s-%s-%d found %d similar", fdk.Cytp, fdk.Bysl, fdk.SmpNum, len(pend))
		nk := make([]float64, len(f1.K))
		nd := make([]float64, len(f1.D))
		nj := make([]float64, len(f1.J))
		for j := 0; j < f1.SmpNum; j++ {
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
		for _, pf := range pend {
			if f1 == pf {
				continue
			}
			f1.FdNum += pf.FdNum
		}
	}
	return fdvs
}

func convert2Fdvs(key *fdKey, fdrvs []*model.KDJfdrView) []*model.KDJfdView {
	fdvs := make([]*model.KDJfdView, len(fdrvs))
	for i := 0; i < len(fdrvs); i++ {
		fdv := new(model.KDJfdView)
		fdrv := fdrvs[i]
		fdv.SmpNum = fdrv.SmpNum
		fdv.K = make([]float64, len(fdrv.K))
		fdv.D = make([]float64, len(fdrv.D))
		fdv.J = make([]float64, len(fdrv.J))
		copy(fdv.K, fdrv.K)
		copy(fdv.D, fdrv.D)
		copy(fdv.J, fdrv.J)
		fdv.FdNum = 1
		fdv.Indc = "KDJ"
		fdv.Fid = fmt.Sprintf("%s", uuid.NewV1())
		fdv.Cytp = model.CYTP(key.Cytp)
		fdv.Bysl = key.Bysl
		fdvs[i] = fdv
	}
	return fdvs
}

type fdKey struct {
	Cytp   string
	Bysl   string
	SmpNum int `db:"smp_num"`
	Count  int
}

func CalcKdjDevi(sk, sd, sj, tk, td, tj []float64) float64 {
	kcc, e := util.Devi(sk, tk)
	util.CheckErr(e, fmt.Sprintf("failed to calculate kcc: %+v, %+v", sk, tk))
	dcc, e := util.Devi(sd, td)
	util.CheckErr(e, fmt.Sprintf("failed to calculate dcc: %+v, %+v", sd, td))
	jcc, e := util.Devi(sj, tj)
	util.CheckErr(e, fmt.Sprintf("failed to calculate jcc: %+v, %+v", sj, tj))
	scc := (kcc*1.0 + dcc*4.0 + jcc*5.0) / 10.0
	return -0.001*math.Pow(scc, math.E) + 1
}
