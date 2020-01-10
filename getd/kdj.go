package getd

import (
	"bytes"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"
	"time"

	rm "github.com/carusyte/rima/model"
	"github.com/carusyte/stock/conf"
	"github.com/carusyte/stock/indc"
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/rpc"
	"github.com/carusyte/stock/util"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	logr "github.com/sirupsen/logrus"
)

const (
	LOCAL_PRUNE_THRESHOLD = 50000
)

var (
	kdjFdrMap map[string][]*model.KDJfdrView = make(map[string][]*model.KDJfdrView)
	kdjFdMap  map[string][]*model.KDJfdView  = make(map[string][]*model.KDJfdView)
	lock                                     = sync.RWMutex{}
)

//GetKdjHist Find kdj history up to 'toDate', limited to 'retro' rows. If retro <= 0, no limit is set.
// If toDate is an empty string, no bound is set on date.
func GetKdjHist(code string, tab model.DBTab, retro int, toDate string) (indcs []*model.Indicator) {
	defer func() {
		if r := recover(); r != nil {
			if er, hasError := r.(error); hasError {
				logrus.Panicf("%s, %s, %d, %s, error:\n%+v", code, tab, retro, toDate, er)
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
			if "sql: no rows in result set" == e.Error() {
				logr.Warnf("%s, %s, %d: %+v", code, tab, retro, e.Error())
				return
			}
			logrus.Panicf("%s failed to query kdj hist, sql: %s, \n%+v", code, sql, e)
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
			if "sql: no rows in result set" == e.Error() {
				logr.Warnf("%s, %s, %s, %d: %s", code, tab, toDate, retro, e.Error())
				return
			} else {
				logrus.Panicf("%s failed to query kdj hist, sql: %s, \n%+v", code, sql, e)
			}
		}
		if len(indcs) > 0 && indcs[len(indcs)-1].Date == toDate {
			return
		}
		if len(indcs) > 1 {
			switch tab {
			case model.INDICATOR_DAY:
				return
			case model.INDICATOR_WEEK:
				sql = "select * from kline_w_f where code = ? and date < ? order by klid"
			case model.INDICATOR_MONTH:
				sql = "select * from kline_m_f where code = ? and date < ? order by klid"
			}
			var oqs []*model.TradeDataBase
			_, e = dbmap.Select(&oqs, sql, code, toDate)
			if e != nil {
				if "sql: no rows in result set" == e.Error() {
					logr.Warnf("%s, %s, sql: %s, %s: %+v, ", code, tab, toDate, sql, e.Error())
					return
				}
				logrus.Panicf("%s failed to query kline, sql: %s, \n%+v", code, sql, e)
			}
			qsdy := GetTrDataBtwn(
				code,
				TrDataQry{
					Cycle:     model.DAY,
					Reinstate: model.Forward,
					Basic:     true,
				},
				Date,
				"["+indcs[len(indcs)-1].Date,
				toDate+"]",
				false)
			nq := ToOne(qsdy.Base[1:], qsdy.Base[0].Close, oqs[len(oqs)-1].Klid)
			nidcs := indc.DeftKDJ(append(oqs, nq))
			return append(indcs, nidcs[len(nidcs)-1])
		}
		qsdy := GetTrDataBtwn(
			code,
			TrDataQry{
				Cycle:     model.DAY,
				Reinstate: model.Forward,
				Basic:     true,
			},
			Date,
			"",
			toDate+"]",
			false)
		nq := ToOne(qsdy.Base[1:], qsdy.Base[0].Close, -1)
		nidcs := indc.DeftKDJ([]*model.TradeDataBase{nq})
		return nidcs
	}
	return
}

//SmpKdjFeat sample kdj features
func SmpKdjFeat(code string, cytp model.CYTP, expvr, mxrt float64, mxhold int) {
	//TODO tag cross?
	var (
		itab, ktab model.DBTab
		minSize    int
	)
	switch cytp {
	case model.DAY:
		itab = model.INDICATOR_DAY
		ktab = model.KLINE_DAY
		minSize = 200
	case model.WEEK:
		itab = model.INDICATOR_WEEK
		ktab = model.KLINE_WEEK
		minSize = 30
	case model.MONTH:
		itab = model.INDICATOR_MONTH
		ktab = model.KLINE_MONTH
		minSize = 15
	default:
		logrus.Panicf("not supported cycle type: %+v", cytp)
	}
	hist := GetKdjHist(code, itab, 0, "")
	//TODO refactor: use GetTrDataDB instead
	klhist := GetKlineDb(code, ktab, 0, false)
	if len(hist) != len(klhist) {
		logrus.Panicf("%s %s and %s does not match: %d:%d", code, itab, ktab, len(hist),
			len(klhist))
	}
	if len(hist) < minSize {
		logrus.Printf("%s %s insufficient data for sampling: %d", code, cytp, len(hist))
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
			cross, fnd := ToLstJDCross(hist[:i+1])
			if fnd {
				samp := len(cross)
				kft := new(model.IndcFeatRaw)
				kft.Code = code
				kft.Udate = dt
				kft.Utime = tm
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
		}
		i += tspan
	}
	return
}

//sample KDJ buy point features, usually skip the first buy sample under IPO halo
func smpKdjBY(code string, cytp model.CYTP, hist []*model.Indicator, klhist []*model.Quote,
	expvr, mxrt float64, mxhold int) (indf []*model.IndcFeatRaw, kfds []*model.KDJfdRaw) {
	skip := true
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
			if skip {
				skip = false
			} else {
				//sample backward and find the last J, D cross point
				cross, fnd := ToLstJDCross(hist[:i+1])
				if fnd {
					samp := len(cross)
					kft := new(model.IndcFeatRaw)
					kft.Code = code
					kft.Udate = dt
					kft.Utime = tm
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
			}
		}
		i += tspan
	}
	return
}

//ToLstJDCross extract elements starting from the latest J and D cross or
// minimum element. Minimum is defined by 'Kdjv.sample_size_min' at config file.
func ToLstJDCross(kdjs []*model.Indicator) (cross []*model.Indicator, found bool) {
	c := 1
	for i := len(kdjs) - 1; i > 0; i-- {
		j := kdjs[i].KDJ_J
		d := kdjs[i].KDJ_D
		if j == d {
			if c < conf.Args.Kdjv.SampleSizeMin {
				c = int(math.Min(float64(conf.Args.Kdjv.SampleSizeMin), float64(len(kdjs))))
			}
			cross = kdjs[len(kdjs)-c:]
			return cross, len(cross) >= conf.Args.Kdjv.SampleSizeMin
		}
		pj := kdjs[i-1].KDJ_J
		pd := kdjs[i-1].KDJ_D
		c++
		if pj == pd {
			if c < conf.Args.Kdjv.SampleSizeMin {
				c = int(math.Min(float64(conf.Args.Kdjv.SampleSizeMin), float64(len(kdjs))))
			}
			cross = kdjs[len(kdjs)-c:]
			return cross, len(cross) >= conf.Args.Kdjv.SampleSizeMin
		}
		if (j < d && pj < pd) || (j > d && pj > pd) {
			continue
		}
		if c < conf.Args.Kdjv.SampleSizeMin {
			c = int(math.Min(float64(conf.Args.Kdjv.SampleSizeMin), float64(len(kdjs))))
		}
		cross = kdjs[len(kdjs)-c:]
		return cross, len(cross) >= conf.Args.Kdjv.SampleSizeMin
	}
	return kdjs, false
}

//GetKdjFeatDatRaw get kdj raw feature data from cache, or database if not found.
func GetKdjFeatDatRaw(cytp model.CYTP, buy bool, num int) []*model.KDJfdrView {
	bysl := "BY"
	if !buy {
		bysl = "SL"
	}
	mk := fmt.Sprintf("%s-%s-%d", cytp, bysl, num)
	lock.Lock()
	defer lock.Unlock()
	if fdvs, exists := kdjFdrMap[mk]; exists {
		return fdvs
	}
	start := time.Now()
	sql, e := dot.Raw("KDJ_FEAT_DAT_RAW")
	util.CheckErr(e, "failed to get KDJ_FEAT_DAT_RAW sql")
	rows, e := dbmap.Query(sql, string(cytp)+bysl+"%", cytp, bysl, num)
	if e != nil {
		if "sql: no rows in result set" == e.Error() {
			fdvs := make([]*model.KDJfdrView, 0)
			kdjFdrMap[mk] = fdvs
			return fdvs
		} else {
			logrus.Panicf("failed to query kdj feat dat raw, sql:\n%s\n%+v", sql, e)
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
			kfv = newKDJfdrView(code, smpDate, smpNum)
			fdvs = append(fdvs, kfv)
		}
		kfv.Add(klid, k, d, j)
		pcode = code
		pfid = fid
	}
	if err := rows.Err(); err != nil {
		logrus.Fatal(err)
	}
	kdjFdrMap[mk] = fdvs
	logr.Debugf("query kdj_feat_dat_raw(%s,%s,%d): %.2f", cytp, bysl, num, time.Since(start).Seconds())
	return fdvs
}

func GetKdjFeatDat(cytp model.CYTP, buy bool, num int) []*model.KDJfdView {
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
	sql, e := dot.Raw("KDJ_FEAT_DAT")
	util.CheckErr(e, "failed to get KDJ_FEAT_DAT sql")
	rows, e := dbmap.Query(sql, cytp, bysl, num)
	if e != nil {
		if "sql: no rows in result set" == e.Error() {
			fdvs := make([]*model.KDJfdView, 0)
			kdjFdMap[mk] = fdvs
			return fdvs
		} else {
			logrus.Panicf("failed to query kdj feat dat, sql:\n%s\n%+v", sql, e)
		}
	}
	defer rows.Close()
	var (
		fid                string
		pfid               string
		smpNum, fdNum, seq int
		weight, k, d, j    float64
		kfv                *model.KDJfdView
	)
	fdvs := make([]*model.KDJfdView, 0, 16)
	for rows.Next() {
		rows.Scan(&fid, &smpNum, &fdNum, &weight, &seq, &k, &d, &j)
		if fid != pfid {
			kfv = newKDJfdView(fid, bysl, cytp, smpNum, fdNum, weight)
			fdvs = append(fdvs, kfv)
		}
		kfv.Add(k, d, j)
		pfid = fid
	}
	if err := rows.Err(); err != nil {
		logrus.Panicln("failed to query kdj feat dat.", err)
	}
	kdjFdMap[mk] = fdvs
	logr.Debugf("query kdj_feat_dat(%s,%s,%d): %.2f", cytp, bysl, num, time.Since(start).Seconds())
	return fdvs
}

func GetAllKdjFeatDat() (map[string][]*model.KDJfdView, int) {
	lock.Lock()
	defer lock.Unlock()
	if len(kdjFdMap) > 0 {
		count := 0
		for _, fds := range kdjFdMap {
			count += len(fds)
		}
		return kdjFdMap, count
	}
	start := time.Now()
	sql, e := dot.Raw("KDJ_FEAT_DAT_ALL")
	util.CheckErr(e, "failed to get KDJ_FEAT_DAT_ALL sql")
	rows, e := dbmap.Query(sql)
	if e != nil {
		if "sql: no rows in result set" == e.Error() {
			return kdjFdMap, 0
		} else {
			logrus.Panicf("failed to query kdj feat dat, sql:\n%s\n%+v", sql, e)
		}
	}
	defer rows.Close()
	var (
		fid, bysl, cytp    string
		pfid, mk, pmk      string
		smpNum, fdNum, seq int
		count              = 0
		weight, k, d, j    float64
		kfv                *model.KDJfdView
	)
	fdvs := make([]*model.KDJfdView, 0, 16)
	for rows.Next() {
		rows.Scan(&fid, &bysl, &cytp, &smpNum, &fdNum, &weight, &seq, &k, &d, &j)
		mk = fmt.Sprintf("%s-%s-%d", cytp, bysl, smpNum)
		if mk != pmk && pmk != "" {
			kdjFdMap[pmk] = fdvs
			fdvs = make([]*model.KDJfdView, 0, 16)
		}
		if fid != pfid {
			kfv = newKDJfdView(fid, bysl, model.CYTP(cytp), smpNum, fdNum, weight)
			fdvs = append(fdvs, kfv)
		}
		kfv.Add(k, d, j)
		pfid = fid
		pmk = mk
		count++
	}
	kdjFdMap[mk] = fdvs
	if err := rows.Err(); err != nil {
		logrus.Panicln("failed to query kdj feat dat.", err)
	}
	logr.Debugf("query all kdj_feat_dat: %d, mk: %d,  time: %.2f", count, len(kdjFdMap), time.Since(start).Seconds())
	return kdjFdMap, count
}

func newKDJfdrView(code, date string, num int) *model.KDJfdrView {
	return &model.KDJfdrView{code, date, num, make([]int, 0, 16), make([]float64, 0, 16),
		make([]float64, 0, 16), make([]float64, 0, 16)}
}

func newKDJfdView(fid, bysl string, cytp model.CYTP, smpNum, fdNum int, weight float64) *model.KDJfdView {
	v := &model.KDJfdView{}
	v.Indc = "KDJ"
	v.Cytp = model.CYTP(cytp)
	v.Fid = fid
	v.Bysl = bysl
	v.SmpNum = smpNum
	v.FdNum = fdNum
	v.Weight = weight
	v.K = make([]float64, 0, 16)
	v.D = make([]float64, 0, 16)
	v.J = make([]float64, 0, 16)
	return v
}

func purgeKdjFeatDat(code string) {
	tran, e := dbmap.Begin()
	util.CheckErr(e, "failed to begin new transaction")
	//purge data of this code before insertion
	_, e = tran.Exec("delete from indc_feat_raw where code = ?", code)
	if e != nil {
		logrus.Printf("failed to purge indc_feat_raw, %s", code)
		tran.Rollback()
		logrus.Panicln(e)
	}
	_, e = tran.Exec("delete from kdj_feat_dat_raw where code = ?", code)
	if e != nil {
		logrus.Printf("failed to purge indc_feat_raw, %s", code)
		tran.Rollback()
		logrus.Panicln(e)
	}
	tran.Commit()
}

func saveIndcFt(code string, cytp model.CYTP, feats []*model.IndcFeatRaw, kfds []*model.KDJfdRaw) {
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

		tran, e := dbmap.Begin()
		util.CheckErr(e, "failed to begin new transaction")
		_, err := tran.Exec(stmt, valueArgs...)
		if err != nil {
			logrus.Printf("%s failed to bulk insert indc_feat_raw", code)
			tran.Rollback()
			logrus.Panicln(err)
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
			logrus.Printf("%s failed to bulk insert kdj_feat_dat_raw", code)
			tran.Rollback()
			logrus.Panicln(err)
		}

		tran.Commit()
	}
}

//PruneKdjFeatDat Merge similar kdj feature data based on devia
func PruneKdjFeatDat(prec float64, pruneRate float64, resume bool) {
	st := time.Now()
	logr.Debugf("Pruning KDJ feature data. precision:%.3f, prune rate:%.2f, resume: %t", prec, pruneRate, resume)
	var fdks []*fdKey
	var e error
	if resume {
		// skip data already in indc_feat
		sql, e := dot.Raw("KDJ_FEAT_DAT_RAW_UNPRUNED_COUNT")
		util.CheckErr(e, "failed to get sql KDJ_FEAT_DAT_RAW_UNPRUNED_COUNT")
		_, e = dbmap.Select(&fdks, sql)
	} else {
		_, e = dbmap.Select(&fdks, "select cytp, bysl, smp_num, count(*) count from "+
			"indc_feat_raw group by cytp, bysl, smp_num order by count")
	}
	if e != nil {
		if "sql: no rows in result set" == e.Error() {
			return
		}
		logrus.Panicln("failed to query indc_feat_dat_raw", e)

	}
	if !resume {
		_, e = dbmap.Exec("truncate table indc_feat")
		util.CheckErr(e, "failed to truncate indc_feat")
		_, e = dbmap.Exec("truncate table kdj_feat_dat")
		util.CheckErr(e, "failed to truncate kdj_feat_dat")
	}
	chfdk := make(chan *fdKey, JOB_CAPACITY)
	chfdks := make(chan *fdKey, JOB_CAPACITY)
	sumbf := 0
	for _, k := range fdks {
		sumbf += k.Count
		switch conf.Args.RunMode {
		case conf.AUTO:
			if k.Count > LOCAL_PRUNE_THRESHOLD {
				chfdk <- k
			} else {
				chfdks <- k
			}
		default:
			chfdk <- k
		}
	}
	var wg sync.WaitGroup
	switch conf.Args.RunMode {
	case conf.AUTO:
		// maybe we can steal jobs from remote channel if local task is done?
		p, _ := rpc.Available(false)
		for i := 0; i < p; i++ {
			wg.Add(1)
			go doPruneKdjFeatDat(chfdk, &wg, prec, pruneRate, conf.REMOTE)
		}
		wg.Add(1)
		go doPruneKdjFeatDat(chfdks, &wg, prec, pruneRate, conf.LOCAL)
	case conf.REMOTE:
		p, _ := rpc.Available(false)
		for i := 0; i < p; i++ {
			wg.Add(1)
			go doPruneKdjFeatDat(chfdk, &wg, prec, pruneRate, conf.REMOTE)
		}
	case conf.LOCAL:
		wg.Add(1)
		go doPruneKdjFeatDat(chfdk, &wg, prec, pruneRate, conf.LOCAL)
	case conf.DISTRIBUTED:
		wg.Add(1)
		go doPruneKdjFeatDat(chfdk, &wg, prec, pruneRate, conf.DISTRIBUTED)
	}
	close(chfdk)
	close(chfdks)
	wg.Wait()
	//FIXME this count is incorrect if run in resume mode
	sumaf, e := dbmap.SelectInt("select count(*) from indc_feat")
	util.CheckErr(e, "failed to count indc_feat")
	prate := float64(sumbf-int(sumaf)) / float64(sumbf) * 100
	logrus.Printf("raw kdj feature data pruned. before: %d, after: %d, rate: %.2f%%, time: %.2f",
		sumbf, sumaf, prate, time.Since(st).Seconds())
}

func doPruneKdjFeatDat(chfdk chan *fdKey, wg *sync.WaitGroup, prec float64, pruneRate float64, runMode conf.RunMode) {
	defer wg.Done()
	for fdk := range chfdk {
		st := time.Now()
		fdrvs := GetKdjFeatDatRaw(model.CYTP(fdk.Cytp), fdk.Bysl == "BY", fdk.SmpNum)
		nprec := prec * (1 - 1./math.Pow(math.E*math.Pi, math.E)*math.Pow(float64(fdk.SmpNum-2),
			1+1./(math.Sqrt2*math.Pi)))
		logr.Debugf("pruning: %s size: %d, nprec: %.3f", fdk.ID(), len(fdrvs), nprec)
		fdvs := convert2Fdvs(fdk, fdrvs)
		fdvs = smartPruneKdjFeatDat(fdk, fdvs, nprec, pruneRate, runMode)
		for _, fdv := range fdvs {
			fdv.Weight = float64(fdv.FdNum) / float64(len(fdrvs))
		}
		saveKdjFd(fdvs)
		prate := float64(fdk.Count-len(fdvs)) / float64(fdk.Count) * 100
		logr.Debugf("%s pruned and saved, before: %d, after: %d, rate: %.2f%%    time: %.2f",
			fdk.ID(), fdk.Count, len(fdvs), prate, time.Since(st).Seconds())
	}
}

func smartPruneKdjFeatDat(fdk *fdKey, fdvs []*model.KDJfdView, nprec float64,
	pruneRate float64, runMode conf.RunMode) []*model.KDJfdView {
	var e error
	if len(fdvs) <= 100 {
		return fdvs
	}
	switch runMode {
	case conf.LOCAL:
		fdvs = pruneKdjFeatDatLocal(fdk, fdvs, nprec, pruneRate, 0.9)
	case conf.REMOTE:
		fdvs, e = pruneKdjFeatDatRemote(fdk, fdvs, nprec, pruneRate)
	case conf.AUTO:
		if len(fdvs) <= LOCAL_PRUNE_THRESHOLD {
			fdvs = pruneKdjFeatDatLocal(fdk, fdvs, nprec, pruneRate, 0.8)
		} else {
			_, h := rpc.Available(false)
			if h > 0 {
				fdvs, e = pruneKdjFeatDatRemote(fdk, fdvs, nprec, pruneRate)
			} else {
				logr.Warn("no available rpc servers, using local power")
				fdvs = pruneKdjFeatDatLocal(fdk, fdvs, nprec, pruneRate, 0.9)
			}
		}
	}
	if e != nil {
		logr.Warnf("remote processing failed, fall back to local power\n%+v", e)
		fdvs = pruneKdjFeatDatLocal(fdk, fdvs, nprec, pruneRate, 0.8)
	}
	return fdvs
}

func pruneKdjFeatDatRemote(fdk *fdKey, fdvs []*model.KDJfdView, nprec float64, pruneRate float64) ([]*model.KDJfdView, error) {
	stp := time.Now()
	bfc := len(fdvs)
	req := &rm.KdjPruneReq{fdk.ID(), nprec, pruneRate, fdvs}
	var rep *rm.KdjPruneRep
	e := rpc.Call("IndcScorer.PruneKdj", req, &rep, 3)
	if e != nil {
		logrus.Printf("RPC service IndcScorer.PruneKdj failed\n%+v", e)
		return fdvs, e
	}
	fdvs = rep.Data
	prate := float64(bfc-len(fdvs)) / float64(bfc) * 100
	logr.Debugf("%s pruned(remote), before: %d, after: %d, rate: %.2f%% time: %.2f",
		fdk.ID(), bfc, len(fdvs), prate, time.Since(stp).Seconds())
	return fdvs, nil
}

func pruneKdjFeatDatLocal(fdk *fdKey, fdvs []*model.KDJfdView, nprec, pruneRate, cpuPower float64) []*model.KDJfdView {
	for prate, p := 1.0, 1; prate > pruneRate; p++ {
		stp := time.Now()
		bfc := len(fdvs)
		fdvs = passKdjFeatDatPrune(fdvs, nprec, cpuPower)
		prate = float64(bfc-len(fdvs)) / float64(bfc)
		logr.Debugf("%s pass %d, before: %d, after: %d, rate: %.2f%% time: %.2f",
			fdk.ID(), p, bfc, len(fdvs), prate*100, time.Since(stp).Seconds())
	}
	return fdvs
}

func saveKdjFd(fdvs []*model.KDJfdView) {
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
		tran, e := dbmap.Begin()
		util.CheckErr(e, "failed to begin new transaction")
		_, err := tran.Exec(stmt, valueArgs...)
		if err != nil {
			tran.Rollback()
			logrus.Panicln("failed to bulk insert indc_feat", err)
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
				logrus.Panicln("failed to bulk insert kdj_feat_dat", err)
			}
		}
		tran.Commit()
	}
}

func passKdjFeatDatPrune(fdvs []*model.KDJfdView, prec, cpuPower float64) []*model.KDJfdView {
	return passKdjFdPara(fdvs, prec, cpuPower)
}

func passKdjFdPara(fdvs []*model.KDJfdView, prec, cpuPower float64) []*model.KDJfdView {
	var wg sync.WaitGroup
	p := int(float64(runtime.NumCPU()) * cpuPower)
	ptags := new(sync.Map)
	chjob := make(chan int, len(fdvs))
	chcdd := make(chan map[int][]int, len(fdvs))
	chout := make(chan []*model.KDJfdView)
	for i := 0; i < p; i++ {
		wg.Add(1)
		go scanKdjFD(&wg, fdvs, prec, ptags, chjob, chcdd)
	}
	go reduceKdjFD(fdvs, ptags, chcdd, chout)
	for i := 0; i < len(fdvs)-1; i++ {
		chjob <- i
	}
	close(chjob)
	wg.Wait()
	close(chcdd)
	for out := range chout {
		close(chout)
		return out
	}
	return nil
}

func passKdjFdSingle(fdvs []*model.KDJfdView, prec float64) []*model.KDJfdView {
	off := 0
	for i := 0; i < len(fdvs)-1; i++ {
		f1 := fdvs[i]
		cdd := make([]int, 0, 16)
		pend := make([]*model.KDJfdView, 0, 16)
		for j := i + 1; j < len(fdvs); {
			f2 := fdvs[j]
			d := CalcKdjDevi(f1.K, f1.D, f1.J, f2.K, f2.D, f2.J)
			if d >= prec {
				if j < len(fdvs)-1 {
					fdvs = append(fdvs[:j], fdvs[j+1:]...)
				} else {
					fdvs = fdvs[:j]
				}
				pend = append(pend, f2)
				cdd = append(cdd, j+off)
				off++
			} else {
				j++
			}
		}
		//logr.Debugf("%s-%s-%d found %d similar", fdk.Cytp, fdk.Bysl, fdk.SmpNum, len(pend))
		logr.Debugf("%d #cdd: %+v", i, cdd)
		for _, p := range pend {
			mergeKdjFd(f1, p)
		}
	}
	return fdvs
}

func scanKdjFD(wg *sync.WaitGroup, fdvs []*model.KDJfdView, prec float64, ptags *sync.Map,
	chjob chan int, chcdd chan map[int][]int) {
	defer wg.Done()
	for j := range chjob {
		cdd := make([]int, 0, 16)
		if _, ok := ptags.Load(j); !ok {
			f1 := fdvs[j]
			for i := j + 1; i < len(fdvs); i++ {
				if _, ok := ptags.Load(i); !ok {
					f2 := fdvs[i]
					d := CalcKdjDevi(f1.K, f1.D, f1.J, f2.K, f2.D, f2.J)
					if d >= prec {
						cdd = append(cdd, i)
					}
				}
			}
		}
		chcdd <- map[int][]int{j: cdd}
	}
}

func reduceKdjFD(fdvs []*model.KDJfdView, ptags *sync.Map, chcdd chan map[int][]int,
	chout chan []*model.KDJfdView) {
	i := 0
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 1<<16)
			runtime.Stack(buf, false)
			logr.Errorf("i=%d recover from cleanKdjFD() is not nil \n %+v \n %+v", i,
				r, string(bytes.Trim(buf, "\x00")))
		}
	}()
	wmap := make(map[int][]int)
	for cdd := range chcdd {
		for k, v := range cdd {
			wmap[k] = v
		}
		for list, ok := wmap[i]; ok; list, ok = wmap[i] {
			//logr.Debugf("reducing %d", i)
			if _, ok := ptags.Load(i); !ok {
				f1 := fdvs[i]
				c := 0
				for _, x := range list {
					if _, loaded := ptags.LoadOrStore(x, 0); !loaded {
						mergeKdjFd(f1, fdvs[x])
						c++
					}
				}
				if c > 0 {
					logr.Debugf("reduced %d, #cdd: %d", i, c)
				}
			}
			delete(wmap, i)
			i++
		}
	}
	rfdvs := make([]*model.KDJfdView, 0, 16)
	for i, f := range fdvs {
		if _, ok := ptags.Load(i); !ok {
			rfdvs = append(rfdvs, f)
		}
	}
	chout <- rfdvs
}

func mergeKdjFd(to, fr *model.KDJfdView) {
	tofn := float64(to.FdNum)
	frfn := float64(fr.FdNum)
	deno := tofn + frfn
	for i := 0; i < to.SmpNum; i++ {
		to.K[i] = (to.K[i]*tofn + fr.K[i]*frfn) / deno
		to.D[i] = (to.D[i]*tofn + fr.D[i]*frfn) / deno
		to.J[i] = (to.J[i]*tofn + fr.J[i]*frfn) / deno
	}
	to.FdNum += fr.FdNum
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
		fdv.Fid = fmt.Sprintf("%s", uuid.Must(uuid.NewV1()))
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

func (f *fdKey) ID() string {
	return fmt.Sprintf("%s-%s-%d", f.Cytp, f.Bysl, f.SmpNum)
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
