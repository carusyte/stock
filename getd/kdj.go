package getd

import (
	"fmt"
	"strings"
	"github.com/carusyte/stock/model"
	"math"
	"log"
	"github.com/carusyte/stock/util"
	"time"
	logr "github.com/sirupsen/logrus"
)

func GetKdjHist(code string, tab model.DBTab, retro int) (indcs []*model.Indicator) {
	if retro > 0 {
		sql := fmt.Sprintf("SELECT * FROM %s WHERE	code = ? ORDER BY klid LIMIT ?", tab)
		_, e := dbmap.Select(&indcs, sql, code, retro)
		util.CheckErr(e, "failed to query kdj hist, sql:\n"+sql)
	} else {
		sql := fmt.Sprintf("SELECT * FROM %s WHERE	code = ? ORDER BY klid", tab)
		_, e := dbmap.Select(&indcs, sql, code)
		util.CheckErr(e, "failed to query kdj hist, sql:\n"+sql)
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
	hist := GetKdjHist(code, itab, 0)
	klhist := GetKlineDb(code, ktab, 0, false)
	if len(hist) != len(klhist) {
		log.Panicf("%s %s and %s does not match: %d:%d", code, itab, ktab, len(hist),
			len(klhist))
	}
	if len(hist) < 3 {
		log.Printf("%s insufficient data for sampling.", code)
		return
	}
	indf, kfds := smpKdjBY(code, cytp, hist, klhist, expvr, mxrt, mxhold)
	indfSl, kfdsSl := smpKdjSL(code, cytp, hist, klhist, expvr, mxrt, mxhold)
	indf = append(indf, indfSl...)
	kfds = append(kfds, kfdsSl...)
	saveIndcFt(indf, kfds)
}

// sample KDJ sell point features
func smpKdjSL(code string, cytp model.CYTP, hist []*model.Indicator, klhist []*model.Quote,
	expvr float64, mxrt float64, mxhold int) (indf []*model.IndcFeat, kfds []*model.KDJfd) {
	dt, tm := util.TimeStr()
	kfds = make([]*model.KDJfd, 0, 16)
	indf = make([]*model.IndcFeat, 0, 16)
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
			if pc < nc {
				rt := (lc - nc) / math.Abs(lc) * 100
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
			kft := new(model.IndcFeat)
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
				kfd := new(model.KDJfd)
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
	expvr, mxrt float64, mxhold int) (indf []*model.IndcFeat, kfds []*model.KDJfd) {
	dt, tm := util.TimeStr()
	kfds = make([]*model.KDJfd, 0, 16)
	indf = make([]*model.IndcFeat, 0, 16)
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
			kft := new(model.IndcFeat)
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
				kfd := new(model.KDJfd)
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
			if c == 1 {
				c = int(math.Min(3.0, float64(len(kdjs))))
			}
			return kdjs[len(kdjs)-c:]
		}
		pj := kdjs[i-1].KDJ_J
		pd := kdjs[i-1].KDJ_D
		c++
		if pj == pd {
			if c == 1 {
				c = int(math.Min(3.0, float64(len(kdjs))))
			}
			return kdjs[len(kdjs)-c:]
		}
		if (j < d && pj < pd) || (j > d && pj > pd) {
			continue
		}
		if c == 1 {
			c = int(math.Min(3.0, float64(len(kdjs))))
		}
		return kdjs[len(kdjs)-c:]
	}
	return kdjs;
}

func GetKdjFeatDat(cytp model.CYTP, buy bool) map[string][]*model.KDJfd {
	bysl := "BY"
	if !buy {
		bysl = "SL"
	}
	m := make(map[string][]*model.KDJfd)
	sql, e := dot.Raw("KDJ_FEAT_DAT")
	util.CheckErr(e, "failed to get KDJ_FEAT_DAT sql")
	rows, e := dbmap.Query(fmt.Sprintf(sql, string(cytp)+bysl+"%"))
	util.CheckErr(e, "failed to query kdj feat dat, sql:\n"+sql)
	defer rows.Close()
	for rows.Next() {
		k := new(model.KDJfd)
		f := new(model.IndcFeat)
		k.Feat = f
		var remark *string
		rows.Scan(&k.Code, &f.Indc, &k.Fid, &f.Cytp, &f.Bysl, &f.SmpDate, &f.SmpNum, &f.Mark, &f.Tspan, &f.Mpt, &remark,
			&f.Udate, &f.Utime, &k.Klid, &k.K, &k.D, &k.J, &k.Udate, &k.Utime)
		f.Code = k.Code
		f.Fid = k.Fid
		if remark != nil {
			f.Remarks.Valid = true
			f.Remarks.String = *remark
		}
		mk := k.Code + k.Fid
		if ks, exist := m[mk]; !exist {
			m[mk] = append(make([]*model.KDJfd, 0, 16), k)
		} else {
			m[mk] = append(ks, k)
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	return m
}

func saveIndcFt(feats []*model.IndcFeat, kfds []*model.KDJfd) {
	tran, e := dbmap.Begin()
	util.CheckErr(e, "failed to begin new transaction")
	if len(feats) > 0 && len(kfds) > 0 {
		//delete the last two BY and SL feat
		ftdel := make([]*model.IndcFeat, 0, 2)
		_, e := tran.Select(&ftdel, "select * from indc_feat where code = ? and indc = 'KDJ' and cytp = ? and bysl = "+
			"'BY' order by smp_date desc limit 2", feats[0].Code, feats[0].Cytp)
		if e != nil {
			if "sql: no rows in result set" != e.Error() {
				_, e = tran.Delete(ftdel)
				util.CheckErr(e, feats[0].Code+" failed to delete last 2 KDJ BUY feat")
			} else {
				log.Panicf("%s failed to select last 2 KDJ BUY feat:\n%+v", feats[0].Code, e)
			}
		}
		_, e = tran.Select(&ftdel, "select * from indc_feat where code = ? and indc = 'KDJ' and cytp = ? and bysl = "+
			"'SL' order by smp_date desc limit 2", feats[0].Code, feats[0].Cytp)
		if e != nil {
			if "sql: no rows in result set" != e.Error() {
				_, e = tran.Delete(ftdel)
				util.CheckErr(e, feats[0].Code+" failed to delete last 2 KDJ SELL feat")
			} else {
				log.Panicf("%s failed to select last 2 KDJ SELL feat:\n%+v", feats[0].Code, e)
			}
		}

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
		stmt := fmt.Sprintf("INSERT INTO indc_feat (code,indc,cytp,bysl,smp_date,smp_num,fid,mark,tspan,mpt,remarks,"+
			"udate,utime) VALUES %s on duplicate key update smp_num=values(smp_num),mark=values(mark),tspan=values"+
			"(tspan),mpt=values(mpt),remarks=values(remarks),udate=values(udate),utime=values(utime)",
			strings.Join(valueStrings, ","))
		_, err := tran.Exec(stmt, valueArgs...)
		util.CheckErr(err, code+" failed to bulk insert indc_feat")

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
		stmt = fmt.Sprintf("INSERT INTO kdj_feat_dat (code,fid,klid,k,d,j,"+
			"udate,utime) VALUES %s on duplicate key update k=values(k),d=values(d),"+
			"j=values(j),udate=values(udate),utime=values(utime)",
			strings.Join(valueStrings, ","))
		_, err = tran.Exec(stmt, valueArgs...)
		util.CheckErr(err, code+" failed to bulk insert kdj_feat_dat")

		tran.Commit()
	}
}
