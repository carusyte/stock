package score

import (
	"fmt"
	"reflect"
	"github.com/pkg/errors"
	"math"
)

// Assess scores based on KdjV scores against its statistical data.
type KdjSt struct {
	Code                               string
	Name                               string
	Len                                string
	Kdjv, Sl, Sh, Bl, Bh, Smean, Bmean float64
}

func (k *KdjSt) GetFieldStr(name string) string {
	switch name {
	case "LEN":
		return k.Len
	case "KDJV":
		return fmt.Sprintf("%.2f", k.Kdjv)
	case "SL":
		return fmt.Sprintf("%.2f", k.Sl)
	case "SH":
		return fmt.Sprintf("%.2f", k.Sh)
	case "BL":
		return fmt.Sprintf("%.2f", k.Bl)
	case "BH":
		return fmt.Sprintf("%.2f", k.Bh)
	case "BMEAN":
		return fmt.Sprintf("%.2f", k.Bmean)
	case "SMEAN":
		return fmt.Sprintf("%.2f", k.Smean)
	default:
		r := reflect.ValueOf(k)
		f := reflect.Indirect(r).FieldByName(name)
		if !f.IsValid() {
			panic(errors.New("undefined field for KDJSt: " + name))
		}
		return fmt.Sprintf("%+v", f.Interface())
	}
}

var (
	kdjv *KdjV = new(KdjV)
)

func (k *KdjSt) Get(stock []string, limit int, ranked bool) (r *Result) {
	r = new(Result)
	r.PfIds = append(r.PfIds, k.Id())
	vr := kdjv.Get(stock, -1, false)
	for _, vri := range vr.Items {
		v := vri.Profiles[kdjv.Id()].FieldHolder.(*KdjV)
		item := new(Item)
		item.Code = vri.Code
		item.Name = vri.Name

		kst := new(KdjSt)
		kst.Code = item.Code
		kst.Name = item.Name
		kst.Bh = v.Bh
		kst.Bl = v.Bl
		kst.Sl = v.Sl
		kst.Sh = v.Sh
		kst.Len = v.Len
		kst.Smean = v.Smean
		kst.Bmean = v.Bmean
		kst.Kdjv = vri.Profiles[kdjv.Id()].Score
		item.Profiles = make(map[string]*Profile)
		ip := new(Profile)
		item.Profiles[kst.Id()] = ip
		ip.FieldHolder = kst
		ip.Score = kdjstScore(kst)
		item.Score += ip.Score

		r.AddItem(item)
	}
	r.SetFields(k.Id(), k.Fields()...)
	if ranked {
		r.Sort()
	}
	r.Shrink(limit)
	return
}

// The greater the Bmean is than the Smean, the higher the score, this factor has overall influence
// The greater the kdjv score is than the max(Bl,Sl), the higher the score
// Get max score if kdjv score >= max(Bh, Sh) and Bmean/Smean - 1 >= 0.3
// Get 0 if kdjv score is no greater than max(Bl,Sl) or Bmean <= Smean
func kdjstScore(kst *KdjSt) (s float64) {
	bsln := math.Max(kst.Bl, kst.Sl) //baseline
	if kst.Kdjv <= bsln || kst.Bmean <= kst.Smean {
		return 0
	} else {
		mr := 0.0
		if kst.Smean == 0 && kst.Bmean > 0 {
			mr = 0.3
		} else {
			mr = (kst.Bmean - kst.Smean) / kst.Smean
		}
		high := math.Max(kst.Bh, kst.Sh)
		if mr >= 0.3 && kst.Kdjv >= high {
			return 100
		}
		mod := 2.0/5.0*math.Pow(10.0/3.0*mr, math.E) + 0.6 // mod == 1 when mr == 0.3; mod == 0.6 when mr -> +0
		mod = math.Min(1, mod)
		if kst.Kdjv > kst.Bmean {
			x := kst.Kdjv - kst.Bmean
			h := high - kst.Bmean
			s = 60 + 40*math.Pow(h, -1/math.E)*math.Pow(x, 1/math.E)
		} else {
			x := kst.Kdjv - bsln
			h := kst.Bmean - bsln
			s = 60 * math.Pow(x/h, math.E)
		}
		s *= mod
	}
	s = math.Min(100, s)
	return
}

func (k *KdjSt) Geta() (r *Result) {
	return k.Get(nil, -1, false)
}

func (k *KdjSt) Id() string {
	return "KDJSt"
}

func (k *KdjSt) Fields() []string {
	return []string{"LEN", "KDJV", "SMEAN", "BMEAN", "SL", "SH", "BL", "BH"}
}

func (k *KdjSt) Description() string {
	panic("implement me")
}
