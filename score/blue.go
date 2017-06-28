package score

import (
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"fmt"
	"reflect"
	"github.com/pkg/errors"
	"math"
)

// Search for stocks with excellent financial report.
// Medium to long term model, mainly focusing on yearly financial reports.
// · Low latest P/E, normally below 30
// · High average yearly EPS of up to 5 years
// · Growing EPS each year of up to 5 years
// · Growing latest ROE
// · High average ROE of up to 5 years
// · Growing ROE each year of up to 5 years
// · Low latest P/U
// · Growing UDPPS each term, counting 5 terms
// · Low latst P/O
// · High average OCFPS each year of up to 5 years
// Get warnings/penalties if:
// · High latest DAR
// · High average DAR of up to 5 years
type BlueChip struct {
	model.Finance
	Name string
	Pe   float64 `db:"pe"`
	Pu   float64 `db:"pu"`
	Po   float64 `db:"po"`
}

// The assessment metric diverts, some of them are somewhat negative correlated.
const (
	SCORE_PE     float64 = 15
	SCORE_AEPS           = 7
	SCORE_GEPS           = 10
	SCORE_ROE            = 14
	SCORE_AROE           = 5
	SCORE_GROE           = 7
	SCORE_PU             = 15
	SCORE_GUDPPS         = 7
	SCORE_PO             = 15
	SCORE_AOCFPS         = 5
	PENALTY_DAR          = 30
	SURVIVOR     int     = 50
)

func (b *BlueChip) Get(s []*model.Stock) (r *Result) {
	//TODO implement this scorer
	r = &Result{}
	r.ProfileIds = append(r.ProfileIds, b.Id())
	var blus []*BlueChip
	if s == nil || len(s) == 0 {
		sql, e := dot.Raw("BLUE")
		util.CheckErr(e, "failed to get BLUE sql")
		_, e = dbmap.Select(&blus, sql)
		util.CheckErr(e, "failed to query database, sql:\n"+sql)
	} else {
		//TODO select by specified stock codes
	}

	r.Items = make([]*Item, len(blus))
	for i, ib := range blus {
		item := new(Item)
		r.Items[i] = item
		item.Code = ib.Code
		item.Name = ib.Name
		item.Profiles = make(map[string]*Profile)
		ip := new(Profile)
		item.Profiles[b.Id()] = ip
		ip.Weight = 1
		ip.FieldHolder = ib
		ip.AddField("Latest Report")
		ip.Score += sEps(ib)

		item.Score += ip.Score
	}
	r.Sort()
	return
}

//Score by assessing EPS
func sEps(b *BlueChip) (s float64) {
	if b.Eps.Float64 < 0 {
		s = 0
	} else {
		s = SCORE_PE * math.Min(1, b.Eps.Float64)
	}
	return
}

func (*BlueChip) Id() string {
	return "BLUE"
}

func (b *BlueChip) GetFieldStr(name string) string {
	//TODO parse field in string
	switch name {
	case "Latest Report":
		return b.Year
	default:
		r := reflect.ValueOf(b)
		f := reflect.Indirect(r).FieldByName(name)
		if !f.IsValid() {
			panic(errors.New("undefined field for BLUE: " + name))
		}
		return fmt.Sprintf("%+v", f.Interface())
	}
}

func (b *BlueChip) Description() string {
	//TODO write some description
	return ""
}
