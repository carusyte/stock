package score

import (
	"github.com/carusyte/stock/model"
	"github.com/carusyte/stock/util"
	"fmt"
	"reflect"
	"github.com/pkg/errors"
	"math"
	"database/sql"
)

// Search for stocks with excellent financial report.
// Medium to long term model, mainly focusing on yearly financial reports.
// · Low latest P/E, normally below 50
// · Growing EPS each year and quarter, up to 3 years
// · Low latest P/U
// · Growing UDPPS each year and quarter, up to 3 years
// Get warnings/penalties if:
// · High latest DAR
// · High average DAR, up to 5 years
type BlueChip struct {
	model.Finance
	Name string
	Pe   sql.NullFloat64 `db:"pe"`
	Pu   sql.NullFloat64 `db:"pu"`
	Po   sql.NullFloat64 `db:"po"`
}

// The assessment metric diverts, some of them are somewhat negative correlated.
const (
	BLUE_HIST_SIZE int     = 15
	SCORE_GEPS     float64 = 30
	SCORE_GUDPPS           = 30
	SCORE_PE               = 20
	SCORE_PU               = 20
	PENALTY_DAR            = 20
	PE_THRESHOLD           = 50
)

func (b *BlueChip) Get(s []*model.Stock, limit int) (r *Result) {
	//TODO implement this scorer
	r = &Result{}
	r.ProfileIds = append(r.ProfileIds, b.Id())
	var blus []*BlueChip
	if s == nil || len(s) == 0 {
		sql, e := dot.Raw("BLUE")
		util.CheckErr(e, "failed to get BLUE sql")
		_, e = dbmap.Select(&blus, sql, PE_THRESHOLD)
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

		hist := getFinHist(ib.Code, BLUE_HIST_SIZE)

		ip.Score += sEps(ib, hist)

		item.Score += ip.Score
	}
	r.Sort()
	r.Shrink(limit)
	return
}

func getFinHist(code string, size int) (fins []*model.Finance) {
	sql, e := dot.Raw("BLUE_HIST")
	util.CheckErr(e, "failed to get BLUE_HIST sql")
	_, e = dbmap.Select(&fins, sql, code, size)
	util.CheckErr(e, "failed to query BLUE_HIST for "+code)
	return
}

// Score by assessing EPS
// P/E: Get max score if 0 < P/E <= 5, get 0 if P/E >= 40
// EPS GR: Get max score if EPS_YOY is all positive and avg EPS_YOY >= 15, get 0 if avg negative growth rate is <= -15%
func sEps(b *BlueChip, hist []*model.Finance) (s float64) {
	ZERO_PE := 40.0
	MAX_PE := 5.0
	// score latest P/E
	if b.Eps.Float64 < 0 || b.Eps.Float64 >= ZERO_PE {
		s = 0
	} else {
		s = SCORE_PE * math.Min(1, math.Pow((ZERO_PE-b.Eps.Float64)/(ZERO_PE-MAX_PE), 0.5))
	}
	//TODO make it
	// score average P/E

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
