package score

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"sort"

	"github.com/carusyte/stock/global"
	"github.com/olekukonko/tablewriter"
)

const JOB_CAPACITY = global.JOB_CAPACITY

//TODO TRY 30/60 min ENE
var (
	dbmap = global.Dbmap
	dot   = global.Dot
)

type Profile struct {
	//Score for this aspect
	Score float64
	//Field holder handy to get formatted field value
	FieldHolder FieldHolder
}

func (p *Profile) String() string {
	j, e := json.Marshal(p)
	if e != nil {
		fmt.Println(e)
	}
	return fmt.Sprintf("%v", string(j))
}

func (it *Item) Cmt(c ...string) {
	it.Comments = append(it.Comments, c...)
}

func (it *Item) Cmtf(f string, i ...interface{}) {
	it.Cmt(fmt.Sprintf(f, i...))
}

type Item struct {
	//Security Code
	Code string
	//Security Name
	Name string
	//Industry
	Industry string
	//Total score
	Score float64
	//Reminds
	Comments []string
	//Score evaluation aspect
	Profiles map[string]*Profile
}

func (i *Item) String() string {
	j, e := json.Marshal(i)
	if e != nil {
		fmt.Println(e)
	}
	return fmt.Sprintf("%v", string(j))
}

type Result struct {
	Items []*Item
	//Code - Item map
	itMap map[string]*Item
	PfIds []string
	//Profile weights in total score
	PfWts []float64
	//Weight in parent result
	Weight float64
	Fields map[string][]string
}

func (r *Result) Stocks() []string {
	s := make([]string, len(r.Items))
	for i := range s {
		s[i] = r.Items[i].Code
	}
	return s
}

func (r *Result) AddItem(items ...*Item) {
	if r.Items == nil {
		r.Items = make([]*Item, len(items))
		for i := range items {
			r.Items[i] = items[i]
		}
	} else {
		r.Items = append(r.Items, items...)
	}
	if r.itMap == nil {
		r.itMap = make(map[string]*Item)
	}
	for i := range items {
		r.itMap[items[i].Code] = items[i]
	}
}

func (r *Result) Sort() (rr *Result) {
	rr = r
	sort.Slice(r.Items, func(i, j int) bool {
		return r.Items[i].Score > r.Items[j].Score
	})
	return
}

func (r *Result) Shrink(num int) *Result {
	if 0 <= num && num < len(r.Items) {
		r.Items = r.Items[:num]
	}
	return r
}

func (r *Result) SetFields(id string, fields ...string) {
	if r.Fields == nil {
		r.Fields = make(map[string][]string)
	}
	r.Fields[id] = fields
}

func (r *Result) String() string {
	if len(r.Items) == 0 {
		return ""
	}

	var bytes bytes.Buffer
	table := tablewriter.NewWriter(&bytes)
	table.SetRowLine(true)

	hd := make([]string, 0, 16)
	hd = append(hd, "Rank")
	hd = append(hd, "Code")
	hd = append(hd, "Name")
	hd = append(hd, "Industry")
	hd = append(hd, "Score")
	fns := []string{}
	fidx := map[string]int{}
	pfidx := map[string]int{}
	idx := 5 + len(r.PfIds)
	for _, pfid := range r.PfIds {
		pfidx[pfid] = len(hd)
		hd = append(hd, pfid)
		for _, fn := range r.Fields[pfid] {
			fns = append(fns, fn)
			fidx[pfid+"."+fn] = idx
			idx++
		}
	}
	hd = append(hd, fns...)
	hd = append(hd, "Comments")

	table.SetHeader(hd)
	data := make([][]string, len(r.Items))
	for i, itm := range r.Items {
		data[i] = make([]string, len(hd))
		data[i][0] = fmt.Sprintf("%d", i+1)
		data[i][1] = itm.Code
		data[i][2] = itm.Name
		data[i][3] = itm.Industry
		data[i][4] = fmt.Sprintf("%.2f", itm.Score)
		for pfid, p := range itm.Profiles {
			data[i][pfidx[pfid]] = fmt.Sprintf("%.2f", p.Score)
			for _, fn := range r.Fields[pfid] {
				data[i][fidx[pfid+"."+fn]] = p.FieldHolder.GetFieldStr(fn)
			}
		}
		cmt := ""
		if len(itm.Comments) == 1 {
			cmt = itm.Comments[0]
		} else if len(itm.Comments) > 1 {
			for i, c := range itm.Comments {
				cmt += fmt.Sprintf("%d.%s", i+1, c)
				if i < len(itm.Comments)-1 {
					cmt += "\n"
				}
			}
		}
		data[i][len(data[i])-1] = cmt
	}
	table.AppendBulk(data)
	table.Render()

	return bytes.String()
}

type Scorer interface {
	Get(stock []string, limit int, ranked bool) (r *Result)
	Geta() (r *Result)
	Id() string
	Fields() []string
	Description() string
}

type FieldHolder interface {
	GetFieldStr(name string) string
}

func Combine(rs ...*Result) (fr *Result) {
	fr = &Result{}
	for i, r := range rs {
		fr.PfIds = append(fr.PfIds, r.PfIds...)
		fr.PfWts = append(fr.PfWts, r.Weight)
		fr.Weight += r.Weight
		for pfid := range r.Fields {
			if _, exists := fr.Fields[pfid]; exists {
				log.Panicf("unable to combine identical profile: %s", pfid)
			} else {
				fr.SetFields(pfid, r.Fields[pfid]...)
			}
		}
		if i == 0 {
			fr.AddItem(r.Items...)
			for _, it := range fr.Items {
				it.Score *= r.Weight
			}
		} else {
			for _, it := range r.Items {
				if mi, ok := fr.itMap[it.Code]; ok {
					mi.Score += it.Score * r.Weight
					for k := range it.Profiles {
						if _, exists := mi.Profiles[k]; exists {
							log.Panicf("profile [%s] already exists: %+v", k, mi.Profiles[k])
						} else {
							mi.Profiles[k] = it.Profiles[k]
						}
					}
					if mi.Industry == "" && it.Industry != "" {
						mi.Industry = it.Industry
					}
				} else {
					fr.AddItem(it)
					it.Score *= r.Weight
				}
			}
		}
	}
	return
}

type wsPair struct {
	w, s float64
}

//WtScore weighted score recorder
type WtScore map[string]*wsPair

//Add adds score and weight using the specified item id.
// if an item with the provided id already exists, its
// weight and score will be replaced.
func (ws WtScore) Add(id string, score, weight float64) {
	if wsp, ok := ws[id]; ok {
		wsp.w += weight - wsp.w
		wsp.s = score
	} else {
		ws[id] = &wsPair{w: weight, s: score}
	}
}

//Sum sums the resulting weighted total score by calculating
// the provided weight and sub-score item pairs.
func (ws WtScore) Sum() float64 {
	n, d := .0, .0
	for _, wsp := range ws {
		n += wsp.w * wsp.s
		if wsp.w > 0 {
			d += wsp.w
		}
	}
	return n / d
}
