package score

import (
	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/model"
	"log"
	"encoding/json"
	"fmt"
	"bytes"
	"github.com/olekukonko/tablewriter"
	"sort"
)

const JOB_CAPACITY = global.JOB_CAPACITY
const MAX_CONCURRENCY = global.MAX_CONCURRENCY

var (
	dbmap = global.Dbmap
	dot   = global.Dot
)

type Profile struct {
	//Score for this aspect
	Score float64
	//Weight in total score
	Weight float64
	//Maintain field names in order
	FieldNames []string
	//Reminds
	Comments []string
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

func (p * Profile) Cmt(c... string){
	p.Comments = append(p.Comments, c...)
}

func (p *Profile) Cmtf(f string, i... interface{}){
	p.Cmt(fmt.Sprintf(f,i...))
}

type Item struct {
	//Security Code
	Code string
	//Security Name
	Name string
	//Total score
	Score float64
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

func (p *Profile) AddField(name string) {
	p.FieldNames = append(p.FieldNames, name)
}

func (p *Profile) AddFieldAt(i int, name string) {
	if i > len(p.FieldNames) {
		log.Panicf("can't add field at index > %d", len(p.FieldNames))
	} else if i == len(p.FieldNames) {
		p.AddField(name)
	} else {
		p.FieldNames = append(p.FieldNames, "")
		copy(p.FieldNames[i+1:], p.FieldNames[i:])
		p.FieldNames[i] = name
	}
}

type Result struct {
	Items      []*Item
	ProfileIds []string
}

func (r *Result) Sort() (rr *Result){
	rr = r
	sort.Slice(r.Items, func(i,j int) bool{
		return r.Items[i].Score > r.Items[j].Score
	})
	return
}

func (r *Result) String() string {
	if len(r.Items) == 0 {
		return ""
	}

	var bytes bytes.Buffer
	table := tablewriter.NewWriter(&bytes)
	table.SetRowLine(true)

	var hd []string
	hd = append(hd, "Rank")
	hd = append(hd, "Code")
	hd = append(hd, "Name")
	hd = append(hd, "Score")
	for _, a := range r.Items[0].Profiles {
		for _, fn := range a.FieldNames {
			hd = append(hd, fn)
		}
	}
	hd = append(hd, "Comments")

	table.SetHeader(hd);
	data := make([][]string, len(r.Items))
	for i, itm := range r.Items {
		data[i] = make([]string, len(hd))
		data[i][0] = fmt.Sprintf("%d",i+1)
		data[i][1] = itm.Code
		data[i][2] = itm.Name
		data[i][3] = fmt.Sprintf("%.2f", itm.Score)
		idx := 4
		cmt := ""
		for _, p := range itm.Profiles {
			for _, fn := range p.FieldNames{
				data[i][idx] = p.FieldHolder.GetFieldStr(fn)
				idx++
			}
			if len(p.Comments) == 1{
				cmt = p.Comments[0]
			}else if len(p.Comments) > 1{
				for i,c := range p.Comments{
					cmt += fmt.Sprintf("%d.%s",i+1,c)
					if i < len(p.Comments)-1{
						cmt += "\n"
					}
				}
			}
		}
		data[i][idx] = cmt
	}
	table.AppendBulk(data)
	table.Render()

	return bytes.String()
}

type Scorer interface {
	Get(stock []*model.Stock) (r *Result)
	Id() string
	Description() string
}

type FieldHolder interface {
	GetFieldStr(name string) string
}
