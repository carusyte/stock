package score

import (
	"github.com/carusyte/stock/global"
	"github.com/carusyte/stock/model"
	"log"
	"encoding/json"
	"fmt"
	"bytes"
	"github.com/olekukonko/tablewriter"
)

const JOB_CAPACITY = global.JOB_CAPACITY
const MAX_CONCURRENCY = global.MAX_CONCURRENCY

var (
	dbmap = global.Dbmap
	dot   = global.Dot
)

//TODO implement scoring

type Aspect struct {
	//Score for this aspect
	Score float64
	//Weight in total score
	Weight float64
	//Maintain field names in order
	FieldNames []string
	//Reminds
	Comment string
	//Field holder handy to get formatted field value
	FieldHolder FieldHolder
}

func (a *Aspect) String() string {
	j, e := json.Marshal(a)
	if e != nil {
		fmt.Println(e)
	}
	return fmt.Sprintf("%v", string(j))
}

type Item struct {
	//Security Code
	Code string
	//Security Name
	Name string
	//Total score
	Score float64
	//Score evaluation aspect
	Aspects map[string]*Aspect
}

func (i *Item) String() string {
	j, e := json.Marshal(i)
	if e != nil {
		fmt.Println(e)
	}
	return fmt.Sprintf("%v", string(j))
}

func (a *Aspect) AddField(name string) {
	a.FieldNames = append(a.FieldNames, name)
}

func (a *Aspect) AddFieldAt(i int, name string, value interface{}) {
	if i > len(a.FieldNames) {
		log.Panicf("can't add field at index > %d", len(a.FieldNames))
	} else if i == len(a.FieldNames) {
		a.AddField(name)
	} else {
		a.FieldNames = append(a.FieldNames, "")
		copy(a.FieldNames[i+1:], a.FieldNames[i:])
		a.FieldNames[i] = name
	}
}

type Result struct {
	Items     []*Item
	AspectIds []string
}

func (r *Result) String() string {
	if len(r.Items) == 0 {
		return ""
	}

	var bytes bytes.Buffer
	table := tablewriter.NewWriter(&bytes)
	table.SetRowLine(true)

	var hd []string
	hd = append(hd, "Code")
	hd = append(hd, "Name")
	hd = append(hd, "Score")
	for _, a := range r.Items[0].Aspects {
		for _, fn := range a.FieldNames {
			hd = append(hd, fn)
		}
	}

	table.SetHeader(hd);
	data := make([][]string, len(r.Items))
	for i, itm := range r.Items {
		data[i] = make([]string, len(hd))
		data[i][0] = itm.Code
		data[i][1] = itm.Name
		data[i][2] = fmt.Sprintf("%.2f", itm.Score)
		idx := 3
		for _, a := range itm.Aspects{
			for _, fn := range a.FieldNames{
				data[i][idx] = a.FieldHolder.GetFieldStr(fn)
				idx++
			}
		}
	}
	table.AppendBulk(data)
	table.Render()

	return bytes.String()
}

type Scorer interface {
	Get(stock []*model.Stock) (r Result)
	Id() string
	Description() string
}

type FieldHolder interface {
	GetFieldStr(name string) string
}
