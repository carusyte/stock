package advisor

import (
	"bytes"
	"github.com/carusyte/stock/db"
	"github.com/carusyte/stock/util"
	"github.com/gchaincl/dotsql"
	"github.com/olekukonko/tablewriter"
	"gopkg.in/gorp.v2"
	"reflect"
)

type advisor struct {
	dbMap  *gorp.DbMap
	dotsql *dotsql.DotSql
}

func New() *advisor {
	dot, err := dotsql.LoadFromFile("/Users/jx/ProgramData/go/src/github.com/carusyte/stock/ask/sql.txt")
	util.CheckErr(err, "failed to init dotsql")
	a := &advisor{db.Get(false, false), dot}
	return a
}

type Table struct {
	Head []Head
	Data []interface{}
}

type Head struct {
	Name string
	Len  int
}

func (t *Table) Count() int {
	return len(t.Data)
}

func (t *Table) String() string {
	var bytes bytes.Buffer
	table := tablewriter.NewWriter(&bytes)
	table.SetRowLine(true)

	var hd []string
	for _, e := range t.Head {
		hd = append(hd, e.Name)
	}
	table.SetHeader(hd);
	data := make([][]string, t.Count())
	for i, e := range t.Data {
		data[i] = make([]string, len(hd))
		v := reflect.TypeOf(e).Elem()
		for j := 0; j < v.NumField(); j++ {
			data[i][j] = util.FieldValueStr(e, j)
		}
	}
	table.AppendBulk(data)
	table.Render()

	return bytes.String()
}
