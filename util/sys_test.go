package util

import (
	"log"
	"testing"
)

func TestWriteJSONFile(t *testing.T) {
	p := &struct {
		Code     string        `json:"code"`
		Klid     int           `json:"klid"`
		Refs     []string      `json:"refs"`
		Features [][][]float64 `json:"features"`
		SeqLens  []int         `json:"seqlens"`
	}{
		"this is a string",
		5428,
		[]string{"000002", "000003", "000004"},
		[][][]float64{
			[][]float64{
				[]float64{1, 2, 3, 4, 5},
				[]float64{2, 3, 4, 5, 6},
			},
			[][]float64{
				[]float64{5, 4, 3, 2, 1},
				[]float64{9, 8, 7, 6, 5},
			},
		},
		[]int{7, 5, 4, 3, 2, 1, 0},
	}
	path := "/Volumes/WD-1TB/wcc_infer/wic_test"
	c := true
	fp, e := WriteJSONFile(p, path, c)
	if e != nil {
		panic(e)
	} else {
		log.Printf("final path: %s", fp)
	}
}
