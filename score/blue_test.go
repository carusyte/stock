package score

import (
	"testing"
	"fmt"
)

func BenchmarkBlueGet(t *testing.B) {
	blue := new(BlueChip)
	r := blue.Get(nil, 500, true)
	fmt.Printf("%v", r)
}

func TestDiv(t *testing.T){
	a := 10.
	a /= 5.
	fmt.Printf("================== %f", a)
}
