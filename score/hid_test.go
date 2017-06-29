package score

import (
	"testing"
	"fmt"
)

func TestHiD_Get(t *testing.T) {
	hid := new(HiD)
	r := hid.Get(nil, 50, true)
	fmt.Printf("%v", r)
}
