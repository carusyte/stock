package score

import (
	"testing"
	"fmt"
)

func TestGet(t *testing.T) {
	fmt.Println(new(KdjSt).Get([]string{"600828"}, -1, false))
}
