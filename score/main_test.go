package score

import (
	"testing"
	"log"
)

func TestCombineResults(t *testing.T) {
	r1 := new(HiD).Geta()
	r1.Weight = 0.5
	r2 := new(BlueChip).Geta()
	r2.Weight = 0.5
	log.Printf("\n%+v", Combine(r1, r2).Sort())
}

func TestKdjV(t *testing.T) {
	r1 := new(KdjV).Get([]string{"600104"}, -1, false)
	log.Printf("\n%+v", r1)
}
