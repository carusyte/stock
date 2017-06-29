package score

import (
	"testing"
	"log"
)

func BenchmarkCombineResults(b *testing.B) {
	r1 := new(HiD).Get(nil, 5, false)
	r1.Weight = 0.5
	r2 := new(BlueChip).Get(nil, 5, false)
	r2.Weight = 0.5
	log.Printf("%+v", Combine(r1, r2))
}
