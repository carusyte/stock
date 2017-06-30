package score

import (
	"testing"
	"log"
)

func BenchmarkCombineResults(b *testing.B) {
	r1 := new(HiD).Geta()
	r1.Weight = 0.5
	r2 := new(BlueChip).Geta()
	r2.Weight = 0.5
	log.Printf("\n%+v", Combine(r1, r2).Sort())
}
