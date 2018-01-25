package sampler

import (
	"log"
	"testing"
)

func TestSingle(t *testing.T) {
	code := "600104"
	kpts, e := KeyPoints(code, -1, 120, nil)
	if e != nil {
		log.Println(e)
		t.FailNow()
	}
	e = SaveKpts(kpts...)
	if e != nil {
		log.Println(e)
		t.FailNow()
	}
	log.Printf("%s kpts data saved. size=%d", code, len(kpts))
}
