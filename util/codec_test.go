package util

import (
	"database/sql"
	"testing"
	"unsafe"
)

func Test_Codec(t *testing.T) {
	type st struct {
		A string
		B int
		C float64
		D sql.NullFloat64
	}
	s1 := st{
		A: "A",
		B: 2,
		C: 3.4,
		D: sql.NullFloat64{
			Float64: 5.1,
			Valid:   true,
		},
	}
	edat := EncodeToBytes(s1)
	log.Debugf("encoded length: %d", len(edat))
	s2 := new(st)
	DecodeBytes(edat, s2)
	log.Debugf("decoded: %+v", s2)
}

func Test_ZIP(t *testing.T) {
	type st struct {
		A string
		B int
		C float64
		D sql.NullFloat64
	}
	s1 := &st{
		A: "A",
		B: 2,
		C: 3.4,
		D: sql.NullFloat64{
			Float64: 5.1,
			Valid:   true,
		},
	}
	log.Debugf("original object size: %d, %+v", unsafe.Sizeof(*s1), s1)
	cdat := Compress(s1)
	log.Debugf("compressed length: %d", len(cdat))
	s2 := &st{}
	DecodeCompressed(cdat, s2)
	log.Debugf("decompressed object: %+v", s2)
}
