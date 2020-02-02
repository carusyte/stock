package util

import (
	"bytes"
	"compress/gzip"
	"encoding/gob"
	"io/ioutil"
	"reflect"
)

//EncodeToBytes transforms given object to bytes.
func EncodeToBytes(p interface{}) []byte {
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	e := enc.Encode(p)
	if e != nil {
		log.Fatalf("failed to encode %+v: %+v", p, e)
	}
	return buf.Bytes()
}

//Compress the given interface.
func Compress(p interface{}) []byte {
	var s []byte
	var ok bool
	if s, ok = p.([]byte); !ok {
		s = EncodeToBytes(p)
	}
	zipbuf := bytes.Buffer{}
	zipped := gzip.NewWriter(&zipbuf)
	defer zipped.Close()
	zipped.Write(s)
	return zipbuf.Bytes()
}

//Decompress the given byte array
func Decompress(s []byte) []byte {
	rdr, _ := gzip.NewReader(bytes.NewReader(s))
	defer rdr.Close()
	data, err := ioutil.ReadAll(rdr)
	if err != nil {
		log.Fatal(err)
	}
	return data
}

//DecodeBytes to the given interface.
func DecodeBytes(s []byte, p interface{}) {
	dec := gob.NewDecoder(bytes.NewReader(s))
	var err error
	if reflect.ValueOf(p).Kind() == reflect.Ptr {
		err = dec.Decode(p)
	} else {
		err = dec.Decode(&p)
	}
	if err != nil {
		log.Fatal(err)
	}
}
