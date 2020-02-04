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
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)
	e := enc.Encode(p)
	if e != nil {
		log.Panicf("failed to encode %+v: %+v", p, e)
	}
	return buf.Bytes()
}

//Compress the given interface.
func Compress(p interface{}) []byte {
	zipbuf := &bytes.Buffer{}
	zipped := gzip.NewWriter(zipbuf)
	defer zipped.Close()
	if s, ok := p.([]byte); ok {
		if wb, e := zipped.Write(s); e != nil {
			log.Panicf("failed to compress %d bytes: %+v", len(s), e)
		} else {
			log.Debugf("gzip has written %d bytes", wb)
		}
	} else {
		enc := gob.NewEncoder(zipped)
		if e := enc.Encode(p); e != nil {
			log.Panicf("failed to encode %+v: %+v", p, e)
		}
	}
	return zipbuf.Bytes()
}

//Decompress the given byte array
func Decompress(s []byte) []byte {
	rdr, e := gzip.NewReader(bytes.NewReader(s))
	if e != nil {
		log.Panicf("failed to create gzip reader for decompression: %+v", e)
	}
	defer rdr.Close()
	data, e := ioutil.ReadAll(rdr)
	if e != nil {
		log.Panicf("failed to decompress: %+v", e)
	} else {
		log.Debugf("read gzipped bytes: %d", len(data))
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
		log.Panicf("failed to decode bytes to %+v, %+v", reflect.TypeOf(p), err)
	}
}

//DecodeCompressed bytes to the given interface.
func DecodeCompressed(s []byte, p interface{}) {
	rdr, e := gzip.NewReader(bytes.NewReader(s))
	if e != nil {
		log.Panicf("failed to create gzip reader for decompression: %+v", e)
	}
	defer rdr.Close()
	dec := gob.NewDecoder(rdr)
	if reflect.ValueOf(p).Kind() == reflect.Ptr {
		e = dec.Decode(p)
	} else {
		e = dec.Decode(&p)
	}
	if e != nil {
		log.Panicf("failed to decode bytes to %+v, %+v", reflect.TypeOf(p), e)
	}
}
