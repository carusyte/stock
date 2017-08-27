package util

import (
	"testing"
)

func TestWordCount(t *testing.T) {
	svrAddr := "115.159.237.46:45321"
	service := "GTest.WordCount"
	var rep bool
	e := RpcCall(svrAddr, service, "", &rep)
	CheckErr(e, "failed word count")
}
