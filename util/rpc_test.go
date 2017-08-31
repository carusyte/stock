package util

import (
	"testing"
)

func TestWordCount(t *testing.T) {
	service := "GTest.WordCount"
	var rep bool
	e := RpcCall(service, "", &rep, 3)
	CheckErr(e, "failed word count")
}
