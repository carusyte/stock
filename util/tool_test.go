package util

import (
	"testing"
	"fmt"
	"github.com/sirupsen/logrus"
	"sort"
)

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

func TestDiffStrs(t *testing.T) {
	s1 := []string{"123", "abc", "du8", "0c2", "190","a3c"}
	//s2 := []string{"123", "abc", "du8", "0c2"}
	//same, diff := DiffStrs(s1, s2)
	//fmt.Printf("%v, %+v\n", same, diff)
	s3 := []string{"123", "a3c", "du8", "443", "0c2"}
	fmt.Printf("S1: %+v\n",s1)
	fmt.Printf("S3: %+v\n",s3)
	same, dif1, dif2 := DiffStrings(s1, s3)
	fmt.Printf("%v, %+v, %+v\n", same, dif1, dif2)
}

func TestSearchStrings(t *testing.T) {
	s1 := []string{"123", "abc", "du8", "0c2"}
	sort.Strings(s1)
	fmt.Printf("%d", sort.SearchStrings(s1, "c99"))

}
