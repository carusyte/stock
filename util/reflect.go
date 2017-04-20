package util

import (
	"reflect"
	"strconv"
)

func FieldValueStr(i interface{}, idx int) string {
	v := reflect.ValueOf(i)
	f := reflect.Indirect(v).Field(idx)
	switch f.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.Itoa(int(f.Int()))
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(f.Float(), 'f', -1, 64)
	default:
		return f.String()
	}
}
