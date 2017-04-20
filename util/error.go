package util

import (
	"log"
)

func CheckErr(err error, msg string) bool {
	if err != nil {
		log.Panicln(msg, err)
		return true
	}
	return false
}

func CheckErrNop(err error, msg string) (e bool){
	e = err != nil
	if e {
		log.Printf("%s, [%+v]",msg,err)
	}
	return
}
