package util

import "github.com/sirupsen/logrus"

//CheckErr checks if err is nil, if not, panic with msg and err.
func CheckErr(err error, msg string) bool {
	if err != nil {
		logrus.Panicln(msg, err)
		return true
	}
	return false
}

//CheckErrNop checks if err is nil, if not, print log with msg and err.
func CheckErrNop(err error, msg string) (e bool) {
	e = err != nil
	if e {
		logrus.Printf("%s, [%+v]", msg, err)
	}
	return
}
