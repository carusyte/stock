package util

//CheckErr checks if err is nil, if not, panic with msg and err.
func CheckErr(err error, msg string) bool {
	if err != nil {
		log.Panicln(msg, err)
		return true
	}
	return false
}

//CheckErrNop checks if err is nil, if not, print log with msg and err.
func CheckErrNop(err error, msg string) (e bool) {
	e = err != nil
	if e {
		log.Printf("%s, [%+v]", msg, err)
	}
	return
}
