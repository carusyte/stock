package getd

import "github.com/carusyte/stock/conf"

//Cleanup cleans up any resources allocated for the program, including processes running outside of this one.
func Cleanup() {
	switch conf.Args.Datasource.Kline {
	case conf.THS:
		cleanupTHS()
	}
}
