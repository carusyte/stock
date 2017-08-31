package util

import (
	"github.com/shirou/gopsutil/cpu"
)

func CpuUsage() (idle float64, e error) {
	var ps []float64
	ps, e = cpu.Percent(0, false)
	if e != nil {
		return
	}
	return ps[0], e
}
