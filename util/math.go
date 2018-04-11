package util

import (
	"log"
	"math"
)

// calculates standard score
//func StdScore(in []float64, )

//LogReturn calculates log return based on previous value, current value and bias.
// bias is only used either previous or current value is not greater than 0.
func LogReturn(prev, cur, bias float64) float64 {
	if bias <= 0 {
		log.Panicf("bias %f must be greater than 0.", bias)
	}
	if prev == 0 {
		return 0
		// if prev == 0 && cur == 0 {
		// 	return 0
		// } else if prev == 0 {
		// 	if cur > 0 {
		// 		return math.Log((cur + bias) / bias)
		// 	}
		// 	return math.Log(bias / (math.Abs(cur) + bias))
	} else if cur == 0 {
		if prev > 0 {
			return math.Log(bias / (prev + bias))
		}
		return math.Log((math.Abs(prev) + bias) / bias)
	} else if prev < 0 && cur < 0 {
		return math.Log(math.Abs(prev) / math.Abs(cur))
	} else if prev < 0 {
		return math.Log((cur + math.Abs(prev) + bias) / bias)
	} else if cur < 0 {
		return math.Log(bias / (prev + math.Abs(cur) + bias))
	}
	return math.Log(cur / prev)
}
