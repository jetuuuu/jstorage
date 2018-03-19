package utils

import "math"

func Round(f float64) int {
	return int(math.Floor(f + .5))
}
