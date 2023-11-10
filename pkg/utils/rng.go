package utils

import "math/rand"

func RandomIntRange(rng *rand.Rand, start, end int) int {
	return start + rng.Intn(end-start)
}

func RandomIntSliceRange(rng *rand.Rand, min, max, n int) []int {
	res := make([]int, n)
	for i := 0; i < n; i++ {
		res[i] = RandomIntRange(rng, min, max)
	}

	return res
}
