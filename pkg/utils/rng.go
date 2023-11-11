package utils

import (
	"math/rand"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

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

func RandomString(rng *rand.Rand) string {
	b := RandASCIIBytes(rng, 8)
	return string(b)
}

func RandASCIIBytes(rng *rand.Rand, n int) []byte {
	output := make([]byte, n)
	randomness := make([]byte, n)
	_, err := rng.Read(randomness)
	if err != nil {
		panic(err)
	}
	l := len(letterBytes)

	// fill output
	for pos := range output {
		random := uint8(randomness[pos])
		randomPos := random % uint8(l)
		output[pos] = letterBytes[randomPos]
	}

	return output
}
