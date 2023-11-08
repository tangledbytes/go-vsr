package array

import (
	"context"
	"log/slog"
	"math/rand"
)

type Rand[T any] struct {
	rng             *rand.Rand
	randpickpercent float64
	data            []T
}

func NewRand[T any](rng *rand.Rand) *Rand[T] {
	randpickpercent := rng.Float64() * 0.1

	slog.Log(
		context.Background(),
		slog.Level(108),
		"new rand array created",
		"rand pick percent", randpickpercent*100,
	)

	return &Rand[T]{
		rng:             rng,
		randpickpercent: randpickpercent,
		data:            make([]T, 0),
	}
}

func (r *Rand[T]) Push(value T) {
	r.data = append(r.data, value)
}

func (r *Rand[T]) Pop() (T, bool) {
	randpickChance := r.rng.Float64()
	if randpickChance <= r.randpickpercent {
		return r.popRandom()
	}

	return r.popOrdered()
}

func (r *Rand[T]) Len() int {
	return len(r.data)
}

func (r *Rand[T]) popOrdered() (T, bool) {
	var t T
	if len(r.data) == 0 {
		return t, false
	}

	t, r.data[0] = r.data[0], r.data[len(r.data)-1]
	r.data = r.data[:len(r.data)-1]
	return t, true
}

func (r *Rand[T]) popRandom() (T, bool) {
	var t T
	if len(r.data) == 0 {
		return t, false
	}

	i := r.rng.Intn(len(r.data))
	t, r.data[i] = r.data[i], r.data[len(r.data)-1]

	r.data = r.data[:len(r.data)-1]
	return t, true
}
