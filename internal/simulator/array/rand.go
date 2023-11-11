package array

import (
	"log/slog"
	"math/rand"

	"github.com/tangledbytes/go-vsr/internal/simulator/constant"
)

type Rand[T any] struct {
	rng             *rand.Rand
	randpickpercent float64
	data            []T
}

func NewRand[T any](rng *rand.Rand, logger *slog.Logger) *Rand[T] {
	randpickpercent := rng.Float64() * constant.UNORDERED_PACKET_DELIVERY_PERCENT

	logger.Debug(
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

	t = r.data[0]
	r.data = r.data[1:]
	return t, true
}

func (r *Rand[T]) popRandom() (T, bool) {
	var t T
	if len(r.data) == 0 {
		return t, false
	}

	// Only pick from the first 0.01% of the array
	lenFirstPercent := int(float64(len(r.data)) * float64(0.01))

	i := r.rng.Intn(lenFirstPercent + 1)
	t = r.data[i]
	r.data = append(r.data[:i], r.data[i+1:]...)
	return t, true
}
