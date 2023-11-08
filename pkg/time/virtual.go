package time

import "github.com/tangledbytes/go-vsr/pkg/assert"

type Virtual struct {
	ticks  uint64
	factor uint64
}

// NewVirtual returns a new simulated timer with the given factor.
//
// The factor determines how fast the timer will tick. A factor of 1 means
// that every tick will increase the ticks by 1. A factor of 2 means that
// every tick will increase the ticks by 2.
//
// The factor must be greater than 0.
func NewVirtual(factor uint64) *Virtual {
	assert.Assert(factor > 0, "factor must be greater than 0")

	return &Virtual{
		ticks:  0,
		factor: factor,
	}
}

func (s *Virtual) Now() uint64 {
	return s.ticks
}

func (s *Virtual) Tick() {
	s.ticks += s.factor
}

func (s *Virtual) TickBy(units uint64) {
	adjusted := units / s.factor
	s.ticks += adjusted
}
