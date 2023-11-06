package time

import "github.com/tangledbytes/go-vsr/pkg/assert"

type Simulated struct {
	ticks  uint64
	factor uint64
}

// NewSimulated returns a new simulated timer with the given factor.
//
// The factor determines how fast the timer will tick. A factor of 1 means
// that every tick will increase the ticks by 1. A factor of 2 means that
// every tick will increase the ticks by 2.
//
// The factor must be greater than 0.
func NewSimulated(factor uint64) *Simulated {
	assert.Assert(factor > 0, "factor must be greater than 0")

	return &Simulated{
		ticks:  0,
		factor: factor,
	}
}

func (s *Simulated) Now() uint64 {
	return s.ticks
}

func (s *Simulated) Tick() {
	s.ticks += s.factor
}
