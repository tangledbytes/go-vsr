package time

import "time"

type Real struct{}

func NewReal() *Real {
	return &Real{}
}

func (r *Real) Now() uint64 {
	return uint64(time.Now().UnixNano())
}

func (r *Real) Tick() {
	// no-op
}

// Real implements Timer
var _ Time = (*Real)(nil)
