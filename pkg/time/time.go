package time

const NANOSECOND = 1
const MICROSECOND = 1000 * NANOSECOND
const MILLISECOND = 1000 * MICROSECOND
const SECOND = 1000 * MILLISECOND
const MINUTE = 60 * SECOND

// Time abstracts time in the system
type Time interface {
	Now() uint64
	Tick()
}
