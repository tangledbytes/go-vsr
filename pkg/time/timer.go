package time

// Timer is a very simple timer that can be used to check if a certain amount of
// time has passed and if it has then execute an action. The timer doesn't drives
// itself, that is, it needs to be checked manually and the time needs to be ticked
// manually as well (this can be done via OS as well in case of a real time).
//
// Timer allows storing any opaque state into it.
type Timer struct {
	start uint64
	ticks uint64
	time  Time

	state  any
	action func(Time)
}

// NewTimer takes a time and a number of ticks and returns a new timer that will
// execute the action after the given number of ticks has passed.
func NewTimer(time Time, ticks uint64) *Timer {
	return &Timer{
		start: time.Now(),
		ticks: ticks,
		time:  time,
	}
}

// Reset resets the timer
func (t *Timer) Reset() {
	t.start = t.time.Now()
}

// Done returns true if the timer has finished
func (t *Timer) Done() bool {
	return t.time.Now()-t.start >= t.ticks
}

// Action sets the action that will be executed when the timer is done
func (t *Timer) Action(fn func(Time)) {
	t.action = fn
}

// ActIfDone returns true if the timer is done and will execute the
// registered action (if any)
func (t *Timer) ActIfDone() bool {
	if !t.Done() {
		return false
	}

	if t != nil {
		t.action(t.time)
	}

	return true
}

// SetState allows storing arbitrary state into the timer
func (t *Timer) SetState(state any) {
	t.state = state
}

// State returns the state stored in the timer
func (t *Timer) State() (any, bool) {
	return t.state, t.state != nil
}
