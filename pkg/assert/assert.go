//go:build !release

package assert

import "fmt"

// Assert panics if cond is false and will
// print msg to the console.
//
// Assert is a no-op when compiled with the
// release build tag.
func Assert(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintln("assertion failed:", fmt.Sprintf(format, args...)))
	}
}
