//go:build release

package assert

// Assert panics if cond is false and will
// print msg to the console.
//
// Assert is a no-op when compiled with the
// release build tag.
func Assert(cond bool, msg string) {
	// no-op
}
