package fuqtest

import (
	"context"
	"fmt"
	"runtime"
)

/* goPanicOnError Runs a function in a new goroutine and panics if the
 * function returns a non-nil error value.  The function must take a
 * context.
 */
func GoPanicOnError(ctx context.Context, f func(context.Context) error) {
	trace := make([]byte, 2048)
	n := runtime.Stack(trace, false)
	trace = trace[:n]
	go func() {
		if err := f(ctx); err != nil {
			panic(fmt.Sprintf("%s\n\nconversation loop error: %v", trace, err))
		}
	}()
}
