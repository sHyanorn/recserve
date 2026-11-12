package breaker

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// The gateway's settings against the three conditions D037's revision of
// M7 set before they were chosen, on the fake clock of a synctest bubble
// (gobreaker reads time.Now). Calls start at a steady rate; one that times
// out takes the history budget and fails with context.DeadlineExceeded, one
// that succeeds takes 0.5 ms.
const budget = 5 * time.Millisecond

// drive starts calls to a breaker with the gateway's settings, rate a
// second for n calls, call i timing out when timeout(i). It returns the
// time from the first call's start to the breaker's first opening, and
// whether it opened.
func drive(t *testing.T, rate, n int, timeout func(i int) bool) (opened time.Duration, ok bool) {
	t.Helper()
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		var mu sync.Mutex
		b := New(Gateway, func(from, to string) {
			mu.Lock()
			defer mu.Unlock()
			if !ok && to == "open" {
				opened, ok = time.Since(start), true
			}
		})
		var wg sync.WaitGroup
		for i := range n {
			wg.Go(func() {
				_ = b.do("history", func() error {
					if timeout(i) {
						time.Sleep(budget)
						return context.DeadlineExceeded
					}
					time.Sleep(500 * time.Microsecond)
					return nil
				})
			})
			time.Sleep(time.Second / time.Duration(rate))
		}
		wg.Wait()
	})
	return opened, ok
}

// On a healthy path, a run of up to 10 timeouts in a row does not open the
// breaker, wherever it falls in the windows: at the rate of the fault runs'
// history reads, and three times that, the calls of the runs with the
// cache on.
func TestGatewayIgnoresShortRuns(t *testing.T) {
	for _, rate := range []int{275, 825} {
		for k := 0; k <= rate+int(Gateway.MinCalls); k++ {
			if at, ok := drive(t, rate, k+10+2*rate, func(i int) bool { return i >= k && i < k+10 }); ok {
				t.Fatalf("%d calls/s, 10 timeouts from call %d: opened after %v", rate, k, at)
			}
		}
	}
}

// When every call fails, at 275 calls a second, the breaker opens within
// 1 s of the first failure, however many calls succeeded in the window
// before it.
func TestGatewayOpensWithinASecond(t *testing.T) {
	const rate = 275
	for k := 0; k <= 2*rate; k++ {
		at, ok := drive(t, rate, k+2*rate, func(i int) bool { return i >= k })
		start := time.Duration(k) * time.Second / rate
		if !ok || at-start > time.Second {
			t.Fatalf("all calls time out from call %d (%v): opened %v after the first call", k, start, at)
		}
	}
}

// Below MinCalls calls in a window the breaker never opens, however many of
// them fail: every call waits out its budget (D037, Limits).
func TestGatewayBelowMinimumStaysClosed(t *testing.T) {
	const rate = 90
	if at, ok := drive(t, rate, 10*rate, func(int) bool { return true }); ok {
		t.Fatalf("%d calls/s, all timing out: opened after %v", rate, at)
	}
}
