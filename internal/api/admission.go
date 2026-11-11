package api

import (
	"errors"
	"math"
	"net/http"
	"time"

	"golang.org/x/time/rate"
)

// Limits bounds the recommend requests a gateway takes (D050); zero values
// leave a limit off.
type Limits struct {
	Rate     float64 // requests per second, with a burst of one second's worth
	InFlight int     // requests at a time
}

// The reasons a request is turned away, as recserve_shed_total counts them.
const (
	shedRate     = "rate"
	shedInFlight = "inflight"
)

var errOverloaded = errors.New("overloaded: retry later")

// gate applies Limits. A request over the rate, or one that finds the
// in-flight limit reached, is answered 503 with Retry-After at once:
// nothing waits for a token or a slot, since a request that waited would
// spend its latency budget at the door.
type gate struct {
	limiter *rate.Limiter // nil without a rate limit
	slots   chan struct{} // nil without an in-flight limit
}

func newGate(l Limits) *gate {
	g := &gate{}
	if l.Rate > 0 {
		g.limiter = rate.NewLimiter(rate.Limit(l.Rate), int(math.Ceil(l.Rate)))
	}
	if l.InFlight > 0 {
		g.slots = make(chan struct{}, l.InFlight)
	}
	return g
}

// allow reports whether a request arriving at now is within the rate.
func (g *gate) allow(now time.Time) bool {
	return g.limiter == nil || g.limiter.AllowN(now, 1)
}

// enter takes an in-flight slot, if one is free; leave returns it.
func (g *gate) enter() bool {
	if g.slots == nil {
		return true
	}
	select {
	case g.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (g *gate) leave() {
	if g.slots != nil {
		<-g.slots
	}
}

// turnAway answers a request that the limits turn away.
func (s *Server) turnAway(w http.ResponseWriter, reason string) error {
	s.shed.WithLabelValues(reason).Inc()
	w.Header().Set("Retry-After", "1")
	return httpError{http.StatusServiceUnavailable, errOverloaded}
}
