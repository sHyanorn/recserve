package api

import (
	"errors"
	"math"
	"net/http"

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

// admit applies s.Limits to h. A request over the rate, or one that finds
// the in-flight limit reached, is answered 503 with Retry-After at once:
// nothing waits for a token or a slot, since a request that waited would
// spend its latency budget at the door.
func (s *Server) admit(h func(http.ResponseWriter, *http.Request) error) func(http.ResponseWriter, *http.Request) error {
	var limiter *rate.Limiter
	if s.Limits.Rate > 0 {
		limiter = rate.NewLimiter(rate.Limit(s.Limits.Rate), int(math.Ceil(s.Limits.Rate)))
	}
	var slots chan struct{}
	if s.Limits.InFlight > 0 {
		slots = make(chan struct{}, s.Limits.InFlight)
	}
	turnAway := func(w http.ResponseWriter, reason string) error {
		s.shed.WithLabelValues(reason).Inc()
		w.Header().Set("Retry-After", "1")
		return httpError{http.StatusServiceUnavailable, errOverloaded}
	}
	return func(w http.ResponseWriter, r *http.Request) error {
		if limiter != nil && !limiter.AllowN(s.Now(), 1) {
			return turnAway(w, shedRate)
		}
		if slots != nil {
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			default:
				return turnAway(w, shedInFlight)
			}
		}
		return h(w, r)
	}
}
