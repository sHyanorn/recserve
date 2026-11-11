package api

import (
	"encoding/json"
	"io"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// ExposureQueue is the number of exposures that may wait to be written.
const ExposureQueue = 4096

// Exposure records one answered recommend request, as a line of the
// exposure log (docs/SPEC.md §6).
type Exposure struct {
	Time         time.Time `json:"time"`
	UserID       string    `json:"user_id"`
	TS           int64     `json:"ts"`
	Experiment   string    `json:"experiment"`
	Arm          string    `json:"arm"`
	Ranking      string    `json:"ranking"`
	ModelVersion string    `json:"model_version"`
	Items        []string  `json:"items"`
	Degraded     []string  `json:"degraded"`
	Cached       bool      `json:"cached"`
}

// Exposures writes exposures as JSON lines from a goroutine of its own. A
// request only queues its exposure; one that finds the queue full is
// dropped and counted, so a writer that blocks never holds up requests.
type Exposures struct {
	queue   chan Exposure
	stop    chan struct{}
	done    chan struct{}
	dropped prometheus.Counter
}

// NewExposures starts writing exposures to w.
func NewExposures(w io.Writer) *Exposures {
	e := &Exposures{
		queue: make(chan Exposure, ExposureQueue),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
		dropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "recserve_exposures_dropped_total", Help: "Exposures not written to the exposure log: the queue was full or the write failed.",
		}),
	}
	go func() {
		defer close(e.done)
		enc := json.NewEncoder(w)
		write := func(x Exposure) {
			if enc.Encode(x) != nil {
				e.dropped.Inc()
			}
		}
		for {
			select {
			case x := <-e.queue:
				write(x)
			case <-e.stop:
				for {
					select {
					case x := <-e.queue:
						write(x)
					default:
						return
					}
				}
			}
		}
	}()
	return e
}

// Log queues x, or drops it if the queue is full.
func (e *Exposures) Log(x Exposure) {
	select {
	case e.queue <- x:
	default:
		e.dropped.Inc()
	}
}

// Close writes the exposures queued and stops. Requests still running may
// go on logging; what they log is not written.
func (e *Exposures) Close() {
	close(e.stop)
	<-e.done
}

// Collector returns the counter of dropped exposures, for /metrics.
func (e *Exposures) Collector() prometheus.Collector { return e.dropped }
