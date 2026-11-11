// Package profiling serves the runtime's profiles (net/http/pprof) on a mux
// of the caller's, for a listener that only operators reach.
package profiling

import (
	"net/http"
	"net/http/pprof"
)

// Handle registers the profiles under /debug/pprof/ on mux.
func Handle(mux *http.ServeMux) {
	mux.HandleFunc("GET /debug/pprof/", pprof.Index) // the named profiles: heap, goroutine, block and the others
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
}
