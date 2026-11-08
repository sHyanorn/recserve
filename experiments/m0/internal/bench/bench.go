// Package bench holds the helpers shared by the M0 risk checks: exact
// percentiles, the noise baseline of docs/BENCHMARKS.md, .npy and raw
// float32 readers, and the environment record.
package bench

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strings"
	"time"
)

// Percentile returns the nearest-rank p-quantile (0 < p <= 1) of sorted.
func Percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(math.Ceil(p*float64(len(sorted)))) - 1
	return sorted[max(i, 0)]
}

// Summary is the latency summary written to result files, in microseconds.
type Summary struct {
	N    int     `json:"n"`
	P50  float64 `json:"p50_us"`
	P90  float64 `json:"p90_us"`
	P99  float64 `json:"p99_us"`
	P999 float64 `json:"p999_us"`
	Max  float64 `json:"max_us"`
}

// Summarize sorts d in place and returns its percentiles.
func Summarize(d []time.Duration) Summary {
	slices.Sort(d)
	us := func(x time.Duration) float64 { return float64(x) / 1e3 }
	s := Summary{N: len(d)}
	if len(d) > 0 {
		s.P50, s.P90, s.P99 = us(Percentile(d, .5)), us(Percentile(d, .9)), us(Percentile(d, .99))
		s.P999, s.Max = us(Percentile(d, .999)), us(d[len(d)-1])
	}
	return s
}

// Env records the machine and toolchain a result came from.
func Env(extra map[string]string) map[string]string {
	e := map[string]string{
		"go":         runtime.Version(),
		"gomaxprocs": fmt.Sprint(runtime.GOMAXPROCS(0)),
		"numcpu":     fmt.Sprint(runtime.NumCPU()),
		"date":       time.Now().UTC().Format(time.RFC3339),
	}
	if b, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, l := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(l, "model name") {
				e["cpu"] = strings.TrimSpace(strings.SplitN(l, ":", 2)[1])
				break
			}
		}
	}
	if out, err := exec.Command("uname", "-r").Output(); err == nil {
		e["kernel"] = strings.TrimSpace(string(out))
	}
	for k, v := range extra {
		e[k] = v
	}
	return e
}

// WriteJSON writes v to path, indented.
func WriteJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

// ReadF32 reads a headerless little-endian float32 file of n rows of dim values.
func ReadF32(path string, dim int) ([]float32, int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, err
	}
	if len(b)%(4*dim) != 0 {
		return nil, 0, fmt.Errorf("%s: %d bytes is not a multiple of %d rows", path, len(b), 4*dim)
	}
	v := make([]float32, len(b)/4)
	for i := range v {
		v[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[4*i:]))
	}
	return v, len(v) / dim, nil
}

// ReadI32 reads a headerless little-endian int32 file.
func ReadI32(path string) ([]int32, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(b)%4 != 0 {
		return nil, fmt.Errorf("%s: length %d", path, len(b))
	}
	v := make([]int32, len(b)/4)
	for i := range v {
		v[i] = int32(binary.LittleEndian.Uint32(b[4*i:]))
	}
	return v, nil
}
