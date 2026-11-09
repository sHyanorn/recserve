package bench

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// cpuFlags are the instruction sets worth recording: they decide how fast
// the same code runs on two machines with the same vCPU count.
var cpuFlags = []string{"sse4_2", "avx", "avx2", "fma", "avx512f", "avx512_vnni", "avx512_bf16", "amx_tile", "amx_bf16", "amx_int8"}

// Env records the machine, process and code a result came from, with
// extra entries added. The executable's checksum identifies the build: the
// same source can run at different speeds in two builds (noise.go).
func Env(extra map[string]string) map[string]string {
	e := map[string]string{
		"go":         runtime.Version(),
		"gomaxprocs": fmt.Sprint(runtime.GOMAXPROCS(0)),
		"numcpu":     fmt.Sprint(runtime.NumCPU()),
		"date":       time.Now().UTC().Format(time.RFC3339),
		"commit":     "unknown",
	}
	if b, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, l := range strings.Split(string(b), "\n") {
			k, v, _ := strings.Cut(l, ":")
			switch strings.TrimSpace(k) {
			case "model name":
				e["cpu"] = strings.TrimSpace(v)
			case "flags":
				have := strings.Fields(v)
				var got []string
				for _, f := range cpuFlags {
					if slices.Contains(have, f) {
						got = append(got, f)
					}
				}
				e["cpu_flags"] = strings.Join(got, " ")
			}
			if e["cpu"] != "" && e["cpu_flags"] != "" {
				break
			}
		}
	}
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		if l, _, ok := strings.Cut(string(b), "\n"); ok && strings.HasPrefix(l, "MemTotal:") {
			e["memory"] = strings.Join(strings.Fields(strings.TrimPrefix(l, "MemTotal:")), " ")
		}
	}
	if out, err := exec.Command("uname", "-r").Output(); err == nil {
		e["kernel"] = strings.TrimSpace(string(out))
	}
	var set unix.CPUSet
	if unix.SchedGetaffinity(0, &set) == nil {
		var cpus []string
		for i := 0; len(cpus) < set.Count(); i++ {
			if set.IsSet(i) {
				cpus = append(cpus, fmt.Sprint(i))
			}
		}
		e["cpu_affinity"] = strings.Join(cpus, ",")
	}
	if exe, err := os.Executable(); err == nil {
		e["executable_sha256"] = FileSHA256(exe)
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				e["commit"] = s.Value
			case "vcs.modified":
				e["uncommitted_changes"] = s.Value
			}
		}
	}
	for k, v := range extra {
		e[k] = v
	}
	return e
}

// FileSHA256 returns the hex SHA-256 of a file, or the error text.
func FileSHA256(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return "error: " + err.Error()
	}
	defer f.Close() //nolint:errcheck // read only
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "error: " + err.Error()
	}
	return hex.EncodeToString(h.Sum(nil))
}
