package bench

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"golang.org/x/sys/unix"
)

func affinity(t *testing.T) unix.CPUSet {
	t.Helper()
	var s unix.CPUSet
	if err := unix.SchedGetaffinity(0, &s); err != nil {
		t.Fatal(err)
	}
	return s
}

// allowedCPU returns a CPU the calling thread may run on.
func allowedCPU(t *testing.T) int {
	t.Helper()
	s := affinity(t)
	for c := range 1024 {
		if s.IsSet(c) {
			return c
		}
	}
	t.Fatal("no CPU in the affinity mask")
	return -1
}

func TestPinnedRunsOnTheCPUAndRestores(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	before := affinity(t)
	cpu := allowedCPU(t)
	var inside unix.CPUSet
	if err := pinned(cpu, func() { inside = affinity(t) }); err != nil {
		t.Fatal(err)
	}
	if inside.Count() != 1 || !inside.IsSet(cpu) {
		t.Errorf("fn ran with affinity of %d CPUs, want CPU %d only", inside.Count(), cpu)
	}
	if after := affinity(t); after != before {
		t.Error("the caller's affinity changed")
	}
}

func TestPinnedFailsWithoutRunning(t *testing.T) {
	for _, cpu := range []int{-1, 1 << 20} {
		ran := false
		if err := pinned(cpu, func() { ran = true }); err == nil || ran {
			t.Errorf("CPU %d: err %v, ran %v; want an error and no run", cpu, err, ran)
		}
	}
	if _, err := MeasureNoise(1 << 20); err == nil {
		t.Error("MeasureNoise on a CPU that does not exist: no error")
	}
}

// A thread whose affinity cannot be restored must not go back to the pool.
// The runtime terminates it with the goroutine that held it, or, if it is
// the main thread, parks it for good; either way no goroutine runs on it
// again.
func TestPinnedDiscardsAThreadItCannotRestore(t *testing.T) {
	restoreErr := errors.New("restore refused")
	calls := 0
	setAffinity = func(pid int, set *unix.CPUSet) error {
		calls++
		if calls == 2 {
			return restoreErr
		}
		return unix.SchedSetaffinity(pid, set)
	}
	defer func() { setAffinity = unix.SchedSetaffinity }()
	var tid int
	err := pinned(allowedCPU(t), func() { tid = unix.Gettid() })
	if !errors.Is(err, restoreErr) {
		t.Fatalf("err %v, want the restore error", err)
	}
	const workers, rounds = 64, 200
	var wg sync.WaitGroup
	var reused atomic.Bool
	for range workers {
		wg.Go(func() {
			for range rounds {
				if unix.Gettid() == tid {
					reused.Store(true)
				}
				runtime.Gosched()
			}
		})
	}
	wg.Wait()
	if reused.Load() {
		t.Fatalf("thread %d ran other goroutines after its affinity was left pinned", tid)
	}
}
