package onnx

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	ort "github.com/yalue/onnxruntime_go"

	"github.com/sHyanorn/recserve/internal/onnx/onnxtest"
	"github.com/sHyanorn/recserve/internal/vec"
)

func initORT(t *testing.T) string {
	t.Helper()
	lib := onnxtest.Lib(t)
	if err := Init(lib); err != nil {
		t.Fatal(err)
	}
	return lib
}

func TestInitTwice(t *testing.T) {
	lib := initORT(t)
	if err := Init(lib); err != nil {
		t.Fatalf("same library again: %v", err)
	}
	if err := Init(lib + ".other"); err == nil {
		t.Fatal("a second library was accepted")
	}
}

// Without Init, loading a model fails with ErrNotInitialized. The check
// runs in a child process, where no other test has initialized ONNX Runtime.
func TestNotInitialized(t *testing.T) {
	if os.Getenv("RECSERVE_ONNX_CHILD") == "1" {
		if _, err := NewEncoder(onnxtest.Model(t, "mean.onnx"), 4, onnxtest.MeanDim); !errors.Is(err, ErrNotInitialized) {
			t.Fatalf("err = %v, want ErrNotInitialized", err)
		}
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestNotInitialized$", "-test.count=1")
	cmd.Env = append(os.Environ(), "RECSERVE_ONNX_CHILD=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
}

func meanEncoder(t *testing.T, seqLen int) *Encoder {
	t.Helper()
	initORT(t)
	e, err := NewEncoder(onnxtest.Model(t, "mean.onnx"), seqLen, onnxtest.MeanDim)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})
	return e
}

// checkMean returns an error unless got is the mean of the vectors of seq
// within 1e-5.
func checkMean(seq []int32, got []float32) error {
	m, err := vec.NewMatrix(onnxtest.MeanDim, onnxtest.MeanVectors())
	if err != nil {
		return err
	}
	want := m.Mean(seq)
	if len(got) != len(want) {
		return errors.New("wrong length")
	}
	for j := range want {
		if math.Abs(float64(got[j]-want[j])) > 1e-5 {
			return errors.New("not the mean")
		}
	}
	return nil
}

func randomSeq(r *rand.Rand, maxLen int) []int32 {
	seq := make([]int32, 1+r.IntN(maxLen))
	for i := range seq {
		seq[i] = int32(1 + r.IntN(onnxtest.MeanItems))
	}
	return seq
}

func TestEncoderMatchesMean(t *testing.T) {
	const seqLen = 8
	e := meanEncoder(t, seqLen)
	r := rand.New(rand.NewPCG(1, 2))
	ctx := context.Background()
	for range 500 {
		seq := randomSeq(r, seqLen)
		got, err := e.Encode(ctx, seq)
		if err != nil {
			t.Fatal(err)
		}
		if err := checkMean(seq, got); err != nil {
			t.Fatalf("%v: %v gives %v", err, seq, got)
		}
	}
	if got, err := e.Encode(ctx, nil); got != nil || err != nil {
		t.Fatalf("empty sequence: %v, %v", got, err)
	}
	if _, err := e.Encode(ctx, make([]int32, seqLen+1)); err == nil {
		t.Fatal("a sequence longer than the encoder's was accepted")
	}
}

// Concurrent callers share the session, each with its own call; the calls
// go back to the free list for reuse.
func TestEncoderConcurrent(t *testing.T) {
	const seqLen, workers = 8, 16
	e := meanEncoder(t, seqLen)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(uint64(w), 3))
			for range 200 {
				seq := randomSeq(r, seqLen)
				got, err := e.Encode(context.Background(), seq)
				if err == nil {
					err = checkMean(seq, got)
				}
				if err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	if n := len(e.free.idle); n < 1 || n > workers {
		t.Fatalf("%d idle calls after %d workers", n, workers)
	}
}

func TestFreeListIsBounded(t *testing.T) {
	made, destroyed := 0, 0
	f := newFreeList(2, func() (int, error) { made++; return made, nil }, func(int) error { destroyed++; return nil })
	var held []int
	for range 5 {
		v, err := f.get()
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, v)
	}
	for _, v := range held {
		if err := f.put(v); err != nil {
			t.Fatal(err)
		}
	}
	if made != 5 || destroyed != 3 || len(f.idle) != 2 {
		t.Fatalf("made %d, destroyed %d, idle %d; want 5, 3, 2", made, destroyed, len(f.idle))
	}
	if v, err := f.get(); err != nil || made != 5 || (v != 1 && v != 2) {
		t.Fatalf("get after put: %d, %v, made %d", v, err, made)
	}
	if err := f.drain(); err != nil || destroyed != 4 || len(f.idle) != 0 {
		t.Fatalf("drain: %v, destroyed %d, idle %d", err, destroyed, len(f.idle))
	}
}

// A context that has ended stops Encode before it takes a call: on a new
// encoder the free list stays empty.
func TestEncodeEndedContext(t *testing.T) {
	e := meanEncoder(t, 4)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := e.Encode(ctx, []int32{1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if n := len(e.free.idle); n != 0 {
		t.Fatalf("%d idle calls, want 0", n)
	}
}

// An output shape the model does not produce fails the call, and the
// call is still reusable.
func TestEncodeWrongDim(t *testing.T) {
	initORT(t)
	e, err := NewEncoder(onnxtest.Model(t, "mean.onnx"), 4, onnxtest.MeanDim+1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	}()
	for range 2 {
		if _, err := e.Encode(context.Background(), []int32{1}); err == nil {
			t.Fatal("a 3-dimensional output was accepted from a 2-dimensional model")
		}
	}
	if len(e.free.idle) != 1 {
		t.Fatalf("idle calls %d, want 1", len(e.free.idle))
	}
}

func TestEncoderClose(t *testing.T) {
	initORT(t)
	e, err := NewEncoder(onnxtest.Model(t, "mean.onnx"), 4, onnxtest.MeanDim)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Encode(context.Background(), []int32{1}); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Encode(context.Background(), []int32{1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("after Close: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// slowCall returns a session of slow.onnx and a call on an n-element input.
func slowCall(t *testing.T, n int64) (*ort.DynamicAdvancedSession, *call) {
	t.Helper()
	initORT(t)
	sess, err := newSession(onnxtest.Model(t, "slow.onnx"), []string{"x"}, []string{"y"})
	if err != nil {
		t.Fatal(err)
	}
	x, err := ort.NewEmptyTensor[float32](ort.NewShape(n))
	if err != nil {
		t.Fatal(err)
	}
	y, err := ort.NewEmptyTensor[float32](ort.NewShape(n))
	if err != nil {
		t.Fatal(err)
	}
	opts, err := ort.NewRunOptions()
	if err != nil {
		t.Fatal(err)
	}
	c := &call{in: []ort.Value{x}, out: []ort.Value{y}, opts: opts}
	t.Cleanup(func() {
		if err := errors.Join(c.destroy(), sess.Destroy()); err != nil {
			t.Error(err)
		}
	})
	return sess, c
}

// A run whose context ends is terminated between nodes, long before it
// would finish, and its run options are ready for the next run.
func TestRunTerminates(t *testing.T) {
	sess, c := slowCall(t, 1<<20)
	start := time.Now()
	if _, err := run(context.Background(), sess, c); err != nil {
		t.Fatal(err)
	}
	full := time.Since(start)
	if full < 100*time.Millisecond {
		t.Fatalf("a full run took %v; the test needs a slower model", full)
	}
	ctx, cancel := context.WithTimeout(context.Background(), full/20)
	defer cancel()
	start = time.Now()
	reusable, err := run(ctx, sess, c)
	took := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) || !reusable {
		t.Fatalf("err = %v, reusable %v; want DeadlineExceeded, reusable", err, reusable)
	}
	if took > full/2 {
		t.Fatalf("the terminated run took %v, a full run %v", took, full)
	}
	t.Logf("full run %v, terminated after %v: %v", full, took, err)
	if _, err := run(context.Background(), sess, c); err != nil {
		t.Fatalf("run after a terminated one: %v", err)
	}
}

// A run that ONNX Runtime ends for the terminate flag ended with its
// context; any other error of a run during which the context ended is the
// run's own, since reporting it as a timeout would hide a failing model.
func TestRunErrorOfEndedContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	terminated := errors.New("Error running network: Exiting due to terminate flag being set to true.")
	if err := runError(ctx, terminated); !errors.Is(err, context.Canceled) {
		t.Errorf("terminated run: %v, want it to wrap context.Canceled", err)
	}
	failed := errors.New("Error running network: Non-zero status code returned while running Reshape node.")
	if err := runError(ctx, failed); errors.Is(err, context.Canceled) || !errors.Is(err, failed) {
		t.Errorf("failed run: %v, want the run's error alone", err)
	}
}
