package onnx

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/sHyanorn/recserve/internal/onnx/onnxtest"
)

const testFeatures = 6

func newRanker(t *testing.T, model string, rows int) *Ranker {
	t.Helper()
	initORT(t)
	r, err := NewRanker(onnxtest.Model(t, model), testFeatures, rows)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	return r
}

func randomFeatures(r *rand.Rand, rows int) []float32 {
	x := make([]float32, rows*testFeatures)
	for i := range x {
		x[i] = float32(r.NormFloat64())
	}
	return x
}

func checkLinear(x, got []float32) error {
	want := onnxtest.Linear(x)
	if len(got) != len(want) {
		return errors.New("wrong number of scores")
	}
	for i := range want {
		if math.Abs(float64(got[i]-want[i])) > 1e-5 {
			return errors.New("score differs from the linear model")
		}
	}
	return nil
}

func TestRankerMatchesLinear(t *testing.T) {
	const rows = 64
	rk := newRanker(t, "linear_ranker.onnx", rows)
	r := rand.New(rand.NewPCG(1, 1))
	for _, n := range []int{1, 2, 7, rows} {
		x := randomFeatures(r, n)
		got, err := rk.Rank(context.Background(), x)
		if err != nil {
			t.Fatal(err)
		}
		if err := checkLinear(x, got); err != nil {
			t.Fatalf("%d rows: %v", n, err)
		}
	}
	if got, err := rk.Rank(context.Background(), nil); got != nil || err != nil {
		t.Fatalf("no rows: %v, %v", got, err)
	}
}

// Rows past the caller's are zero on every call, also after a call that
// filled them: cross_row_ranker's scores depend on every row, so a stale
// row would change them.
func TestRankerPadsWithZeros(t *testing.T) {
	const rows = 16
	r := rand.New(rand.NewPCG(2, 2))
	x := randomFeatures(r, 3)
	fresh := newRanker(t, "cross_row_ranker.onnx", rows)
	want, err := fresh.Rank(context.Background(), x)
	if err != nil {
		t.Fatal(err)
	}
	used := newRanker(t, "cross_row_ranker.onnx", rows)
	if _, err := used.Rank(context.Background(), randomFeatures(r, rows)); err != nil {
		t.Fatal(err)
	}
	got, err := used.Rank(context.Background(), x)
	if err != nil {
		t.Fatal(err)
	}
	for i := range want {
		if math.Float32bits(got[i]) != math.Float32bits(want[i]) {
			t.Fatalf("row %d: %v after a full call, %v on a new ranker", i, got[i], want[i])
		}
	}
}

func TestRankerRejectsBadInput(t *testing.T) {
	rk := newRanker(t, "linear_ranker.onnx", 4)
	for _, n := range []int{testFeatures - 1, 5 * testFeatures} {
		if _, err := rk.Rank(context.Background(), make([]float32, n)); err == nil {
			t.Errorf("%d values accepted by a ranker of %d features and 4 rows", n, testFeatures)
		}
	}
	col := newRanker(t, "column_ranker.onnx", 4)
	if _, err := col.Rank(context.Background(), make([]float32, testFeatures)); err == nil {
		t.Error("a [C, 1] output was accepted")
	}
}

func TestRankerConcurrent(t *testing.T) {
	const rows, workers = 32, 16
	rk := newRanker(t, "linear_ranker.onnx", rows)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(uint64(w), 4))
			for range 100 {
				x := randomFeatures(r, 1+r.IntN(rows))
				got, err := rk.Rank(context.Background(), x)
				if err == nil {
					err = checkLinear(x, got)
				}
				if err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
}

// A ranker run whose context ends is terminated, and the error wraps the
// context's.
func TestRankerTimeout(t *testing.T) {
	const rows = 1 << 17
	rk := newRanker(t, "slow_ranker.onnx", rows)
	x := make([]float32, rows*testFeatures)
	start := time.Now()
	if _, err := rk.Rank(context.Background(), x); err != nil {
		t.Fatal(err)
	}
	full := time.Since(start)
	if full < 100*time.Millisecond {
		t.Fatalf("a full run took %v; the test needs a slower model", full)
	}
	ctx, cancel := context.WithTimeout(context.Background(), full/20)
	defer cancel()
	start = time.Now()
	_, err := rk.Rank(ctx, x)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if took := time.Since(start); took > full/2 {
		t.Fatalf("the terminated run took %v, a full run %v", took, full)
	}
}

func TestRankerClose(t *testing.T) {
	initORT(t)
	rk, err := NewRanker(onnxtest.Model(t, "linear_ranker.onnx"), testFeatures, 4)
	if err != nil {
		t.Fatal(err)
	}
	x := make([]float32, testFeatures)
	if _, err := rk.Rank(context.Background(), x); err != nil {
		t.Fatal(err)
	}
	if err := rk.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := rk.Rank(context.Background(), x); !errors.Is(err, ErrClosed) {
		t.Fatalf("after Close: %v", err)
	}
	if err := rk.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
