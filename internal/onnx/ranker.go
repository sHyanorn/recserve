package onnx

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	ort "github.com/yalue/onnxruntime_go"
)

// Ranker runs a ranker (docs/SPEC.md §8): input features float32 [Rows,
// Features], output score float32 [Rows]. Every call passes Rows rows, the
// rows past the caller's candidates set to zero, so the tensors of a call
// are allocated once and reused.
type Ranker struct {
	Features, Rows int

	mu     sync.RWMutex // read-held by every Rank, write-held by Close
	closed bool
	sess   *ort.DynamicAdvancedSession
	free   *freeList[*call]
}

// NewRanker loads a ranker from the bytes of its ONNX model. As with
// NewEncoder, the model's inputs and outputs are checked only when it runs.
func NewRanker(model []byte, features, rows int) (*Ranker, error) {
	if features <= 0 || rows <= 0 {
		return nil, fmt.Errorf("ranker features %d and rows %d must be positive", features, rows)
	}
	sess, err := newSession(model, []string{"features"}, []string{"score"})
	if err != nil {
		return nil, err
	}
	r := &Ranker{Features: features, Rows: rows, sess: sess}
	r.free = newFreeList(FreeCalls, r.newCall, (*call).destroy)
	return r, nil
}

func (r *Ranker) newCall() (*call, error) {
	in, err := ort.NewEmptyTensor[float32](ort.NewShape(int64(r.Rows), int64(r.Features)))
	if err != nil {
		return nil, err
	}
	out, err := ort.NewEmptyTensor[float32](ort.NewShape(int64(r.Rows)))
	if err != nil {
		return nil, errors.Join(err, in.Destroy())
	}
	opts, err := ort.NewRunOptions()
	if err != nil {
		return nil, errors.Join(err, in.Destroy(), out.Destroy())
	}
	return &call{in: []ort.Value{in}, out: []ort.Value{out}, opts: opts}, nil
}

// Rank returns one score per row of features, a row-major table of
// Features columns and at most Rows rows; nil if it is empty. If ctx ends
// before the run does, the run is terminated and the error wraps ctx.Err().
func (r *Ranker) Rank(ctx context.Context, features []float32) ([]float32, error) {
	if len(features)%r.Features != 0 || len(features) > r.Rows*r.Features {
		return nil, fmt.Errorf("%d feature values for a ranker of %d features and %d rows", len(features), r.Features, r.Rows)
	}
	n := len(features) / r.Features
	if n == 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, ErrClosed
	}
	c, err := r.free.get()
	if err != nil {
		return nil, err
	}
	in := c.in[0].(*ort.Tensor[float32]).GetData()
	clear(in[copy(in, features):])
	reusable, err := run(ctx, r.sess, c)
	var out []float32
	if err == nil {
		out = slices.Clone(c.out[0].(*ort.Tensor[float32]).GetData()[:n])
	}
	if rerr := release(r.free, c, reusable, err); rerr != nil {
		return nil, fmt.Errorf("releasing a ranker call: %w", rerr)
	}
	if err != nil {
		return nil, err
	}
	if !finite(out) {
		return nil, errors.New("ranker output is not finite")
	}
	return out, nil
}

// Close waits for running Rank calls, then releases the session and the
// idle calls; Rank returns ErrClosed afterwards.
func (r *Ranker) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	return errors.Join(r.free.drain(), r.sess.Destroy())
}
