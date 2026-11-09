package onnx

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"

	ort "github.com/yalue/onnxruntime_go"
)

// FreeCalls bounds the idle calls a model keeps for reuse; calls beyond it
// are created when needed and destroyed after use.
const FreeCalls = 64

// Encoder runs a user encoder (docs/SPEC.md §8): input item_seq int64
// [1, SeqLen], left-padded with 0; output user_emb float32 [1, Dim].
type Encoder struct {
	SeqLen, Dim int

	mu     sync.RWMutex // read-held by every Encode, write-held by Close
	closed bool
	sess   *ort.DynamicAdvancedSession
	free   *freeList[*call]
}

// NewEncoder loads an encoder from the bytes of its ONNX model. ONNX
// Runtime checks the model's inputs and outputs only when it runs, so the
// caller runs known cases before trusting it (the bundle's golden cases).
func NewEncoder(model []byte, seqLen, dim int) (*Encoder, error) {
	if seqLen <= 0 || dim <= 0 {
		return nil, fmt.Errorf("encoder sequence length %d and dimension %d must be positive", seqLen, dim)
	}
	sess, err := newSession(model, []string{"item_seq"}, []string{"user_emb"})
	if err != nil {
		return nil, err
	}
	e := &Encoder{SeqLen: seqLen, Dim: dim, sess: sess}
	e.free = newFreeList(FreeCalls, e.newCall, (*call).destroy)
	return e, nil
}

func (e *Encoder) newCall() (*call, error) {
	in, err := ort.NewEmptyTensor[int64](ort.NewShape(1, int64(e.SeqLen)))
	if err != nil {
		return nil, err
	}
	out, err := ort.NewEmptyTensor[float32](ort.NewShape(1, int64(e.Dim)))
	if err != nil {
		return nil, errors.Join(err, in.Destroy())
	}
	opts, err := ort.NewRunOptions()
	if err != nil {
		return nil, errors.Join(err, in.Destroy(), out.Destroy())
	}
	return &call{in: []ort.Value{in}, out: []ort.Value{out}, opts: opts}, nil
}

// Encode returns the user vector of seq, the internal IDs of the user's
// latest items, oldest first, at most SeqLen of them; nil if seq is empty.
// If ctx ends before the run does, the run is terminated and the error
// wraps ctx.Err().
func (e *Encoder) Encode(ctx context.Context, seq []int32) ([]float32, error) {
	if len(seq) > e.SeqLen {
		return nil, fmt.Errorf("%d items for an encoder of %d", len(seq), e.SeqLen)
	}
	if len(seq) == 0 {
		return nil, nil
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return nil, ErrClosed
	}
	c, err := e.free.get()
	if err != nil {
		return nil, err
	}
	in := c.in[0].(*ort.Tensor[int64]).GetData()
	pad := e.SeqLen - len(seq)
	clear(in[:pad])
	for i, id := range seq {
		in[pad+i] = int64(id)
	}
	reusable, err := run(ctx, e.sess, c)
	var out []float32
	if err == nil {
		out = slices.Clone(c.out[0].(*ort.Tensor[float32]).GetData())
	}
	if reusable {
		err = errors.Join(err, e.free.put(c))
	} else {
		err = errors.Join(err, c.destroy())
	}
	if err != nil {
		return nil, err
	}
	for _, v := range out {
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return nil, errors.New("user encoder output is not finite")
		}
	}
	return out, nil
}

// Close waits for running Encode calls, then releases the session and the
// idle calls; Encode returns ErrClosed afterwards.
func (e *Encoder) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	return errors.Join(e.free.drain(), e.sess.Destroy())
}
