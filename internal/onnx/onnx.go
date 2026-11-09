// Package onnx runs ONNX models with ONNX Runtime through onnxruntime_go
// (D006). Every session runs with one intra-op thread (D015); concurrent
// callers share a session, each with its own tensors.
package onnx

import (
	"context"
	"errors"
	"fmt"
	"sync"

	ort "github.com/yalue/onnxruntime_go"
)

var (
	// ErrNotInitialized is returned when a model is loaded before Init.
	ErrNotInitialized = errors.New("ONNX Runtime is not initialized")
	// ErrClosed is returned by a closed model.
	ErrClosed = errors.New("ONNX model is closed")
)

var (
	initMu  sync.Mutex
	initLib string
)

// Init loads the ONNX Runtime shared library at lib and creates the
// process-wide environment. Calling it again with the same library does
// nothing; another library is an error.
func Init(lib string) error {
	initMu.Lock()
	defer initMu.Unlock()
	if initLib != "" {
		if lib != initLib {
			return fmt.Errorf("onnx: initialized with %s, not %s", initLib, lib)
		}
		return nil
	}
	ort.SetSharedLibraryPath(lib)
	if err := ort.InitializeEnvironment(); err != nil {
		return fmt.Errorf("onnx: %w", err)
	}
	initLib = lib
	return nil
}

// newSession creates a session over the bytes of a model, with one
// intra-op thread.
func newSession(model []byte, inputs, outputs []string) (_ *ort.DynamicAdvancedSession, err error) {
	if !ort.IsInitialized() {
		return nil, ErrNotInitialized
	}
	opts, err := ort.NewSessionOptions()
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, opts.Destroy()) }()
	if err := opts.SetIntraOpNumThreads(1); err != nil {
		return nil, err
	}
	return ort.NewDynamicAdvancedSessionWithONNXData(model, inputs, outputs, opts)
}

// call holds what one run needs besides the session: its inputs, outputs
// and run options. One goroutine uses a call at a time.
type call struct {
	in, out []ort.Value
	opts    *ort.RunOptions
}

func (c *call) destroy() error {
	errs := []error{c.opts.Destroy()}
	for _, v := range append(c.in, c.out...) {
		errs = append(errs, v.Destroy())
	}
	return errors.Join(errs...)
}

// run runs sess on c's tensors. If ctx ends first, the run is terminated
// through c's run options and the error wraps ctx.Err(). reusable is false
// when the terminate flag could not be set or cleared, so c must be
// destroyed.
func run(ctx context.Context, sess *ort.DynamicAdvancedSession, c *call) (reusable bool, err error) {
	if err := ctx.Err(); err != nil {
		return true, err
	}
	var terminateErr error
	fired := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		terminateErr = c.opts.Terminate()
		close(fired)
	})
	err = sess.RunWithOptions(c.in, c.out, c.opts)
	if stop() {
		return true, err
	}
	// ctx ended during the run: the flag may be set even if the run finished.
	<-fired
	if err != nil {
		err = fmt.Errorf("%w: %w", ctx.Err(), err)
	}
	if terminateErr != nil {
		return false, errors.Join(err, fmt.Errorf("terminate: %w", terminateErr))
	}
	if uerr := c.opts.UnsetTerminate(); uerr != nil {
		return false, errors.Join(err, fmt.Errorf("unset terminate: %w", uerr))
	}
	return true, err
}

// freeList keeps up to cap(idle) idle values for reuse; values returned
// beyond that are destroyed.
type freeList[T any] struct {
	idle    chan T
	alloc   func() (T, error)
	destroy func(T) error
}

func newFreeList[T any](size int, alloc func() (T, error), destroy func(T) error) *freeList[T] {
	return &freeList[T]{idle: make(chan T, size), alloc: alloc, destroy: destroy}
}

func (f *freeList[T]) get() (T, error) {
	select {
	case v := <-f.idle:
		return v, nil
	default:
		return f.alloc()
	}
}

func (f *freeList[T]) put(v T) error {
	select {
	case f.idle <- v:
		return nil
	default:
		return f.destroy(v)
	}
}

// drain destroys the idle values.
func (f *freeList[T]) drain() error {
	var errs []error
	for {
		select {
		case v := <-f.idle:
			errs = append(errs, f.destroy(v))
		default:
			return errors.Join(errs...)
		}
	}
}
