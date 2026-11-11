package main

import (
	"context"
	"errors"
	"math/rand/v2"
	"os"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/sHyanorn/recserve/internal/annshard"
	"github.com/sHyanorn/recserve/internal/annshard/annshardtest"
	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/shard/shardtest"
)

// While the fault is on, a search waits until its caller gives up, and the
// replica answers once it is off; the signals switch it.
func TestHangSwitchedBySignals(t *testing.T) {
	var h hang
	signals := make(chan os.Signal)
	go h.follow(signals)
	root := t.TempDir()
	version := annshardtest.WriteVectors(t, root, shardtest.RandomMatrix(rand.New(rand.NewPCG(3, 3)), 50, 2))
	addr := annshardtest.Start(t, root, grpc.UnaryInterceptor(h.intercept))
	c := annshardtest.Dial(t, [][]string{{addr}}, annshard.Options{Config: shard.Config{Shards: 1, Mode: shard.Exact}})
	s, err := c.Prepare(context.Background(), version, 50)
	if err != nil {
		t.Fatal(err)
	}
	search := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_, err := s.Search(ctx, 0, []float32{1, 1}, 5, nil)
		return err
	}
	if err := search(); err != nil {
		t.Fatalf("before the fault: %v", err)
	}
	signals <- syscall.SIGUSR1
	for start := time.Now(); h.on.Load() == false; time.Sleep(time.Millisecond) {
		if time.Since(start) > time.Second {
			t.Fatal("SIGUSR1 did not turn the fault on")
		}
	}
	start := time.Now()
	if err := search(); !errors.Is(err, context.DeadlineExceeded) || time.Since(start) < 200*time.Millisecond {
		t.Fatalf("during the fault: %v after %v, want the caller's deadline", err, time.Since(start))
	}
	signals <- syscall.SIGUSR2
	for start := time.Now(); h.on.Load(); time.Sleep(time.Millisecond) {
		if time.Since(start) > time.Second {
			t.Fatal("SIGUSR2 did not turn the fault off")
		}
	}
	if err := search(); err != nil {
		t.Fatalf("after the fault: %v", err)
	}
}
