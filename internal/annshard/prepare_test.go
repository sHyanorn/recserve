package annshard_test

import (
	"bytes"
	"context"
	"math/rand/v2"
	"runtime"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"github.com/sHyanorn/recserve/internal/annshard"
	"github.com/sHyanorn/recserve/internal/annshard/annshardpb"
	"github.com/sHyanorn/recserve/internal/annshard/annshardtest"
	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/shard/shardtest"
)

// Prepare waits for a replica whose Prepare does not return until its
// context ends, then fails if the replica's shard has no other, and leaves
// none of its calls running.
func TestPrepareFailsWhenItsContextEnds(t *testing.T) {
	r := rand.New(rand.NewPCG(10, 10))
	root := t.TempDir()
	v := annshardtest.WriteVectors(t, root, shardtest.RandomMatrix(r, 50, 2))
	arrived := make(chan struct{}, 1)
	stuck := grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if _, ok := req.(*annshardpb.PrepareRequest); !ok {
			return handler(ctx, req)
		}
		select {
		case arrived <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	})
	c := annshardtest.Dial(t, [][]string{{annshardtest.Start(t, root, stuck)}}, annshard.Options{Config: shard.Config{Shards: 1, Mode: shard.Exact}, PrepareTimeout: 2 * time.Minute})
	const timeout = 200 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	start := time.Now()
	if _, err := c.Prepare(ctx, v, 50); err == nil {
		t.Fatal("prepared with the shard's only replica stuck")
	}
	if took := time.Since(start); took < timeout || took > time.Second {
		t.Fatalf("the Prepare whose context ended after %v took %v", timeout, took)
	}
	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("no Prepare arrived")
	}
	buf := make([]byte, 1<<20)
	if stacks := buf[:runtime.Stack(buf, true)]; bytes.Contains(stacks, []byte("annshard.(*Client).Prepare")) {
		t.Fatalf("a call of Prepare runs after it returned:\n%s", stacks)
	}
}
