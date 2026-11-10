package history

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/sHyanorn/recserve/internal/infratest"
)

// valkeyStores returns a source of empty stores on the test's Valkey: each
// has its own key prefix on one shared client, and the test's keys are
// deleted when it ends.
func valkeyStores(t *testing.T) func() Store {
	t.Helper()
	base, err := NewValkey(infratest.ValkeyAddr(t))
	if err != nil {
		t.Fatal(err)
	}
	root := fmt.Sprintf("test:%016x:", rand.Uint64())
	t.Cleanup(func() {
		defer base.Close()
		ctx, c := context.Background(), base.c
		for cursor := uint64(0); ; {
			page, err := c.Do(ctx, c.B().Scan().Cursor(cursor).Match(root+"*").Count(1000).Build()).AsScanEntry()
			if err != nil {
				t.Errorf("deleting the test's keys: %v", err)
				return
			}
			if len(page.Elements) > 0 {
				if err := c.Do(ctx, c.B().Unlink().Key(page.Elements...).Build()).Error(); err != nil {
					t.Errorf("deleting the test's keys: %v", err)
					return
				}
			}
			if cursor = page.Cursor; cursor == 0 {
				return
			}
		}
	})
	n := 0
	return func() Store {
		n++
		return &Valkey{c: base.c, prefix: fmt.Sprintf("%s%d:", root, n)}
	}
}

// An event time that a score cannot hold exactly is refused, not rounded.
func TestValkeyRefusesTimeBeyondMaxTS(t *testing.T) {
	s, ctx := valkeyStores(t)(), context.Background()
	for _, ts := range []int64{MaxTS + 1, -MaxTS - 1} {
		if err := s.Append(ctx, []Event{{ID: 1, User: "u", Item: "a", TS: 1}, {ID: 2, User: "u", Item: "b", TS: ts}}); err == nil {
			t.Fatalf("ts %d was stored", ts)
		}
	}
	if got, err := s.Before(ctx, "u", 10, 0); err != nil || len(got) != 0 {
		t.Fatalf("a refused batch stored %v, %v", got, err)
	}
}
