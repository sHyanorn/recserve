package shard

import (
	"math"
	"testing"
)

// The mapping is part of the data layout: these values must never change.
func TestGolden(t *testing.T) {
	for _, c := range []struct {
		id      int32
		n, want int
	}{{0, 2, 0}, {1, 2, 1}, {2, 2, 0}, {3, 2, 0}, {12345, 4, 1}, {1 << 30, 3, 1}} {
		if got := Of(c.id, c.n); got != c.want {
			t.Errorf("Of(%d, %d) = %d, want %d", c.id, c.n, got, c.want)
		}
	}
}

func TestBalanced(t *testing.T) {
	const ids = 1_000_000
	for _, n := range []int{2, 3, 4, 7} {
		count := make([]int, n)
		for id := range int32(ids) {
			s := Of(id, n)
			if s < 0 || s >= n {
				t.Fatalf("Of(%d, %d) = %d", id, n, s)
			}
			count[s]++
		}
		for s, c := range count {
			if want := float64(ids) / float64(n); math.Abs(float64(c)-want) > 0.005*want {
				t.Errorf("n=%d shard %d holds %d of %d IDs", n, s, c, ids)
			}
		}
	}
}
