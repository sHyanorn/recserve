package experiment

import (
	"fmt"
	"hash/maphash"
	"math"
	"strings"
	"testing"
)

// golden buckets, computed outside Go:
//
//	printf '%s.%s' NAME USER | sha256sum | cut -c1-16
//
// read as an unsigned integer, mod 10000.
var golden = []struct {
	name, user string
	bucket     int
}{
	{"ranker-v1", "AGKHLEW2SOWHNMFQIJGBECAF2XWA", 1801},
	{"ranker-v1", "AFSKPY37N3C43SOI5IEXEK5JSIYA", 811},
	{"ranker-v1", "u1", 4561},
	{"ranker-v2", "u1", 4087},
	{"exp", "用户", 8962},
	{"exp", "user.with.dots", 7848},
	{"a.b", "c", 7836},
}

// chiSquare999 is the 0.999 quantile of the chi-square distribution with 99
// degrees of freedom.
const chiSquare999 = 148.23

// checkProperty3 checks property 3 of docs/SPEC.md §7 for an assignment
// that places a user in a bucket: the golden buckets, which hold across
// processes and restarts; buckets uniform over 1M IDs (chi-square over 100
// equal ranges); and the arms' shares of those IDs within 0.003 of a
// 10/30/60 split.
func checkProperty3(bucket func(name, user string) int) error {
	for _, g := range golden {
		if got := bucket(g.name, g.user); got != g.bucket {
			return fmt.Errorf("bucket(%q, %q) = %d, want %d", g.name, g.user, got, g.bucket)
		}
	}
	e, err := Parse("split", "a=retrieval:10,b=ranker:30,c=ranker:60")
	if err != nil {
		return err
	}
	const n = 1_000_000
	var ranges [100]int
	arms := map[string]int{}
	for i := range n {
		b := bucket(e.Name, fmt.Sprintf("user-%d", i))
		ranges[b*len(ranges)/Buckets]++
		arms[e.arm(b).Name]++
	}
	var chi float64
	want := float64(n) / float64(len(ranges))
	for _, c := range ranges {
		chi += (float64(c) - want) * (float64(c) - want) / want
	}
	if chi > chiSquare999 {
		return fmt.Errorf("chi-square %.1f over %d ranges, above %.2f", chi, len(ranges), chiSquare999)
	}
	for name, share := range map[string]float64{"a": 0.1, "b": 0.3, "c": 0.6} {
		if got := float64(arms[name]) / n; math.Abs(got-share) > 0.003 {
			return fmt.Errorf("arm %s holds %.4f of the users, configured %.2f", name, got, share)
		}
	}
	return nil
}

func TestProperty3(t *testing.T) {
	if err := checkProperty3(Bucket); err != nil {
		t.Fatal(err)
	}
}

// The broken variant hashes with a seed drawn by the process: uniform and
// split as configured, but another process places users elsewhere.
func TestProperty3BrokenVariant(t *testing.T) {
	seed := maphash.MakeSeed()
	broken := func(name, user string) int { return int(maphash.String(seed, name+"."+user) % Buckets) }
	err := checkProperty3(broken)
	if err == nil || !strings.Contains(err.Error(), "want") {
		t.Fatalf("broken variant: %v, want a golden bucket mismatch", err)
	}
}

func TestAssign(t *testing.T) {
	e, err := Parse("ranker-v1", "control=retrieval:50,treatment=ranker:50")
	if err != nil {
		t.Fatal(err)
	}
	// Buckets 0 to 4999 are control's, 5000 to 9999 treatment's; the
	// buckets were computed as golden's.
	for user, want := range map[string]Arm{
		"u1": {Name: "control", Mode: Retrieval}, // 4561
		"u4": {Name: "control", Mode: Retrieval}, // 1514
		"u2": {Name: "treatment", Mode: Ranker},  // 5518
		"u6": {Name: "treatment", Mode: Ranker},  // 8008
	} {
		if got := e.Assign(user); got.Name != want.Name || got.Mode != want.Mode {
			t.Errorf("Assign(%q) = %+v, want %+v", user, got, want)
		}
	}
	none, err := Parse("", "")
	if err != nil {
		t.Fatal(err)
	}
	if got := none.Assign("u1"); got.Name != "default" || got.Mode != Ranker {
		t.Errorf("without an experiment Assign = %+v, want the default arm with the ranker", got)
	}
}

func TestParseRejects(t *testing.T) {
	for _, c := range []struct{ name, arms string }{
		{"e", ""},
		{"", "a=ranker:100"},
		{"e", "a=ranker:50"},
		{"e", "a=ranker:50,a=retrieval:50"},
		{"e", "a=shuffle:100"},
		{"e", "a=ranker:0,b=ranker:100"},
		{"e", "a=ranker:x"},
		{"e", "a:100"},
		{"e", "=ranker:100"},
		{"e", "a=ranker:60,b=retrieval:60"},
	} {
		if _, err := Parse(c.name, c.arms); err == nil {
			t.Errorf("Parse(%q, %q) accepted", c.name, c.arms)
		}
	}
}
