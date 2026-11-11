// Package experiment assigns users to the arms of an A/B experiment
// (docs/SPEC.md §6, D049).
package experiment

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Mode is how an arm orders the candidates.
type Mode string

// The modes of an arm.
const (
	Ranker    Mode = "ranker"    // by the bundle's ranker, if it has one
	Retrieval Mode = "retrieval" // by retrieval score; the ranker is not run
)

// Buckets is the number of buckets users are hashed into; an arm of weight
// w percent holds 100w of them.
const Buckets = 10_000

// Arm is one arm of an experiment.
type Arm struct {
	Name string
	Mode Mode
	upto int // the arm holds the buckets below upto that no earlier arm holds
}

// Experiment splits users between arms by a hash of the experiment's name
// and the user ID, so a user stays in one arm across processes and restarts
// for as long as the name and the arms stay the same.
type Experiment struct {
	Name string // empty without an experiment
	Arms []Arm
}

// Default is the one arm without an experiment: every request is ranked.
var Default = Arm{Name: "default", Mode: Ranker, upto: Buckets}

// None returns no experiment: every user is in Default.
func None() *Experiment { return &Experiment{Arms: []Arm{Default}} }

// Parse returns the experiment named name with the arms given as
// "name=mode:weight,...": weights are whole percentages summing to 100, and
// the arms take consecutive buckets in the order listed. Without a name
// and arms, every user is in Default.
func Parse(name, arms string) (*Experiment, error) {
	if name == "" && arms == "" {
		return None(), nil
	}
	if name == "" || arms == "" {
		return nil, errors.New("an experiment needs both a name and arms")
	}
	e := &Experiment{Name: name}
	seen := map[string]bool{}
	total := 0
	for spec := range strings.SplitSeq(arms, ",") {
		armName, rest, ok1 := strings.Cut(spec, "=")
		mode, weight, ok2 := strings.Cut(rest, ":")
		w, err := strconv.Atoi(weight)
		switch {
		case !ok1 || !ok2 || armName == "" || err != nil:
			return nil, fmt.Errorf("arm %q: want name=mode:weight", spec)
		case seen[armName]:
			return nil, fmt.Errorf("arm %q named twice", armName)
		case Mode(mode) != Ranker && Mode(mode) != Retrieval:
			return nil, fmt.Errorf("arm %q: mode %q, want %q or %q", armName, mode, Ranker, Retrieval)
		case w < 1 || w > 100:
			return nil, fmt.Errorf("arm %q: weight %d, want 1 to 100", armName, w)
		}
		seen[armName] = true
		total += w
		e.Arms = append(e.Arms, Arm{Name: armName, Mode: Mode(mode), upto: total * Buckets / 100})
	}
	if total != 100 {
		return nil, fmt.Errorf("arm weights sum to %d, want 100", total)
	}
	return e, nil
}

// Bucket returns the bucket of user in the experiment named name: the
// first 8 bytes of the SHA-256 of name, ".", user, as a big-endian unsigned
// integer, mod Buckets.
func Bucket(name, user string) int {
	sum := sha256.Sum256([]byte(name + "." + user))
	return int(binary.BigEndian.Uint64(sum[:8]) % Buckets)
}

// Assign returns user's arm.
func (e *Experiment) Assign(user string) Arm {
	if len(e.Arms) == 1 {
		return e.Arms[0]
	}
	return e.arm(Bucket(e.Name, user))
}

// arm returns the arm that holds bucket b.
func (e *Experiment) arm(b int) Arm {
	for _, a := range e.Arms {
		if b < a.upto {
			return a
		}
	}
	return e.Arms[len(e.Arms)-1] // unreachable: the last arm holds every bucket left
}
