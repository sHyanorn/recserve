package history

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"

	"github.com/valkey-io/valkey-go"
)

// Valkey is a Store on one Valkey server (docs/DECISIONS.md D033). A user's
// history is the sorted set h:<user> with one member per event, the event
// ID as 16 hex digits followed by the raw item ID, scored by the event
// time. The server orders members of equal score as byte strings, which for
// the fixed-width ID is the order by ID, so both stores order events alike.
// Nothing is trimmed and nothing expires.
type Valkey struct {
	c      valkey.Client
	prefix string
}

// idLen is the length of the event ID that starts a member.
const idLen = 16

// Limits of one Append: the events in one ZADD and the commands in one pipeline.
const (
	maxMembers  = 1000
	maxPipeline = 100
)

// NewValkey connects to the Valkey server at addr; it fails if the server
// does not answer.
func NewValkey(addr string) (*Valkey, error) {
	c, err := valkey.NewClient(valkey.ClientOption{
		InitAddress:       []string{addr},
		ForceSingleClient: true, // one server: no cluster probe
		DisableCache:      true, // no client-side caching, so no CLIENT TRACKING
		DisableRetry:      true, // a failed read is the caller's to handle; the client would retry until the context ends
	})
	if err != nil {
		return nil, fmt.Errorf("valkey %s: %w", addr, err)
	}
	return &Valkey{c: c, prefix: "h:"}, nil
}

// Close closes the connections to the server.
func (v *Valkey) Close() { v.c.Close() }

func member(e Event) string {
	var id [8]byte
	binary.BigEndian.PutUint64(id[:], e.ID)
	return hex.EncodeToString(id[:]) + e.Item
}

// Append implements Store. ZADD NX leaves a stored event as it is. The
// events are sent as several commands, not as a transaction: after an error
// some may be stored, and appending them again is safe.
func (v *Valkey) Append(ctx context.Context, events []Event) error {
	byUser := map[string][]Event{}
	for _, e := range events {
		if e.TS > MaxTS || e.TS < -MaxTS {
			return fmt.Errorf("event time %d is beyond ±2^53 ms", e.TS)
		}
		byUser[e.User] = append(byUser[e.User], e)
	}
	var cmds valkey.Commands
	flush := func() error {
		for _, r := range v.c.DoMulti(ctx, cmds...) {
			if err := r.Error(); err != nil {
				return fmt.Errorf("valkey zadd: %w", err)
			}
		}
		cmds = cmds[:0]
		return nil
	}
	for user, evs := range byUser {
		for chunk := range slices.Chunk(evs, maxMembers) {
			zadd := v.c.B().Zadd().Key(v.prefix + user).Nx().ScoreMember()
			for _, e := range chunk {
				zadd = zadd.ScoreMember(float64(e.TS), member(e))
			}
			if cmds = append(cmds, zadd.Build()); len(cmds) == maxPipeline {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
	return flush()
}

// Before implements Store.
func (v *Valkey) Before(ctx context.Context, user string, ts int64, n int) ([]string, error) {
	// Exclusive upper bound. Past MaxTS a float64 no longer tells times
	// apart, and every stored event is earlier.
	below := "(" + strconv.FormatInt(ts, 10)
	if ts > MaxTS {
		below = "+inf"
	}
	z := v.c.B().Zrange().Key(v.prefix + user)
	var cmd valkey.Completed
	if n > 0 { // the last n: read downwards, then turn around
		cmd = z.Min(below).Max("-inf").Byscore().Rev().Limit(0, int64(n)).Build()
	} else {
		cmd = z.Min("-inf").Max(below).Byscore().Build()
	}
	out, err := v.c.Do(ctx, cmd).AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("valkey zrange: %w", err)
	}
	if n > 0 {
		slices.Reverse(out)
	}
	for i, m := range out {
		if len(m) < idLen {
			return nil, fmt.Errorf("history of %q holds the malformed member %q", user, m)
		}
		out[i] = m[idLen:]
	}
	return out, nil
}
