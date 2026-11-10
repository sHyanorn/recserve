package history

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"

	"github.com/valkey-io/valkey-go"

	"github.com/sHyanorn/recserve/internal/valkeyclient"
)

// Valkey is a Store on one Valkey server (docs/DECISIONS.md D033). A user's
// history is the sorted set h:<user> with one member per event, the event
// ID as 16 hex digits followed by the raw item ID, scored by the event
// time. The server orders members of equal score as byte strings, which for
// the fixed-width ID is the order by ID, so both stores order events alike.
// Nothing is trimmed and nothing expires.
type Valkey struct {
	c      *valkeyclient.Client
	prefix string
}

// idLen is the length of the event ID that starts a member.
const idLen = 16

// Limits of one Append: the events in one ZADD and the commands in one pipeline.
const (
	maxMembers  = 1000
	maxPipeline = 100
)

// NewValkey returns the store on the server c is connected to.
func NewValkey(c *valkeyclient.Client) *Valkey { return &Valkey{c: c, prefix: "h:"} }

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
	// One ZADD per user and chunk, pipelined; the commands are built for
	// each attempt, since a command is sent once.
	type zadd struct {
		key    string
		events []Event
	}
	var zadds []zadd
	for user, evs := range byUser {
		for chunk := range slices.Chunk(evs, maxMembers) {
			zadds = append(zadds, zadd{v.prefix + user, chunk})
		}
	}
	for batch := range slices.Chunk(zadds, maxPipeline) {
		err := v.c.Do(ctx, func(c valkey.Client) error {
			cmds := make(valkey.Commands, len(batch))
			for i, z := range batch {
				cmd := c.B().Zadd().Key(z.key).Nx().ScoreMember()
				for _, e := range z.events {
					cmd = cmd.ScoreMember(float64(e.TS), member(e))
				}
				cmds[i] = cmd.Build()
			}
			for _, r := range c.DoMulti(ctx, cmds...) {
				if err := r.Error(); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("valkey zadd: %w", err)
		}
	}
	return nil
}

// Before implements Store.
func (v *Valkey) Before(ctx context.Context, user string, ts int64, n int) ([]string, error) {
	// Exclusive upper bound. Past MaxTS a float64 no longer tells times
	// apart, and every stored event is earlier.
	below := "(" + strconv.FormatInt(ts, 10)
	if ts > MaxTS {
		below = "+inf"
	}
	var out []string
	err := v.c.Do(ctx, func(c valkey.Client) (err error) {
		z := c.B().Zrange().Key(v.prefix + user)
		var cmd valkey.Completed
		if n > 0 { // the last n: read downwards, then turn around
			cmd = z.Min(below).Max("-inf").Byscore().Rev().Limit(0, int64(n)).Build()
		} else {
			cmd = z.Min("-inf").Max(below).Byscore().Build()
		}
		out, err = c.Do(ctx, cmd).AsStrSlice()
		return err
	})
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
