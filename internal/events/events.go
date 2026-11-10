// Package events carries the events of POST /v1/events towards the user
// histories: straight into the store, or into a Kafka topic that the ingest
// consumer applies (docs/DECISIONS.md D034).
package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/sHyanorn/recserve/internal/history"
)

// Event is one user event, as a client sends it and as a record of the
// topic holds it.
type Event struct {
	UserID string `json:"user_id"`
	ItemID string `json:"item_id"`
	TS     int64  `json:"ts"` // milliseconds since the Unix epoch
	Kind   string `json:"kind"`
}

// Validate reports what makes the event unusable.
func (e Event) Validate() error {
	if e.UserID == "" || e.ItemID == "" || e.TS <= 0 || e.TS > history.MaxTS {
		return errors.New("user_id, item_id and a ts in 1..2^53 are required")
	}
	return nil
}

// History returns the event as the history store keeps it. Its ID is
// derived from the four fields, so an event that arrives twice is stored
// once.
func (e Event) History() history.Event {
	return history.Event{ID: history.EventID(e.UserID, e.ItemID, e.TS, e.Kind), User: e.UserID, Item: e.ItemID, TS: e.TS}
}

// Decode reads an event from the value of a record and rejects one that
// /v1/events would reject.
func Decode(value []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(value, &e); err != nil {
		return Event{}, fmt.Errorf("event record: %w", err)
	}
	if err := e.Validate(); err != nil {
		return Event{}, fmt.Errorf("event record: %w", err)
	}
	return e, nil
}

// Producer takes the events of one request.
type Producer interface {
	// Produce returns nil once every event is accepted, an error otherwise
	// or when ctx ends. After an error some events may have been accepted,
	// and one not acknowledged in time may still arrive; producing them
	// again is safe.
	Produce(ctx context.Context, events []Event) error
}

// Direct stores events in the history store before it returns, so the next
// request already sees them.
type Direct struct{ Store history.Store }

// Produce implements Producer.
func (d Direct) Produce(ctx context.Context, events []Event) error {
	evs := make([]history.Event, len(events))
	for i, e := range events {
		evs[i] = e.History()
	}
	return d.Store.Append(ctx, evs)
}
