package events

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	"github.com/sHyanorn/recserve/internal/history"
)

func TestRecordRoundTrip(t *testing.T) {
	for _, e := range []Event{
		{UserID: "u", ItemID: "i", TS: 1, Kind: "view"},
		{UserID: `<u&"'\>`, ItemID: "物品  \x00", TS: history.MaxTS},
	} {
		v, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := Decode(v); err != nil || got != e {
			t.Fatalf("Decode(%s) = %+v, %v", v, got, err)
		}
	}
	for _, v := range []string{
		``, `{`, `[]`,
		`{"user_id":"","item_id":"i","ts":1}`,
		`{"user_id":"u","item_id":"","ts":1}`,
		`{"user_id":"u","item_id":"i","ts":0}`,
		`{"user_id":"u","item_id":"i","ts":9007199254740993}`,
	} {
		if e, err := Decode([]byte(v)); err == nil {
			t.Fatalf("Decode(%s) = %+v", v, e)
		}
	}
}

// Direct stores before it returns, and an event sent twice is stored once.
func TestDirect(t *testing.T) {
	store, ctx := history.NewMemory(), context.Background()
	e := Event{UserID: "u", ItemID: "a", TS: 5, Kind: "view"}
	for range 2 {
		if err := (Direct{store}).Produce(ctx, []Event{e, {UserID: "u", ItemID: "b", TS: 6}}); err != nil {
			t.Fatal(err)
		}
		if got, err := store.Before(ctx, "u", 7, 0); err != nil || !slices.Equal(got, []string{"a", "b"}) {
			t.Fatalf("history %v, %v", got, err)
		}
	}
}
