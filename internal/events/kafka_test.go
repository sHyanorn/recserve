package events

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/infratest"
)

func newKafka(t *testing.T, brokers []string, topic string, timeout time.Duration) *Kafka {
	t.Helper()
	k, err := NewKafka(brokers, topic, timeout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(k.Close)
	return k
}

// Every record of a user is in one partition, in the order produced, also
// within one request; the users spread over the partitions.
func TestKafkaKeysByUserAndKeepsOrder(t *testing.T) {
	brokers, topic := infratest.KafkaBrokers(t), infratest.TopicName()
	infratest.CreateTopic(t, topic, 4)
	k := newKafka(t, brokers, topic, 10*time.Second)
	r := rand.New(rand.NewPCG(1, 0))
	const users, total = 20, 600
	sent := map[string][]int64{}
	for n := 0; n < total; {
		var batch []Event
		for range 1 + r.IntN(8) {
			n++
			u := fmt.Sprint("u", r.IntN(users))
			batch = append(batch, Event{UserID: u, ItemID: "i", TS: int64(n)})
			sent[u] = append(sent[u], int64(n))
		}
		if err := k.Produce(context.Background(), batch); err != nil {
			t.Fatal(err)
		}
	}
	got, partition, used := map[string][]int64{}, map[string]int32{}, map[int32]bool{}
	for _, rec := range infratest.Records(t, topic, len(sentTotal(sent))) {
		e, err := Decode(rec.Value)
		if err != nil || string(rec.Key) != e.UserID {
			t.Fatalf("record key %q, value %q: %v", rec.Key, rec.Value, err)
		}
		if p, ok := partition[e.UserID]; ok && p != rec.Partition {
			t.Fatalf("user %s is in partitions %d and %d", e.UserID, p, rec.Partition)
		}
		partition[e.UserID], used[rec.Partition] = rec.Partition, true
		got[e.UserID] = append(got[e.UserID], e.TS)
	}
	for u, want := range sent {
		if !slices.Equal(got[u], want) {
			t.Fatalf("user %s: read %v, produced %v", u, got[u], want)
		}
	}
	if len(used) < 2 {
		t.Fatalf("%d users are all in partitions %v", users, used)
	}
}

func sentTotal(sent map[string][]int64) []int64 {
	var all []int64
	for _, s := range sent {
		all = append(all, s...)
	}
	return all
}

// An event produced twice is two records, and one event in a history that
// both are applied to.
func TestKafkaSameEventTwice(t *testing.T) {
	brokers, topic := infratest.KafkaBrokers(t), infratest.TopicName()
	infratest.CreateTopic(t, topic, 2)
	k := newKafka(t, brokers, topic, 10*time.Second)
	e, ctx := Event{UserID: "u", ItemID: "a", TS: 5, Kind: "view"}, context.Background()
	for range 2 {
		if err := k.Produce(ctx, []Event{e}); err != nil {
			t.Fatal(err)
		}
	}
	store := history.NewMemory()
	for _, rec := range infratest.Records(t, topic, 2) {
		got, err := Decode(rec.Value)
		if err != nil || got != e {
			t.Fatalf("record %q: %+v, %v", rec.Value, got, err)
		}
		if err := (Direct{store}).Produce(ctx, []Event{got}); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := store.Before(ctx, "u", 6, 0); err != nil || !slices.Equal(got, []string{"a"}) {
		t.Fatalf("history %v, %v", got, err)
	}
}

// silentServer accepts connections and never answers.
func silentServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var conns []net.Conn
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			conns = append(conns, c)
		}
	}()
	t.Cleanup(func() {
		if err := ln.Close(); err != nil {
			t.Error(err)
		}
		<-done
		for _, c := range conns {
			if err := c.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	return ln.Addr().String()
}

// Without a broker that takes the record, Produce fails: by itself once the
// record has waited for the producer's timeout, and at the end of the
// context if that comes first.
func TestKafkaFailsInBoundedTime(t *testing.T) {
	for name, brokers := range map[string]func(t *testing.T) []string{
		"closed port":   func(*testing.T) []string { return []string{"127.0.0.1:1"} },
		"silent server": func(t *testing.T) []string { return []string{silentServer(t)} },
		"missing topic": func(t *testing.T) []string { return infratest.KafkaBrokers(t) },
	} {
		t.Run(name, func(t *testing.T) {
			brokers, e := brokers(t), []Event{{UserID: "u", ItemID: "a", TS: 5}}

			start := time.Now()
			err := newKafka(t, brokers, infratest.TopicName(), time.Second).Produce(context.Background(), e)
			if took := time.Since(start); err == nil || took > 5*time.Second {
				t.Fatalf("producer timeout: %v after %v", err, took)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			start = time.Now()
			err = newKafka(t, brokers, infratest.TopicName(), time.Minute).Produce(ctx, e)
			if took := time.Since(start); !errors.Is(err, context.DeadlineExceeded) || took > 5*time.Second {
				t.Fatalf("context deadline: %v after %v", err, took)
			}
		})
	}
}

func TestKafkaRefusesShortTimeout(t *testing.T) {
	if k, err := NewKafka([]string{"127.0.0.1:1"}, "t", 999*time.Millisecond); err == nil {
		k.Close()
		t.Fatal("a timeout below the client's minimum was accepted")
	}
}
