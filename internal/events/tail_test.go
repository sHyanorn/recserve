package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func fakeCluster(t *testing.T, topic string, partitions int32) []string {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(partitions, topic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	return cluster.ListenAddrs()
}

func produceValues(t *testing.T, brokers []string, topic string, values ...string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.DefaultProduceTopic(topic))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	records := make([]*kgo.Record, len(values))
	for i, v := range values {
		records[i] = &kgo.Record{Key: fmt.Appendf(nil, "u%d", i), Value: []byte(v)}
	}
	if err := cl.ProduceSync(context.Background(), records...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func eventValue(user string, ts int64) string {
	return fmt.Sprintf(`{"user_id":%q,"item_id":"i","ts":%d,"kind":"rating"}`, user, ts)
}

// A Tail reads every partition from where each ended when it was made:
// records produced before are not read, every one produced after is, those
// that hold no valid event are counted apart, and once all are applied no
// record is left behind.
func TestTailReadsFromTheEnd(t *testing.T) {
	const topic = "events"
	brokers := fakeCluster(t, topic, 3)
	var before []string
	for i := range 30 {
		before = append(before, eventValue(fmt.Sprint("before", i), 1))
	}
	produceValues(t, brokers, topic, before...)
	tail, err := NewTail(context.Background(), brokers, topic, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer tail.Close()
	var after []string
	var want []string
	for i := range 30 {
		u := fmt.Sprint("after", i)
		after, want = append(after, eventValue(u, 2)), append(want, u)
	}
	produceValues(t, brokers, topic, append(after, "not an event", `{"user_id":"x"}`)...)

	ctx, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	var got []string
	invalid := 0
	done := make(chan error, 1)
	go func() {
		done <- tail.Run(ctx, func(evs []Event, bad int) {
			mu.Lock()
			defer mu.Unlock()
			for _, e := range evs {
				got = append(got, e.UserID)
			}
			invalid += bad
		})
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		n, bad := len(got), invalid
		mu.Unlock()
		if n+bad == len(want)+2 && tail.Lag() == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("read %d events and %d invalid records, lag %d; want %d, 2 and 0", n, bad, tail.Lag(), len(want))
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("Run returned %v, want context.Canceled", err)
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("read %v, want %v", got, want)
	}
}

// NewTail waits for a topic that does not exist yet, until its context
// ends.
func TestTailWaitsForTheTopic(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	brokers := cluster.ListenAddrs()
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	if _, err := NewTail(ctx, brokers, "missing", slog.New(slog.DiscardHandler)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("NewTail on a missing topic: %v, want the context's end", err)
	}
	created := make(chan error, 1)
	go func() {
		time.Sleep(1500 * time.Millisecond)
		cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
		if err != nil {
			created <- err
			return
		}
		defer cl.Close()
		req := kmsg.NewPtrCreateTopicsRequest()
		rt := kmsg.NewCreateTopicsRequestTopic()
		rt.Topic, rt.NumPartitions, rt.ReplicationFactor = "late", 2, 1
		req.Topics = append(req.Topics, rt)
		_, err = req.RequestWith(context.Background(), cl)
		created <- err
	}()
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tail, err := NewTail(ctx, brokers, "late", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewTail on a topic created after it started: %v", err)
	}
	tail.Close()
	if err := <-created; err != nil {
		t.Fatal(err)
	}
}
