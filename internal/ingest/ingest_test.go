package ingest

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/sHyanorn/recserve/internal/events"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/infratest"
)

func newConsumer(t *testing.T, brokers []string, topic string, store history.Store) *Consumer {
	t.Helper()
	c, err := New(context.Background(), Config{Brokers: brokers, Group: "ingest-" + topic, Topic: topic, Store: store, Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	c.backoff = time.Millisecond
	return c
}

// start runs c and returns the function that stops it and leaves the group.
func start(c *Consumer) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Run(ctx) //nolint:errcheck // the context's error
	}()
	return func() {
		cancel()
		<-done
		c.Close()
	}
}

// record returns e as a record of the topic. A user's records go to one
// partition, as the gateway's producer sends them.
func record(topic string, e events.Event, value []byte) *kgo.Record {
	h := fnv.New32a()
	h.Write([]byte(e.UserID))
	return &kgo.Record{Topic: topic, Partition: int32(h.Sum32() % Partitions), Key: []byte(e.UserID), Value: value}
}

func produce(t *testing.T, brokers []string, records ...*kgo.Record) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := cl.ProduceSync(context.Background(), records...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func produceEvents(t *testing.T, brokers []string, topic string, evs []events.Event) {
	t.Helper()
	records := make([]*kgo.Record, len(evs))
	for i, e := range evs {
		records[i] = record(topic, e, fmt.Appendf(nil, `{"user_id":%q,"item_id":%q,"ts":%d,"kind":%q}`, e.UserID, e.ItemID, e.TS, e.Kind))
	}
	produce(t, brokers, records...)
}

// drained produces a last event to every partition and waits until the
// store holds them all. A partition is applied in order, so by then the
// group has been through every record produced before.
func drained(t *testing.T, brokers []string, topic string, store history.Store) {
	t.Helper()
	tag := fmt.Sprintf("end-%016x-", rand.Uint64())
	records := make([]*kgo.Record, Partitions)
	for p := range records {
		records[p] = &kgo.Record{Topic: topic, Partition: int32(p), Value: fmt.Appendf(nil, `{"user_id":"%s%d","item_id":"x","ts":1,"kind":"end"}`, tag, p)}
	}
	produce(t, brokers, records...)
	deadline := time.Now().Add(60 * time.Second)
	for p := 0; p < Partitions; {
		items, err := store.Before(context.Background(), tag+strconv.Itoa(p), 2, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(items) == 1 {
			p++
			continue
		}
		if time.Now().After(deadline) {
			t.Fatalf("the last event of partition %d was not applied within 60s", p)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// workload draws events of a few users over a few items and times, so that
// events share a time and arrive out of time order.
type workload struct {
	r     *rand.Rand
	users []string
	all   []events.Event // every distinct event drawn
}

func newWorkload(seed uint64) *workload {
	w := &workload{r: rand.New(rand.NewPCG(seed, 0))}
	prefix := fmt.Sprintf("u%016x-", rand.Uint64()) // a shared Valkey holds other runs' users
	for i := range 8 {
		w.users = append(w.users, prefix+strconv.Itoa(i))
	}
	return w
}

// draw returns n events; about one in four repeats an earlier one.
func (w *workload) draw(n int) []events.Event {
	out := make([]events.Event, n)
	for i := range out {
		if len(w.all) > 0 && w.r.IntN(4) == 0 {
			out[i] = w.all[w.r.IntN(len(w.all))]
			continue
		}
		out[i] = events.Event{UserID: w.users[w.r.IntN(len(w.users))], ItemID: "i" + strconv.Itoa(w.r.IntN(30)), TS: 1 + w.r.Int64N(50), Kind: "view"}
		w.all = append(w.all, out[i])
	}
	return out
}

// diff compares the store with one that holds every drawn event exactly
// once, and describes the first user whose history differs.
func (w *workload) diff(t *testing.T, store history.Store) string {
	t.Helper()
	ctx := context.Background()
	want := history.NewMemory()
	for _, e := range w.all {
		if err := want.Append(ctx, []history.Event{e.History()}); err != nil {
			t.Fatal(err)
		}
	}
	for _, u := range w.users {
		for _, q := range [][2]int64{{history.MaxTS + 1, 0}, {25, 0}, {40, 3}} {
			got, err := store.Before(ctx, u, q[0], int(q[1]))
			if err != nil {
				t.Fatal(err)
			}
			exp, err := want.Before(ctx, u, q[0], int(q[1]))
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, exp) {
				return fmt.Sprintf("user %s before %d, last %d: %d items, want %d: %v, want %v", u, q[0], q[1], len(got), len(exp), got, exp)
			}
		}
	}
	return ""
}

// crashes runs the group through a series of consumers, each stopped at a
// random point of its loop as a crash would stop it, while events keep
// arriving, and then lets a last consumer catch up. It returns how the
// histories differ from every event applied exactly once.
func crashes(t *testing.T, brokers []string, topic string, store history.Store, seed uint64, consumers int, commitFirst bool) string {
	t.Helper()
	if err := ensureTopic(context.Background(), brokers, topic); err != nil {
		t.Fatal(err)
	}
	w := newWorkload(seed)
	for range consumers {
		produceEvents(t, brokers, topic, w.draw(1+w.r.IntN(20)))
		stopAt := point(w.r.IntN(4))
		c := newConsumer(t, brokers, topic, store)
		c.commitFirst = commitFirst
		c.hook = func(p point) bool { return p == stopAt }
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		err := c.Run(ctx)
		cancel()
		c.Close()
		if !errors.Is(err, errCrashed) {
			t.Fatalf("seed %d: a consumer due to stop at point %d returned %v", seed, stopAt, err)
		}
	}
	c := newConsumer(t, brokers, topic, store)
	c.commitFirst = commitFirst
	stop := start(c)
	drained(t, brokers, topic, store)
	stop()
	return w.diff(t, store)
}

func fake(t *testing.T) []string {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	return cluster.ListenAddrs()
}

func valkeyStore(t *testing.T) *history.Valkey {
	t.Helper()
	addr := infratest.ValkeyAddr(t)
	return history.NewValkey(infratest.ValkeyClient(t, addr))
}

// Property 9: after the consumer is stopped at random points and restarted,
// every history equals the one built by applying each event exactly once.
func TestCrashedConsumersApplyEveryEventOnce(t *testing.T) {
	t.Run("kfake", func(t *testing.T) {
		brokers := fake(t)
		for seed := range uint64(4) {
			if d := crashes(t, brokers, infratest.TopicName(), history.NewMemory(), seed, 40, false); d != "" {
				t.Errorf("seed %d: %s", seed, d)
			}
		}
	})
	t.Run("kafka", func(t *testing.T) {
		brokers, store, topic := infratest.KafkaBrokers(t), valkeyStore(t), infratest.TopicName()
		infratest.CreateTopic(t, topic, Partitions)
		if d := crashes(t, brokers, topic, store, 1, 30, false); d != "" {
			t.Error(d)
		}
	})
}

// The broken variant commits a batch before applying it, so a consumer
// stopped in between loses the batch.
func TestCommitBeforeApplyLosesEvents(t *testing.T) {
	if d := crashes(t, fake(t), infratest.TopicName(), history.NewMemory(), 0, 40, true); d == "" {
		t.Fatal("the broken variant passed")
	}
}

// Property 8 through the topic: events produced by the gateway's producer
// with repeats and in shuffled order leave the histories of each event
// delivered once.
func TestRepeatedAndReorderedEventsThroughTheTopic(t *testing.T) {
	brokers, store, topic := infratest.KafkaBrokers(t), valkeyStore(t), infratest.TopicName()
	infratest.CreateTopic(t, topic, Partitions)
	w := newWorkload(1)
	deliveries := w.draw(400)
	for _, e := range w.all {
		for range w.r.IntN(3) {
			deliveries = append(deliveries, e)
		}
	}
	w.r.Shuffle(len(deliveries), func(i, j int) { deliveries[i], deliveries[j] = deliveries[j], deliveries[i] })

	stop := start(newConsumer(t, brokers, topic, store))
	k, err := events.NewKafka(brokers, topic, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	for len(deliveries) > 0 {
		n := min(1+w.r.IntN(40), len(deliveries))
		if err := k.Produce(context.Background(), deliveries[:n]); err != nil {
			t.Fatal(err)
		}
		deliveries = deliveries[n:]
	}
	drained(t, brokers, topic, store)
	stop()
	if d := w.diff(t, store); d != "" {
		t.Error(d)
	}
}

// A member joins and another leaves while events arrive; in the end every
// event is applied.
func TestMembersJoinAndLeave(t *testing.T) {
	brokers, store, topic := infratest.KafkaBrokers(t), history.NewMemory(), infratest.TopicName()
	infratest.CreateTopic(t, topic, Partitions)
	w := newWorkload(1)
	stopA := start(newConsumer(t, brokers, topic, store))
	produceEvents(t, brokers, topic, w.draw(200))
	b := newConsumer(t, brokers, topic, store)
	stopB := start(b)
	// Until the new member has applied events, so partitions have moved to it.
	for deadline := time.Now().Add(60 * time.Second); testutil.ToFloat64(b.records.WithLabelValues("applied")) == 0; time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the second member applied no event within 60s")
		}
		produceEvents(t, brokers, topic, w.draw(20))
	}
	stopA()
	produceEvents(t, brokers, topic, w.draw(200))
	drained(t, brokers, topic, store)
	stopB()
	if d := w.diff(t, store); d != "" {
		t.Error(d)
	}
}

// A record that is not a valid event is counted, skipped and committed: the
// events after it are applied, and the next member does not get it again.
func TestPoisonRecordsAreSkipped(t *testing.T) {
	brokers, store, topic := infratest.KafkaBrokers(t), history.NewMemory(), infratest.TopicName()
	infratest.CreateTopic(t, topic, Partitions)
	w := newWorkload(1)
	evs := []events.Event{{UserID: w.users[0], ItemID: "i1", TS: 5, Kind: "view"}, {UserID: w.users[0], ItemID: "i2", TS: 3, Kind: "view"}}
	w.all = evs
	bad := events.Event{UserID: w.users[0]}
	produceEvents(t, brokers, topic, evs[:1])
	produce(t, brokers, record(topic, bad, []byte("not JSON")), record(topic, bad, []byte(`{"user_id":"","item_id":"i1","ts":1,"kind":"view"}`)))
	produceEvents(t, brokers, topic, evs[1:])

	c := newConsumer(t, brokers, topic, store)
	stop := start(c)
	drained(t, brokers, topic, store)
	stop()
	if d := w.diff(t, store); d != "" {
		t.Error(d)
	}
	if n := testutil.ToFloat64(c.records.WithLabelValues("poison")); n != 2 {
		t.Errorf("%v poison records counted, want 2", n)
	}

	next := newConsumer(t, brokers, topic, history.NewMemory())
	stop = start(next)
	drained(t, brokers, topic, next.store)
	stop()
	if n := testutil.ToFloat64(next.records.WithLabelValues("poison")); n != 0 {
		t.Errorf("the next member got %v poison records again", n)
	}
}

// refusing is a store that refuses appends while fail is set, as Valkey
// does once its memory is full.
type refusing struct {
	history.Store
	fail    atomic.Bool
	refused atomic.Int64
}

func (s *refusing) Append(ctx context.Context, evs []history.Event) error {
	if s.fail.Load() {
		s.refused.Add(1)
		return errors.New("OOM command not allowed when used memory > 'maxmemory'")
	}
	return s.Store.Append(ctx, evs)
}

func (s *refusing) waitRefused(t *testing.T, n int64) {
	t.Helper()
	for deadline := time.Now().Add(60 * time.Second); s.refused.Load() < n; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%d appends refused within 60s, want %d", s.refused.Load(), n)
		}
	}
}

// A batch the store refuses is tried again and its offsets stay
// uncommitted, also when the consumer shuts down holding it: the next
// member gets the batch and applies it once the store takes writes.
func TestRefusedBatchIsRetriedAndNotCommitted(t *testing.T) {
	brokers, topic := infratest.KafkaBrokers(t), infratest.TopicName()
	infratest.CreateTopic(t, topic, Partitions)
	store := &refusing{Store: history.NewMemory()}
	store.fail.Store(true)
	w := newWorkload(1)
	produceEvents(t, brokers, topic, w.draw(100))

	stop := start(newConsumer(t, brokers, topic, store))
	store.waitRefused(t, 3)
	stop()

	stop = start(newConsumer(t, brokers, topic, store))
	store.waitRefused(t, store.refused.Load()+3)
	store.fail.Store(false)
	drained(t, brokers, topic, store)
	stop()
	if d := w.diff(t, store); d != "" {
		t.Error(d)
	}
}

// New creates the topic with its partitions, and leaves an existing one.
func TestNewCreatesTheTopic(t *testing.T) {
	brokers, topic := infratest.KafkaBrokers(t), infratest.TopicName()
	infratest.DeleteTopic(t, topic)
	for range 2 {
		newConsumer(t, brokers, topic, history.NewMemory()).Close()
	}
	produce(t, brokers, &kgo.Record{Topic: topic, Partition: Partitions - 1, Value: []byte("x")})
}
