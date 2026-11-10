// Package ingest applies the events topic to the user histories: a consumer
// group that commits a record's offset only after the history store holds
// its event (docs/DECISIONS.md D035).
package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/sHyanorn/recserve/internal/events"
	"github.com/sHyanorn/recserve/internal/history"
)

// Partitions is the partition count the topic is created with (D019).
const Partitions = 6

// Waits between attempts to apply records the store refused.
const (
	minBackoff = 100 * time.Millisecond
	maxBackoff = 5 * time.Second
)

// fetchErrorWait is the wait before the next poll after a fetch error. The
// client hands back an error it does not retry on every poll.
const fetchErrorWait = time.Second

// Config names what a Consumer reads and where it writes.
type Config struct {
	Brokers []string
	Group   string
	Topic   string
	Store   history.Store
	Log     *slog.Logger
	// SessionTimeout is how long the group waits for a member that stopped
	// without leaving before it moves the member's partitions; 0 for the
	// client's default of 45 s.
	SessionTimeout time.Duration
}

// Consumer is one member of the consumer group.
type Consumer struct {
	cl      *kgo.Client
	store   history.Store
	log     *slog.Logger
	backoff time.Duration // the first wait after a failed apply

	records  *prometheus.CounterVec
	failures prometheus.Counter

	// held is set while the offsets of the last poll must not be committed:
	// its batch is not applied yet.
	held atomic.Bool

	hook        func(point) bool // test only: called at each point; true stops the consumer there as a crash would
	commitFirst bool             // test only: the broken variant of property 9, which commits a batch before applying it
}

// point names a place in the loop where a test stops the consumer.
type point int

const (
	polled    point = iota // a batch is fetched, nothing of it applied
	midApply               // the batch's first partition is applied
	applied                // the whole batch is applied, not committed
	committed              // the batch's offsets are committed
)

var errCrashed = errors.New("ingest: stopped by a test hook")

// New creates the topic if it does not exist and joins the group. A group
// without committed offsets starts at the beginning of the topic.
func New(ctx context.Context, cfg Config) (*Consumer, error) {
	if err := ensureTopic(ctx, cfg.Brokers, cfg.Topic); err != nil {
		return nil, err
	}
	c := &Consumer{store: cfg.Store, log: cfg.Log, backoff: minBackoff}
	c.records = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "recserve_ingest_records_total", Help: "Records consumed, by result: applied to the history store, or poison (not a valid event, skipped).",
	}, []string{"result"})
	c.failures = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "recserve_ingest_apply_failures_total", Help: "Attempts to apply records that the history store refused.",
	})
	for _, r := range []string{"applied", "poison"} {
		c.records.WithLabelValues(r)
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		// No rebalance between a poll and the commit of its batch, so a
		// commit never covers a partition that has moved to another member.
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsRevoked(c.revoked),
	}
	if cfg.SessionTimeout > 0 {
		opts = append(opts, kgo.SessionTimeout(cfg.SessionTimeout), kgo.HeartbeatInterval(cfg.SessionTimeout/3))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	c.cl = cl
	return c, nil
}

// Collectors returns the consumer's metrics.
func (c *Consumer) Collectors() []prometheus.Collector {
	return []prometheus.Collector{c.records, c.failures}
}

// Close leaves the group. Polled offsets not yet committed are committed on
// the way out, unless the last batch was not applied: then none is.
func (c *Consumer) Close() { c.cl.CloseAllowingRebalance() }

// revoked commits what is applied before partitions move to another member
// or the consumer leaves: a commit that failed in the loop gets a second
// chance here.
func (c *Consumer) revoked(ctx context.Context, cl *kgo.Client, _ map[string][]int32) {
	if c.held.Load() {
		return
	}
	if err := cl.CommitUncommittedOffsets(ctx); err != nil {
		c.log.Warn("offset commit at revoke failed", "err", err)
	}
}

// Run consumes until ctx ends and returns its error. Each batch is applied
// to the store and then committed, so a consumer that stops in between
// leaves the batch to be delivered again; applying an event twice changes
// nothing (D033). Records the store refuses are retried until it takes them.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		fetches := c.cl.PollFetches(ctx)
		// A poll that returns records blocks rebalances before it notes
		// their offsets, until AllowRebalance: revoked never sees those
		// offsets without held.
		c.held.Store(true)
		if fetches.IsClientClosed() {
			return kgo.ErrClientClosed
		}
		failed := false
		if ctx.Err() == nil {
			fetches.EachError(func(topic string, partition int32, err error) {
				failed = true
				c.log.Warn("fetch failed", "topic", topic, "partition", partition, "err", err)
			})
		}
		if c.at(polled) {
			return errCrashed
		}
		if c.commitFirst && c.commit(ctx) {
			return errCrashed
		}
		first := true
		var err error
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if err != nil {
				return
			}
			if err = c.apply(ctx, c.decode(p.Records)); err == nil && first && c.at(midApply) {
				err = errCrashed
			}
			first = false
		})
		if err != nil {
			return err
		}
		c.held.Store(false)
		if c.at(applied) {
			return errCrashed
		}
		if !c.commitFirst && c.commit(ctx) {
			return errCrashed
		}
		c.cl.AllowRebalance()
		// Checked here, not after the poll: a poll that returns records as
		// ctx ends has moved the offsets, and its batch is applied first.
		if err := ctx.Err(); err != nil {
			return err
		}
		if failed {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(fetchErrorWait):
			}
		}
	}
}

// at reports whether a test stops the consumer at p. A consumer stopped
// there commits nothing more, as if its process had died.
func (c *Consumer) at(p point) bool {
	if c.hook == nil || !c.hook(p) {
		return false
	}
	c.held.Store(true)
	return true
}

// commit commits the offsets of everything polled and reports whether a
// test stops the consumer after it. A failed commit is retried by the next
// one, which covers it.
func (c *Consumer) commit(ctx context.Context) (crashed bool) {
	if err := c.cl.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
		c.log.Warn("offset commit failed", "err", err)
	}
	return c.at(committed)
}

// decode returns the events of the records. A record that is not a valid
// event is counted and left out: it will never decode, and waiting on it
// would stop its partition for good. The log names its place, not its
// content.
func (c *Consumer) decode(records []*kgo.Record) []history.Event {
	evs := make([]history.Event, 0, len(records))
	for _, r := range records {
		e, err := events.Decode(r.Value)
		if err != nil {
			c.records.WithLabelValues("poison").Inc()
			c.log.Warn("record skipped: not a valid event", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "bytes", len(r.Value))
			continue
		}
		evs = append(evs, e.History())
	}
	return evs
}

// apply appends the events of one partition to the store, trying again
// after a growing wait until it succeeds or ctx ends.
func (c *Consumer) apply(ctx context.Context, evs []history.Event) error {
	if len(evs) == 0 {
		return nil
	}
	wait := c.backoff
	for {
		err := c.store.Append(ctx, evs)
		if err == nil {
			c.records.WithLabelValues("applied").Add(float64(len(evs)))
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.failures.Inc()
		c.log.Warn("history store refused a batch; retrying", "events", len(evs), "wait", wait, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		wait = min(2*wait, maxBackoff)
	}
}

// ensureTopic creates the topic with Partitions partitions and the broker's
// default replication factor. A topic that exists is left as it is.
func ensureTopic(ctx context.Context, brokers []string, topic string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer cl.Close()
	req := kmsg.NewPtrCreateTopicsRequest()
	req.TimeoutMillis = 15_000
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = topic, Partitions, -1
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err == nil {
		if len(resp.Topics) != 1 {
			return fmt.Errorf("creating topic %s: %d topics in the response", topic, len(resp.Topics))
		}
		err = kerr.ErrorForCode(resp.Topics[0].ErrorCode)
	}
	if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("creating topic %s: %w", topic, err)
	}
	return nil
}
