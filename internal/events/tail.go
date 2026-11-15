package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// tailErrorWait is the wait before the next poll after a fetch error. The
// client hands back an error it does not retry on every poll.
const tailErrorWait = time.Second

// topicWait is the wait before NewTail asks again for a topic that does not
// exist yet.
const topicWait = time.Second

// Tail reads every partition of a topic from the offsets that ended it
// when the Tail was made, outside any consumer group and committing
// nothing, so each reader gets every event produced after its start
// (D062).
type Tail struct {
	cl  *kgo.Client
	log *slog.Logger
	lag atomic.Int64
}

// NewTail lists the end offsets of the topic's partitions and returns a
// Tail that reads from them. It waits for the topic to exist until ctx
// ends.
func NewTail(ctx context.Context, brokers []string, topic string, log *slog.Logger) (*Tail, error) {
	ends, err := endOffsets(ctx, brokers, topic)
	if err != nil {
		return nil, err
	}
	offsets := make(map[int32]kgo.Offset, len(ends))
	for p, o := range ends {
		offsets[p] = kgo.NewOffset().At(o)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: offsets}))
	if err != nil {
		return nil, err
	}
	return &Tail{cl: cl, log: log}, nil
}

// endOffsets returns the offset after the last record of each of the
// topic's partitions.
func endOffsets(ctx context.Context, brokers []string, topic string) (map[int32]int64, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	var mresp *kmsg.MetadataResponse
	for {
		meta := kmsg.NewPtrMetadataRequest()
		mt := kmsg.NewMetadataRequestTopic()
		mt.Topic = kmsg.StringPtr(topic)
		meta.Topics = append(meta.Topics, mt)
		if mresp, err = meta.RequestWith(ctx, cl); err != nil {
			return nil, fmt.Errorf("topic %s: %w", topic, err)
		}
		if len(mresp.Topics) != 1 {
			return nil, fmt.Errorf("topic %s: metadata names %d topics", topic, len(mresp.Topics))
		}
		err := kerr.ErrorForCode(mresp.Topics[0].ErrorCode)
		if err == nil {
			break
		}
		if !errors.Is(err, kerr.UnknownTopicOrPartition) {
			return nil, fmt.Errorf("topic %s: %w", topic, err)
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("topic %s: %w", topic, errors.Join(err, ctx.Err()))
		case <-time.After(topicWait):
		}
	}
	list := kmsg.NewPtrListOffsetsRequest()
	list.ReplicaID = -1
	lt := kmsg.NewListOffsetsRequestTopic()
	lt.Topic = topic
	for _, p := range mresp.Topics[0].Partitions {
		lp := kmsg.NewListOffsetsRequestTopicPartition()
		lp.Partition, lp.Timestamp = p.Partition, -1 // -1: the end
		lt.Partitions = append(lt.Partitions, lp)
	}
	list.Topics = append(list.Topics, lt)
	lresp, err := list.RequestWith(ctx, cl)
	if err != nil {
		return nil, fmt.Errorf("topic %s: %w", topic, err)
	}
	ends := map[int32]int64{}
	for _, t := range lresp.Topics {
		for _, p := range t.Partitions {
			if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
				return nil, fmt.Errorf("topic %s, partition %d: %w", topic, p.Partition, err)
			}
			ends[p.Partition] = p.Offset
		}
	}
	if len(ends) != len(mresp.Topics[0].Partitions) {
		return nil, fmt.Errorf("topic %s: end offsets of %d of its %d partitions", topic, len(ends), len(mresp.Topics[0].Partitions))
	}
	return ends, nil
}

// Run reads until ctx ends and returns its error. It gives apply the
// valid events of each fetch, in each partition's order, and the number
// of records that held none, which it leaves out.
func (t *Tail) Run(ctx context.Context, apply func(events []Event, poison int)) error {
	behind := map[int32]int64{}
	for {
		fetches := t.cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}
		failed := false
		fetches.EachError(func(topic string, partition int32, err error) {
			failed = true
			t.log.Warn("fetch failed", "topic", topic, "partition", partition, "err", err)
		})
		var evs []Event
		invalid := 0
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			for _, r := range p.Records {
				e, err := Decode(r.Value)
				if err != nil {
					invalid++
					continue
				}
				evs = append(evs, e)
			}
			if n := len(p.Records); n > 0 {
				behind[p.Partition] = p.HighWatermark - (p.Records[n-1].Offset + 1)
			}
		})
		if len(evs) > 0 || invalid > 0 {
			apply(evs, invalid)
		}
		var lag int64
		for _, n := range behind {
			lag += n
		}
		t.lag.Store(lag)
		if failed {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(tailErrorWait):
			}
		}
	}
}

// Lag returns the records produced but not yet read and applied, summed
// over the partitions, as of each partition's last fetch that returned
// records.
func (t *Tail) Lag() int64 { return t.lag.Load() }

// Close closes the connections.
func (t *Tail) Close() { t.cl.Close() }
