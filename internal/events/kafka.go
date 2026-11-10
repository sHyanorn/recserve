package events

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Topic is the topic the gateway produces to and the ingest consumer reads.
const Topic = "events"

// Kafka produces each event as one record keyed by its user, so a user's
// events stay in one partition in the order they were produced. The
// producer is idempotent and waits for the acknowledgment of every in-sync
// replica, the client's defaults.
type Kafka struct{ c *kgo.Client }

// NewKafka returns a producer to the topic. It does not connect: brokers
// that are down fail a Produce, not the start. Until the topic's metadata
// is loaded, a record fails once it has waited for the timeout; after that,
// a record waiting through a broker outage stays buffered and is produced
// when the broker is back (D034, point 4).
func NewKafka(brokers []string, topic string, timeout time.Duration) (*Kafka, error) {
	if timeout < time.Second {
		return nil, fmt.Errorf("events timeout %v: the Kafka client takes no record timeout below 1s", timeout)
	}
	c, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.ProducerLinger(0), // a request waits for its records; batches still form while a produce request is in flight
		kgo.RecordDeliveryTimeout(timeout),
	)
	if err != nil {
		return nil, err
	}
	return &Kafka{c: c}, nil
}

// Close fails the records still buffered and closes the connections.
func (k *Kafka) Close() { k.c.Close() }

// Produce implements Producer: nil means the brokers acknowledged every
// record. It returns when ctx ends even if the client cannot give up a
// record yet: one already sent stays with an idempotent producer until its
// outcome is known, and may still be written.
func (k *Kafka) Produce(ctx context.Context, events []Event) error {
	records := make([]*kgo.Record, len(events))
	for i, e := range events {
		v, err := json.Marshal(e)
		if err != nil {
			return err
		}
		records[i] = &kgo.Record{Key: []byte(e.UserID), Value: v}
	}
	results := make(chan error, len(records))
	for _, r := range records {
		// Not ctx: when the context of a batch's first record ends, the
		// client fails every record buffered for that partition, other
		// requests' too. A full buffer fails the record at once.
		k.c.TryProduce(context.Background(), r, func(_ *kgo.Record, err error) { results <- err })
	}
	var first error
	for range records {
		select {
		case err := <-results:
			if first == nil {
				first = err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return first
}
