// Package infratest finds the services that integration tests run against
// (deploy/compose.yaml).
package infratest

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/sHyanorn/recserve/internal/valkeyclient"
)

// ValkeyAddr returns the Valkey address named by RECSERVE_VALKEY_ADDR.
// Without it the test is skipped, or fails if RECSERVE_REQUIRE_INFRA is 1,
// as in CI.
func ValkeyAddr(t testing.TB) string {
	t.Helper()
	return env(t, "RECSERVE_VALKEY_ADDR")
}

// ValkeyClient returns a client of the Valkey server at addr, closed when
// the test ends.
func ValkeyClient(t testing.TB, addr string) *valkeyclient.Client {
	t.Helper()
	c, err := valkeyclient.New(addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func env(t testing.TB, name string) string {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		if os.Getenv("RECSERVE_REQUIRE_INFRA") == "1" {
			t.Fatalf("RECSERVE_REQUIRE_INFRA is 1 but %s is not set", name)
		}
		t.Skipf("%s not set", name)
	}
	return v
}

// KafkaBrokers returns the brokers named by RECSERVE_KAFKA_BROKERS,
// separated by commas, under the rules of ValkeyAddr.
func KafkaBrokers(t testing.TB) []string {
	t.Helper()
	return strings.Split(env(t, "RECSERVE_KAFKA_BROKERS"), ",")
}

// TopicName returns a topic name no other test uses.
func TopicName() string { return fmt.Sprintf("test-%016x", rand.Uint64()) }

// CreateTopic creates the topic on the test's Kafka and deletes it when the
// test ends.
func CreateTopic(t testing.TB, topic string, partitions int32) {
	t.Helper()
	c := kafkaClient(t)
	req := kmsg.NewPtrCreateTopicsRequest()
	req.TimeoutMillis = 15_000
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = topic, partitions, 1
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(context.Background(), c)
	if err == nil {
		err = kerr.ErrorForCode(resp.Topics[0].ErrorCode)
	}
	if err != nil {
		t.Fatalf("creating topic %s: %v", topic, err)
	}
	DeleteTopic(t, topic)
}

// DeleteTopic deletes the topic when the test ends.
func DeleteTopic(t testing.TB, topic string) {
	t.Helper()
	c := kafkaClient(t)
	t.Cleanup(func() {
		req := kmsg.NewPtrDeleteTopicsRequest()
		req.TimeoutMillis = 15_000
		req.TopicNames = []string{topic}
		dt := kmsg.NewDeleteTopicsRequestTopic()
		dt.Topic = &topic
		req.Topics = append(req.Topics, dt)
		if _, err := req.RequestWith(context.Background(), c); err != nil {
			t.Errorf("deleting topic %s: %v", topic, err)
		}
	})
}

// Records reads the topic from its start until it has n records, and fails
// the test if they do not arrive within 30 seconds. They are in the order
// read: in order within a partition, in no order across partitions.
func Records(t testing.TB, topic string, n int) []*kgo.Record {
	t.Helper()
	c := kafkaClient(t, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var out []*kgo.Record
	for len(out) < n {
		fetches := c.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			t.Fatalf("topic %s gave %d of %d records: %v", topic, len(out), n, err)
		}
		out = append(out, fetches.Records()...)
	}
	return out
}

func kafkaClient(t testing.TB, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	c, err := kgo.NewClient(append(opts, kgo.SeedBrokers(KafkaBrokers(t)...))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}
