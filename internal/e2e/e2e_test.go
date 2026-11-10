// Package e2e runs the gateway and ingest binaries against the compose
// Kafka and Valkey (make e2e). Two of its tests pause Valkey and stop Kafka,
// which would fail every other test that uses them, so the package runs
// only with RECSERVE_E2E=1, on its own.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sHyanorn/recserve/internal/bundle/bundletest"
	"github.com/sHyanorn/recserve/internal/events"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/infratest"
)

const composeFile = "../../deploy/compose.yaml"

// bin holds the built binaries; empty without RECSERVE_E2E.
var bin string

func TestMain(m *testing.M) {
	os.Exit(func() int {
		if os.Getenv("RECSERVE_E2E") == "" {
			return m.Run()
		}
		dir, err := os.MkdirTemp("", "recserve-e2e")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		defer os.RemoveAll(dir) //nolint:errcheck // a leftover temporary directory
		if out, err := exec.Command("go", "build", "-o", dir, "../../cmd/gateway", "../../cmd/ingest").CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "go build: %v\n%s", err, out)
			return 1
		}
		bin = dir
		return m.Run()
	}())
}

func need(t *testing.T) {
	t.Helper()
	if bin == "" {
		t.Skip("RECSERVE_E2E is not set")
	}
}

// start runs a built binary until the test ends; its output is logged if
// the test fails.
func start(t *testing.T, name string, args ...string) *exec.Cmd {
	t.Helper()
	var out bytes.Buffer
	cmd := exec.Command(filepath.Join(bin, name), args...)
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		kill(cmd)
		if t.Failed() {
			t.Logf("%s %s:\n%s", name, strings.Join(args, " "), out.String())
		}
	})
	return cmd
}

// kill ends a process as a crash would, with SIGKILL, and waits for it.
func kill(cmd *exec.Cmd) {
	cmd.Process.Kill() //nolint:errcheck // it has exited already
	cmd.Wait()         //nolint:errcheck // killed
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck // only its address is used
	return ln.Addr().String()
}

// eventually polls ok until it is true, and fails the test after limit.
func eventually(t *testing.T, limit time.Duration, what string, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(limit); !ok(); time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("after %v: %s", limit, what)
		}
	}
}

func up(url string) bool {
	resp, err := http.Get(url)
	if err != nil {
		return false
	}
	resp.Body.Close() //nolint:errcheck // only the status is used
	return resp.StatusCode == http.StatusOK
}

// gateway starts a gateway on the compose services with the test bundle
// and returns its URL once it is ready.
func gateway(t *testing.T) string {
	t.Helper()
	addr := freeAddr(t)
	start(t, "gateway", "-addr", addr, "-admin-addr", "", "-bundle", bundletest.Write(t),
		"-valkey-addr", infratest.ValkeyAddr(t), "-kafka-brokers", strings.Join(infratest.KafkaBrokers(t), ","), "-events-timeout", "1s")
	url := "http://" + addr
	eventually(t, 20*time.Second, "the gateway is not ready", func() bool { return up(url + "/readyz") })
	return url
}

// ingest starts an ingest consumer of the group and returns it once it
// serves, which is after it has created the topic.
func ingest(t *testing.T, group string) *exec.Cmd {
	t.Helper()
	addr := freeAddr(t)
	cmd := start(t, "ingest", "-addr", addr, "-group", group, "-session-timeout", "6s",
		"-valkey-addr", infratest.ValkeyAddr(t), "-kafka-brokers", strings.Join(infratest.KafkaBrokers(t), ","))
	eventually(t, 30*time.Second, "ingest is not live", func() bool { return up("http://" + addr + "/livez") })
	return cmd
}

type response struct {
	Items    []struct{ ID string } `json:"items"`
	Degraded []string              `json:"degraded"`
	Accepted int                   `json:"accepted"`
}

func (r response) has(item string) bool {
	return slices.ContainsFunc(r.Items, func(i struct{ ID string }) bool { return i.ID == item })
}

func post(t *testing.T, url string, body any) (int, response) {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // read to its end below
	var out response
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil && resp.StatusCode < 300 {
		t.Fatal(err)
	}
	return resp.StatusCode, out
}

func recommend(t *testing.T, gw, user string, recent ...string) (int, response) {
	t.Helper()
	return post(t, gw+"/v1/recommend", map[string]any{"user_id": user, "k": bundletest.Items, "recent": recent})
}

func compose(t *testing.T, args ...string) {
	t.Helper()
	if out, err := exec.Command("docker", append([]string{"compose", "-f", composeFile}, args...)...).CombinedOutput(); err != nil {
		t.Fatalf("docker compose %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

func id(prefix string) string { return fmt.Sprintf("%s-%016x", prefix, rand.Uint64()) }

func valkeyStore(t *testing.T) *history.Valkey {
	t.Helper()
	store, err := history.NewValkey(infratest.ValkeyAddr(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	return store
}

// An event sent to /v1/events goes through Kafka and ingest into Valkey,
// and a later /v1/recommend sees it: the item leaves the user's
// recommendations. The 202 does not promise when (D034), so the test polls.
func TestEventReachesRecommend(t *testing.T) {
	need(t)
	gw := gateway(t)
	ingest(t, id("e2e"))
	user := id("u")
	if code, resp := recommend(t, gw, user); code != http.StatusOK || !resp.has("item3") || len(resp.Degraded) != 0 {
		t.Fatalf("before the event: %d %+v", code, resp)
	}
	event := events.Event{UserID: user, ItemID: "item3", TS: 1, Kind: "view"}
	if code, resp := post(t, gw+"/v1/events", map[string]any{"events": []events.Event{event}}); code != http.StatusAccepted || resp.Accepted != 1 {
		t.Fatalf("/v1/events: %d %+v", code, resp)
	}
	eventually(t, 30*time.Second, "the event did not reach /v1/recommend", func() bool {
		code, resp := recommend(t, gw, user)
		if code != http.StatusOK || len(resp.Degraded) != 0 {
			t.Fatalf("%d %+v", code, resp)
		}
		return !resp.has("item3") && len(resp.Items) > 0
	})
}

// Ingest is killed with SIGKILL at random moments while events arrive, and
// started again in the same group. In the end every user's history in
// Valkey is the one of each produced event applied once (property 9, on
// the binaries).
func TestIngestKilledAndRestarted(t *testing.T) {
	need(t)
	const (
		users, batches, perBatch = 12, 60, 40
		kills                    = 5
	)
	seed := rand.Uint64()
	t.Log("seed", seed)
	r := rand.New(rand.NewPCG(seed, 9))
	gw := gateway(t)
	group, prefix := id("e2e"), id("k")
	want := history.NewMemory()
	var sent []events.Event
	ctx := context.Background()

	var wg sync.WaitGroup
	defer wg.Wait()
	produced := make(chan struct{})
	wg.Go(func() {
		defer close(produced)
		for range batches {
			batch := make([]events.Event, perBatch)
			for i := range batch {
				if len(sent) > 0 && r.IntN(4) == 0 {
					batch[i] = sent[r.IntN(len(sent))] // an event sent before
					continue
				}
				batch[i] = events.Event{
					UserID: fmt.Sprint(prefix, "-", r.IntN(users)), ItemID: fmt.Sprint("item", 1+r.IntN(bundletest.Items+2)),
					TS: int64(1 + r.IntN(200)), Kind: "view",
				}
			}
			sent = append(sent, batch...)
			hist := make([]history.Event, len(batch))
			for i, e := range batch {
				hist[i] = e.History()
			}
			if err := want.Append(ctx, hist); err != nil {
				t.Error(err)
				return
			}
			if code, resp := post(t, gw+"/v1/events", map[string]any{"events": batch}); code != http.StatusAccepted {
				t.Errorf("/v1/events: %d %+v", code, resp)
				return
			}
			time.Sleep(400 * time.Millisecond)
		}
	})
	// A restarted member waits up to the session timeout of 6 s for the
	// group to drop the killed one, so the kills fall both into that wait
	// and into the consuming.
	kr := rand.New(rand.NewPCG(seed, 10))
	for range kills {
		cmd := ingest(t, group)
		time.Sleep(time.Duration(500+kr.IntN(8000)) * time.Millisecond)
		kill(cmd)
	}
	<-produced
	if t.Failed() {
		return
	}
	ingest(t, group)
	store := valkeyStore(t)
	var diff string
	eventually(t, 90*time.Second, "the histories differ: "+diff, func() bool {
		for u := range users {
			user := fmt.Sprint(prefix, "-", u)
			w, err := want.Before(ctx, user, history.MaxTS, 0)
			if err != nil {
				t.Fatal(err)
			}
			got, err := store.Before(ctx, user, history.MaxTS, 0)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, w) {
				diff = fmt.Sprintf("%s has %d events, want %d", user, len(got), len(w))
				return false
			}
		}
		return true
	})
}

// While Valkey is paused, the gateway answers every request with 200 from
// the request's recent items, reports history_unavailable, and does not
// wait for Valkey; after it, requests use the stored history again
// (property 10, on the binary).
func TestValkeyPaused(t *testing.T) {
	need(t)
	gw := gateway(t)
	user := id("u")
	store := valkeyStore(t)
	if err := store.Append(context.Background(), []history.Event{{ID: 1, User: user, Item: "item3", TS: 1}}); err != nil {
		t.Fatal(err)
	}
	stored := func() bool {
		code, resp := recommend(t, gw, user)
		if code != http.StatusOK {
			t.Fatalf("%d %+v", code, resp)
		}
		return len(resp.Degraded) == 0 && !resp.has("item3") && resp.has("item5")
	}
	if !stored() {
		t.Fatal("before the pause the stored history is not used")
	}
	compose(t, "pause", "valkey")
	paused := true
	t.Cleanup(func() {
		if paused {
			compose(t, "unpause", "valkey")
		}
	})
	for i := range 40 {
		begin := time.Now()
		code, resp := recommend(t, gw, user, "item5")
		took := time.Since(begin)
		if code != http.StatusOK || !slices.Equal(resp.Degraded, []string{"history_unavailable"}) || resp.has("item5") || !resp.has("item3") {
			t.Fatalf("request %d while paused: %d %+v", i, code, resp)
		}
		if took > time.Second {
			t.Fatalf("request %d while paused took %v", i, took)
		}
	}
	compose(t, "unpause", "valkey")
	paused = false
	eventually(t, 20*time.Second, "the stored history is not used again", stored)
}

// With Kafka stopped, /v1/events answers 503 and /v1/recommend goes on as
// before.
func TestKafkaStopped(t *testing.T) {
	need(t)
	gw := gateway(t)
	ingest(t, id("e2e")) // creates the topic
	user := id("u")
	event := func(ts int64) map[string]any {
		return map[string]any{"events": []events.Event{{UserID: user, ItemID: "item3", TS: ts, Kind: "view"}}}
	}
	if code, resp := post(t, gw+"/v1/events", event(1)); code != http.StatusAccepted {
		t.Fatalf("/v1/events with Kafka up: %d %+v", code, resp)
	}
	compose(t, "stop", "kafka")
	t.Cleanup(func() { compose(t, "up", "-d", "--wait", "kafka") })
	if code, resp := post(t, gw+"/v1/events", event(2)); code != http.StatusServiceUnavailable {
		t.Fatalf("/v1/events with Kafka stopped: %d %+v", code, resp)
	}
	for range 20 {
		if code, resp := recommend(t, gw, user); code != http.StatusOK || len(resp.Degraded) != 0 || len(resp.Items) == 0 {
			t.Fatalf("/v1/recommend with Kafka stopped: %d %+v", code, resp)
		}
	}
}
