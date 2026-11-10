// Command ingest applies the events topic to the user histories in Valkey
// (docs/DECISIONS.md D035). Several instances share the topic's partitions
// as one consumer group.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/sHyanorn/recserve/internal/events"
	"github.com/sHyanorn/recserve/internal/history"
	"github.com/sHyanorn/recserve/internal/ingest"
)

func main() {
	brokers := flag.String("kafka-brokers", "", "comma-separated Kafka brokers")
	valkeyAddr := flag.String("valkey-addr", "", "Valkey address of the user histories")
	group := flag.String("group", "ingest", "consumer group")
	addr := flag.String("addr", ":8082", "listen address of /metrics and /livez")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log, *brokers, *valkeyAddr, *group, *addr); err != nil {
		log.Error("ingest stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, brokers, valkeyAddr, group, addr string) error {
	if brokers == "" || valkeyAddr == "" {
		return errors.New("-kafka-brokers and -valkey-addr are required")
	}
	store, err := history.NewValkey(valkeyAddr)
	if err != nil {
		return err
	}
	defer store.Close()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	c, err := ingest.New(ctx, ingest.Config{Brokers: strings.Split(brokers, ","), Group: group, Topic: events.Topic, Store: store, Log: log})
	if err != nil {
		return err
	}
	defer c.Close() // commits what is applied and leaves the group

	reg := prometheus.NewRegistry()
	reg.MustRegister(append(c.Collectors(), collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))...)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics listener stopped", "err", err)
		}
	}()
	log.Info("consuming", "topic", events.Topic, "group", group, "addr", addr)
	err = c.Run(ctx)
	if cerr := srv.Close(); cerr != nil {
		log.Warn("closing the metrics listener", "err", cerr)
	}
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
