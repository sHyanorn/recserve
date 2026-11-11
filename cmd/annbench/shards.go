package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"time"

	"github.com/sHyanorn/recserve/internal/bench"
	"github.com/sHyanorn/recserve/internal/hnsw"
	"github.com/sHyanorn/recserve/internal/shard"
	"github.com/sHyanorn/recserve/internal/vec"
)

// Scatter-gather over shard processes, for the comparison of
// docs/BENCHMARKS.md (Hand-written HNSW): each shard serves the rows that
// shard.Of assigns to it with its own index; the parent sends every query
// to all shards and merges their top k. Messages are little-endian: a
// request is k and ef as int32, then the query's float32 values; a reply
// is a count n, then n pairs of a global ID (int32) and a score (float32).

// shardConn is the parent's end of one shard: requests go to w, replies
// come from r.
type shardConn struct {
	w    *bufio.Writer
	r    *bufio.Reader
	done func() error // closes the requests and waits for the shard to exit
}

// spawnFunc starts shard i of n and returns its connection.
type spawnFunc func(i, n int) (*shardConn, error)

type shardInfo struct {
	Nodes             int     `json:"nodes"`
	UnreachableLayer0 int     `json:"unreachable_layer0"`
	BuildSeconds      float64 `json:"build_seconds"` // 0 when the graph was read from disk
	M                 int     `json:"M"`             // the graph's own, also when read from disk
	EfC               int     `json:"ef_construction"`
}

type shardRecord struct {
	Env     map[string]string `json:"env"`
	Set     string            `json:"set"`
	Queries int               `json:"queries"`
	M       int               `json:"M"`
	EfC     int               `json:"ef_construction"`
	Shards  []shardInfo       `json:"shards"`
	Points  []point           `json:"points"`
}

// serve is a shard process: it indexes its rows of s, reading the graph
// from o.graph if it exists and building and saving it otherwise, reports
// its nodes, then answers requests from r on w until r ends.
func serve(s *bench.ANNSet, o options, r io.Reader, w io.Writer) error {
	var ids []int32 // local ID → global ID
	var rows []float32
	for id := range int32(s.NBase()) {
		if shard.Of(id, o.shards) == o.shard {
			ids = append(ids, id)
			rows = append(rows, s.Row(id)...)
		}
	}
	var ix *hnsw.Index
	info := shardInfo{Nodes: len(ids)}
	if f, err := os.Open(o.graph); err == nil {
		ix, err = hnsw.Read(bufio.NewReader(f), rows, s.Dim)
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return err
		}
	} else {
		start := time.Now()
		if ix, err = hnsw.Build(rows, s.Dim, hnsw.Config{M: o.m, EfConstruction: o.efc, Seed: o.seed, Workers: o.workers}); err != nil {
			return err
		}
		info.BuildSeconds = time.Since(start).Seconds()
		if err := saveGraph(ix, o.graph); err != nil {
			return err
		}
	}
	info.UnreachableLayer0 = unreachable(ix)
	info.M, info.EfC = ix.Params()
	bw := bufio.NewWriter(w)
	if err := binary.Write(bw, binary.LittleEndian, []float64{float64(info.Nodes), float64(info.UnreachableLayer0), info.BuildSeconds, float64(info.M), float64(info.EfC)}); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	br := bufio.NewReader(r)
	head := make([]int32, 2)
	q := make([]float32, s.Dim)
	for {
		if err := binary.Read(br, binary.LittleEndian, head); errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return err
		}
		if err := binary.Read(br, binary.LittleEndian, q); err != nil {
			return err
		}
		hits := ix.Search(q, int(head[0]), int(head[1]), nil)
		out := make([]int32, 1, 1+2*len(hits))
		out[0] = int32(len(hits))
		for _, h := range hits {
			out = append(out, ids[h.ID], int32(math.Float32bits(float32(h.Score))))
		}
		if err := binary.Write(bw, binary.LittleEndian, out); err != nil {
			return err
		}
		if err := bw.Flush(); err != nil {
			return err
		}
	}
}

func saveGraph(ix *hnsw.Index, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	_, err = ix.WriteTo(f)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// ask sends query q to every shard and merges their replies into the top k
// by score, ties to the lower global ID.
func ask(conns []*shardConn, q []float32, k, ef int) ([]vec.Hit, error) {
	for _, c := range conns {
		if err := binary.Write(c.w, binary.LittleEndian, []int32{int32(k), int32(ef)}); err != nil {
			return nil, err
		}
		if err := binary.Write(c.w, binary.LittleEndian, q); err != nil {
			return nil, err
		}
		if err := c.w.Flush(); err != nil {
			return nil, err
		}
	}
	var buf []vec.Hit
	for _, c := range conns {
		var n int32
		if err := binary.Read(c.r, binary.LittleEndian, &n); err != nil {
			return nil, err
		}
		pairs := make([]int32, 2*n)
		if err := binary.Read(c.r, binary.LittleEndian, pairs); err != nil {
			return nil, err
		}
		for i := 0; i < len(pairs); i += 2 {
			buf = append(buf, vec.Hit{ID: pairs[i], Score: float64(math.Float32frombits(uint32(pairs[i+1])))})
		}
	}
	slices.SortFunc(buf, func(a, b vec.Hit) int {
		if vec.Better(a, b) {
			return -1
		}
		return 1
	})
	return buf[:min(k, len(buf))], nil
}

// shards measures scatter-gather over o.shards shard processes started by
// spawn, for every k and ef, with the protocol of query.
func shards(s *bench.ANNSet, o options, measure func() float64, logf func(string, ...any), spawn spawnFunc) (res any, err error) {
	nq, err := queryCount(s, o)
	if err != nil {
		return nil, err
	}
	var conns []*shardConn
	defer func() {
		for _, c := range conns {
			if derr := c.done(); err == nil {
				err = derr
			}
		}
	}()
	r := shardRecord{Env: env(o), Set: o.set, Queries: nq}
	for i := range o.shards {
		c, err := spawn(i, o.shards)
		if err != nil {
			return nil, err
		}
		conns = append(conns, c)
	}
	for i, c := range conns {
		head := make([]float64, 5)
		if err := binary.Read(c.r, binary.LittleEndian, head); err != nil {
			return nil, fmt.Errorf("shard %d: %w", i, err)
		}
		info := shardInfo{Nodes: int(head[0]), UnreachableLayer0: int(head[1]), BuildSeconds: head[2], M: int(head[3]), EfC: int(head[4])}
		logf("shard %d: %d nodes, %d unreachable on layer 0, build %.0f s, M %d, efConstruction %d", i, info.Nodes, info.UnreachableLayer0, info.BuildSeconds, info.M, info.EfC)
		if i > 0 && (info.M != r.M || info.EfC != r.EfC) {
			return nil, fmt.Errorf("shard %d: graph of M %d and efConstruction %d, shard 0's of %d and %d", i, info.M, info.EfC, r.M, r.EfC)
		}
		r.M, r.EfC = info.M, info.EfC
		r.Shards = append(r.Shards, info)
	}
	for _, k := range o.ks {
		for _, ef := range o.efs {
			if ef < k {
				continue
			}
			search := func(q []float32) ([]vec.Hit, error) { return ask(conns, q, k, ef) }
			got, noise := bench.Bracketed(o.ref, measure, func() pass { return timed(s, nq, k, search) })
			if got.err != nil {
				return nil, got.err
			}
			p := point{Index: fmt.Sprintf("recserve/hnsw, %d shards", o.shards), M: r.M, EfC: r.EfC, Ef: ef, K: k,
				Recall: got.recall, Latency: got.latency, QPS: got.qps, GC: got.gc, Noise: noise}
			logPoint(logf, p)
			r.Points = append(r.Points, p)
		}
	}
	return r, nil
}

// spawnProcess starts shard processes of this executable in serve mode;
// their logs go to this process's standard error.
func spawnProcess(o options) spawnFunc {
	return func(i, n int) (*shardConn, error) {
		exe, err := os.Executable()
		if err != nil {
			return nil, err
		}
		cmd := exec.Command(exe, "-mode", "serve", "-data", o.dataDir, "-set", o.set,
			"-shard", strconv.Itoa(i), "-shards", strconv.Itoa(n), "-graph", o.graph+"."+strconv.Itoa(i),
			"-M", strconv.Itoa(o.m), "-efc", strconv.Itoa(o.efc), "-seed", strconv.FormatUint(o.seed, 10),
			"-workers", strconv.Itoa(max(1, o.workers/n)))
		cmd.Stderr = os.Stderr
		in, err := cmd.StdinPipe()
		if err != nil {
			return nil, err
		}
		out, err := cmd.StdoutPipe()
		if err != nil {
			return nil, err
		}
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		return &shardConn{w: bufio.NewWriter(in), r: bufio.NewReader(out), done: func() error {
			if err := in.Close(); err != nil {
				return err
			}
			return cmd.Wait()
		}}, nil
	}
}
