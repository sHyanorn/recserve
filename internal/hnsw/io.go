package hnsw

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// fileMagic starts a saved index; the layout follows it in little-endian:
// n, dim, M, Mmax0, efConstruction, entry, top and len(upper) as int64,
// then the levels as bytes and links0, upperOff and upper as int32.
const fileMagic = "recserve-hnsw-1\n"

// WriteTo writes the graph, without the vectors.
func (x *Index) WriteTo(w io.Writer) (int64, error) {
	bw := bufio.NewWriter(w)
	cw := &countWriter{w: bw}
	head := []int64{int64(x.n), int64(x.dim), int64(x.m), int64(x.m0), int64(x.efc), int64(x.entry), int64(x.top), int64(len(x.upper))}
	if _, err := io.WriteString(cw, fileMagic); err != nil {
		return cw.n, err
	}
	for _, v := range []any{head, x.levels, x.links0, x.upperOff, x.upper} {
		if err := binary.Write(cw, binary.LittleEndian, v); err != nil {
			return cw.n, err
		}
	}
	return cw.n, bw.Flush()
}

// Read reads a graph written by WriteTo and attaches it to data, the rows
// it was built over, dim values each. The graph is checked before use.
func Read(r io.Reader, data []float32, dim int) (*Index, error) {
	br := bufio.NewReader(r)
	magic := make([]byte, len(fileMagic))
	if _, err := io.ReadFull(br, magic); err != nil || string(magic) != fileMagic {
		return nil, errors.New("hnsw: not a saved index")
	}
	head := make([]int64, 8)
	if err := binary.Read(br, binary.LittleEndian, head); err != nil {
		return nil, fmt.Errorf("hnsw: header: %w", err)
	}
	n, d, m, m0, efc, entry, top, nUpper := head[0], head[1], head[2], head[3], head[4], head[5], head[6], head[7]
	switch {
	case int64(dim) != d || dim <= 0 || int64(len(data)) != n*d:
		return nil, fmt.Errorf("hnsw: index of %d rows of %d values, data has %d values of rows of %d", n, d, len(data), dim)
	case m < 2 || m > 1<<10 || (m0 != m && m0 != 2*m) || efc < 1 || top < 0 || top > maxLevel || nUpper < 0 || nUpper > n*maxLevel*(1+m):
		return nil, errors.New("hnsw: header values out of range")
	}
	x := &Index{dim: dim, n: int(n), data: data, m: int(m), m0: int(m0), efc: int(efc), entry: int32(entry), top: int(top)}
	x.levels = make([]uint8, n)
	x.links0 = make([]int32, n*(1+m0))
	x.upperOff = make([]int32, n)
	x.upper = make([]int32, nUpper)
	for _, v := range []any{x.levels, x.links0, x.upperOff, x.upper} {
		if err := binary.Read(br, binary.LittleEndian, v); err != nil {
			return nil, fmt.Errorf("hnsw: body: %w", err)
		}
	}
	if _, err := br.ReadByte(); !errors.Is(err, io.EOF) {
		return nil, errors.New("hnsw: trailing data")
	}
	if err := x.Check(); err != nil {
		return nil, err
	}
	x.pool.New = func() any { return x.newSearcher() }
	return x, nil
}

type countWriter struct {
	w io.Writer
	n int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
