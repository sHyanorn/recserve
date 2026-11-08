package bench

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// Npy is a C-ordered little-endian array read from a .npy file.
type Npy struct {
	Shape []int
	F32   []float32 // set when the dtype is <f4
	I64   []int64   // set when the dtype is <i8
}

var (
	reDescr = regexp.MustCompile(`'descr':\s*'([^']+)'`)
	reOrder = regexp.MustCompile(`'fortran_order':\s*(True|False)`)
	reShape = regexp.MustCompile(`'shape':\s*\(([^)]*)\)`)
)

// ReadNpy reads a .npy file of dtype <f4 or <i8.
func ReadNpy(path string) (*Npy, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(b) < 10 || !bytes.Equal(b[:6], []byte("\x93NUMPY")) {
		return nil, fmt.Errorf("%s: not a .npy file", path)
	}
	var hlen, off int
	switch b[6] {
	case 1:
		hlen, off = int(binary.LittleEndian.Uint16(b[8:])), 10
	case 2, 3:
		hlen, off = int(binary.LittleEndian.Uint32(b[8:])), 12
	default:
		return nil, fmt.Errorf("%s: npy version %d", path, b[6])
	}
	if off+hlen > len(b) {
		return nil, fmt.Errorf("%s: truncated header", path)
	}
	h := string(b[off : off+hlen])
	data := b[off+hlen:]
	d, o, s := reDescr.FindStringSubmatch(h), reOrder.FindStringSubmatch(h), reShape.FindStringSubmatch(h)
	if d == nil || o == nil || s == nil {
		return nil, fmt.Errorf("%s: header %q", path, h)
	}
	if o[1] != "False" {
		return nil, fmt.Errorf("%s: fortran order", path)
	}
	a := &Npy{}
	n := 1
	for _, f := range strings.Split(s[1], ",") {
		if f = strings.TrimSpace(f); f == "" {
			continue
		}
		v, err := strconv.Atoi(f)
		if err != nil {
			return nil, fmt.Errorf("%s: shape %q", path, s[1])
		}
		a.Shape = append(a.Shape, v)
		n *= v
	}
	switch d[1] {
	case "<f4":
		if len(data) != 4*n {
			return nil, fmt.Errorf("%s: %d data bytes for %d values", path, len(data), n)
		}
		a.F32 = make([]float32, n)
		for i := range a.F32 {
			a.F32[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[4*i:]))
		}
	case "<i8":
		if len(data) != 8*n {
			return nil, fmt.Errorf("%s: %d data bytes for %d values", path, len(data), n)
		}
		a.I64 = make([]int64, n)
		for i := range a.I64 {
			a.I64[i] = int64(binary.LittleEndian.Uint64(data[8*i:]))
		}
	default:
		return nil, fmt.Errorf("%s: dtype %s", path, d[1])
	}
	return a, nil
}
