// Package npy reads and writes the NumPy .npy format for C-ordered
// little-endian float32 and int64 arrays.
package npy

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

// Array is a C-ordered array. Exactly one of F32 and I64 is set.
type Array struct {
	Shape []int
	F32   []float32
	I64   []int64
}

var (
	magic   = []byte("\x93NUMPY")
	reDescr = regexp.MustCompile(`'descr':\s*'([^']+)'`)
	reOrder = regexp.MustCompile(`'fortran_order':\s*(True|False)`)
	reShape = regexp.MustCompile(`'shape':\s*\(([^)]*)\)`)
)

// maxElems bounds the element count so a corrupt header cannot overflow it.
const maxElems = 1 << 40

// Parse decodes a .npy file of dtype <f4 or <i8.
func Parse(b []byte) (*Array, error) {
	if len(b) < 10 || !bytes.Equal(b[:6], magic) {
		return nil, errors.New("not a .npy file")
	}
	var hlen, off int
	switch b[6] {
	case 1:
		hlen, off = int(binary.LittleEndian.Uint16(b[8:])), 10
	case 2, 3:
		if len(b) < 12 {
			return nil, errors.New("truncated header")
		}
		hlen, off = int(binary.LittleEndian.Uint32(b[8:])), 12
	default:
		return nil, fmt.Errorf("npy version %d", b[6])
	}
	if hlen > len(b)-off {
		return nil, errors.New("truncated header")
	}
	h := string(b[off : off+hlen])
	data := b[off+hlen:]
	d, o, s := reDescr.FindStringSubmatch(h), reOrder.FindStringSubmatch(h), reShape.FindStringSubmatch(h)
	if d == nil || o == nil || s == nil {
		return nil, fmt.Errorf("header %q", h)
	}
	if o[1] != "False" {
		return nil, errors.New("fortran order")
	}
	a := &Array{}
	n := 1
	for _, f := range strings.Split(s[1], ",") {
		if f = strings.TrimSpace(f); f == "" {
			continue
		}
		v, err := strconv.Atoi(f)
		if err != nil || v < 0 || (v > 0 && n > maxElems/v) {
			return nil, fmt.Errorf("shape %q", s[1])
		}
		a.Shape = append(a.Shape, v)
		n *= v
	}
	switch d[1] {
	case "<f4":
		if len(data) != 4*n {
			return nil, fmt.Errorf("%d data bytes for %d float32 values", len(data), n)
		}
		a.F32 = make([]float32, n)
		for i := range a.F32 {
			a.F32[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[4*i:]))
		}
	case "<i8":
		if len(data) != 8*n {
			return nil, fmt.Errorf("%d data bytes for %d int64 values", len(data), n)
		}
		a.I64 = make([]int64, n)
		for i := range a.I64 {
			a.I64[i] = int64(binary.LittleEndian.Uint64(data[8*i:]))
		}
	default:
		return nil, fmt.Errorf("dtype %s", d[1])
	}
	return a, nil
}

// Encode writes a version 1.0 .npy file holding f32 (if non-nil) or i64 with the given shape.
func Encode(shape []int, f32 []float32, i64 []int64) []byte {
	descr := "<i8"
	if f32 != nil {
		descr = "<f4"
	}
	dims := make([]string, len(shape))
	for i, v := range shape {
		dims[i] = strconv.Itoa(v)
	}
	sh := strings.Join(dims, ", ")
	if len(shape) == 1 {
		sh += ","
	}
	h := fmt.Sprintf("{'descr': '%s', 'fortran_order': False, 'shape': (%s), }", descr, sh)
	pad := 64 - (10+len(h)+1)%64
	if pad == 64 {
		pad = 0
	}
	h += strings.Repeat(" ", pad) + "\n"
	var buf bytes.Buffer
	buf.Write(magic)
	buf.Write([]byte{1, 0})
	_ = binary.Write(&buf, binary.LittleEndian, uint16(len(h)))
	buf.WriteString(h)
	if f32 != nil {
		_ = binary.Write(&buf, binary.LittleEndian, f32)
	} else {
		_ = binary.Write(&buf, binary.LittleEndian, i64)
	}
	return buf.Bytes()
}
