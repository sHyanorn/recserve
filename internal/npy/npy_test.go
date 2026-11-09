package npy

import (
	"bytes"
	"os"
	"slices"
	"testing"
)

func TestParseNumPyFiles(t *testing.T) {
	b, err := os.ReadFile("testdata/f32.npy")
	if err != nil {
		t.Fatal(err)
	}
	a, err := Parse(b)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(a.Shape, []int{2, 3}) || !slices.Equal(a.F32, []float32{0, 0.25, 0.5, 0.75, 1, 1.25}) {
		t.Fatalf("got %v %v", a.Shape, a.F32)
	}
	b, err = os.ReadFile("testdata/i64.npy")
	if err != nil {
		t.Fatal(err)
	}
	if a, err = Parse(b); err != nil || !slices.Equal(a.Shape, []int{3}) || !slices.Equal(a.I64, []int64{-1, 0, 7}) {
		t.Fatalf("got %v %v %v", a, err, a)
	}
}

func TestEncodeMatchesNumPy(t *testing.T) {
	for _, tc := range []struct {
		file  string
		shape []int
		f32   []float32
		i64   []int64
	}{
		{"testdata/f32.npy", []int{2, 3}, []float32{0, 0.25, 0.5, 0.75, 1, 1.25}, nil},
		{"testdata/i64.npy", []int{3}, nil, []int64{-1, 0, 7}},
	} {
		want, err := os.ReadFile(tc.file)
		if err != nil {
			t.Fatal(err)
		}
		if got := Encode(tc.shape, tc.f32, tc.i64); !bytes.Equal(got, want) {
			t.Errorf("%s: encoding differs from NumPy's\n got %q\nwant %q", tc.file, got, want)
		}
	}
}

func TestParseRejects(t *testing.T) {
	good := Encode([]int{2}, []float32{1, 2}, nil)
	for name, b := range map[string][]byte{
		"empty":     nil,
		"magic":     append([]byte("XNUMPY"), good[6:]...),
		"truncated": good[:len(good)-1],
		"overflow":  []byte("\x93NUMPY\x01\x00\x46\x00{'descr': '<f4', 'fortran_order': False, 'shape': (4294967296, 4294967296), }\n"),
		"negative":  []byte("\x93NUMPY\x01\x00\x38\x00{'descr': '<f4', 'fortran_order': False, 'shape': (-1,), }\n"),
	} {
		if _, err := Parse(b); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func FuzzParse(f *testing.F) {
	f.Add(Encode([]int{2, 2}, []float32{1, 2, 3, 4}, nil))
	f.Add(Encode([]int{1}, nil, []int64{5}))
	f.Fuzz(func(t *testing.T, b []byte) {
		a, err := Parse(b)
		if err != nil {
			return
		}
		n := 1
		for _, v := range a.Shape {
			n *= v
		}
		if len(a.F32)+len(a.I64) != n {
			t.Fatalf("shape %v but %d values", a.Shape, len(a.F32)+len(a.I64))
		}
	})
}
