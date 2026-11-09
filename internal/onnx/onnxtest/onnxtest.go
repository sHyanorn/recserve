// Package onnxtest gives tests the ONNX Runtime library and the test models
// in testdata, written by tools/bundle/testdata.py.
package onnxtest

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"math"
	"os"
	"strings"
	"testing"
)

//go:embed testdata
var testdata embed.FS

// Lib returns the ONNX Runtime library named by RECSERVE_ORT_LIB. Without
// it the test is skipped, or fails if RECSERVE_REQUIRE_ORT is set, as in CI.
func Lib(t testing.TB) string {
	t.Helper()
	lib := os.Getenv("RECSERVE_ORT_LIB")
	if lib == "" {
		if os.Getenv("RECSERVE_REQUIRE_ORT") != "" {
			t.Fatal("RECSERVE_REQUIRE_ORT is set but RECSERVE_ORT_LIB is not")
		}
		t.Skip("RECSERVE_ORT_LIB not set")
	}
	return lib
}

// Model returns the named model from testdata after checking it against
// testdata/SHA256SUMS.
func Model(t testing.TB, name string) []byte {
	t.Helper()
	sums, err := testdata.ReadFile("testdata/SHA256SUMS")
	if err != nil {
		t.Fatal(err)
	}
	data, err := testdata.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	got := hex.EncodeToString(sum[:])
	sc := bufio.NewScanner(bytes.NewReader(sums))
	for sc.Scan() {
		if want, file, ok := strings.Cut(sc.Text(), "  "); ok && file == name {
			if got != want {
				t.Fatalf("testdata/%s: sha256 %s, SHA256SUMS says %s", name, got, want)
			}
			return data
		}
	}
	t.Fatalf("testdata/%s is not in SHA256SUMS", name)
	return nil
}

// MeanItems and MeanDim are the shape of the item table of mean.onnx.
const MeanItems, MeanDim = 12, 2

// MeanVectors returns the item table of mean.onnx, row by row: row i, for i
// from 1 to MeanItems, is (cos i, sin i) / i; row 0 is padding and zero.
func MeanVectors() []float32 {
	v := make([]float32, MeanDim*(MeanItems+1))
	for i := 1; i <= MeanItems; i++ {
		v[2*i] = float32(math.Cos(float64(i)) / float64(i))
		v[2*i+1] = float32(math.Sin(float64(i)) / float64(i))
	}
	return v
}

// LinearW and LinearB are the weights and bias of linear_ranker.onnx: the
// score of a row of features x is x · LinearW + LinearB.
var LinearW = []float32{1, 2, -1, 0.5, 0.25, -0.5}

// LinearB is the bias of linear_ranker.onnx.
const LinearB = 0.125

// Linear scores rows of len(LinearW) features as linear_ranker.onnx does,
// up to rounding.
func Linear(features []float32) []float32 {
	n := len(features) / len(LinearW)
	out := make([]float32, n)
	for i := range n {
		s := float64(LinearB)
		for j, w := range LinearW {
			s += float64(features[i*len(LinearW)+j]) * float64(w)
		}
		out[i] = float32(s)
	}
	return out
}
