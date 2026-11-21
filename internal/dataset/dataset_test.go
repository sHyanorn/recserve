package dataset

import (
	"compress/gzip"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func writeSplit(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "x.csv.gz")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	z := gzip.NewWriter(f)
	if _, err := z.Write([]byte(body)); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(z.Close(), f.Close()); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRead(t *testing.T) {
	rows, err := Read(writeSplit(t, "user_id,parent_asin,rating,timestamp\nu1,i1,5.0,1700000000000\nu2,i2,3.0,1600000000000\n"))
	if err != nil || len(rows) != 2 || rows[1] != (Row{"u2", "i2", 1600000000000}) {
		t.Fatalf("got %v, %v", rows, err)
	}
	if got := FirstTS(rows); got != 1600000000000 {
		t.Errorf("FirstTS = %d", got)
	}
}

func TestReadSaysWhatItExpectedAndFound(t *testing.T) {
	_, err := Read(writeSplit(t, "user,item,rating,ts\n"))
	if err == nil || !strings.Contains(err.Error(), "[user item rating ts]") || !strings.Contains(err.Error(), "user_id,parent_asin,rating,timestamp") {
		t.Errorf("wrong header: got %v, want the header found and the one expected", err)
	}
	_, err = Read(writeSplit(t, "user_id,parent_asin,rating,timestamp\nu1,i1,5.0,soon\n"))
	if !errors.Is(err, strconv.ErrSyntax) || !strings.Contains(err.Error(), "line 2") {
		t.Errorf("bad timestamp: got %v, want line 2 and the parse error", err)
	}
}
