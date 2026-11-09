// Package dataset reads the Amazon Reviews 2023 split files written by
// scripts/data.sh (user_id,parent_asin,rating,timestamp, gzip-compressed).
package dataset

import (
	"compress/gzip"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
)

// Row is one interaction.
type Row struct {
	User string
	Item string
	TS   int64 // milliseconds since the Unix epoch
}

// Kind is the event kind given to dataset rows.
const Kind = "rating"

// Read reads a .csv.gz split file.
func Read(path string) (_ []Row, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	z, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	r := csv.NewReader(z)
	r.ReuseRecord = true
	head, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if !slices.Equal(head, []string{"user_id", "parent_asin", "rating", "timestamp"}) {
		return nil, fmt.Errorf("%s: header %v", path, head)
	}
	var rows []Row
	for {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			return rows, nil
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		ts, err := strconv.ParseInt(rec[3], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s line %d: timestamp %q", path, len(rows)+2, rec[3])
		}
		rows = append(rows, Row{User: rec[0], Item: rec[1], TS: ts})
	}
}

// FirstTS returns the earliest timestamp, or 0 for no rows.
func FirstTS(rows []Row) int64 {
	if len(rows) == 0 {
		return 0
	}
	first := rows[0].TS
	for _, r := range rows[1:] {
		first = min(first, r.TS)
	}
	return first
}
