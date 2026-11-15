package recommend

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// Test reads of docs/registration/m8-freshness.md, section 9: each is
// checked and logged before it opens a test file.

const (
	// registrationSHA256 and amendmentsSHA256 are the SHA-256 of the
	// registration and of its amendments, which a test read checks before
	// it opens a test file.
	registrationSHA256 = "aceb32b6c93c8deb48c795dcbf115374c05056cee9a55f883e6e8884c4b59bd4"
	amendmentsSHA256   = "545f29c0826ffc0f2e4998ec9c6ed031bdc9b293901742f00fdeb952931dd31e"
	registrationPath   = "../../docs/registration/m8-freshness.md"
	amendmentsPath     = "../../docs/registration/m8-freshness-amendments.md"
	testReadsLog       = "../../bench-results/m8/test-reads.log"
)

// testReadsAllowed is each category's number of test reads (section 9).
var testReadsAllowed = map[string]int{"Video_Games": 2, "Musical_Instruments": 1}

// failedReads are the log's lines of reads that stopped before they wrote
// or printed any metric, each named by an amendment: they do not count
// (section 9), and a later read of their category must have their
// conditions. The amendment of 2026-11-15 names Video_Games' read 2.
var failedReads = map[string]bool{
	"2026-11-14T22:51:31Z 654f0f812c31828f6831f542bffdd0729ea6f4f1 Video_Games 04b5dcba8e82381ba1648be798869c22b11caa916b49a138db994a5475890530": true,
}

// openTestRead is section 9's check before read n of category opens the
// test file: the registration at reg and its amendments at amend have
// registrationSHA256 and amendmentsSHA256, the log holds n - 1 reads of
// the category and n is within its number, the conditions are those of
// every stopped read of the category in failedReads, and no output path
// exists. It
// then appends the read's line to the log: the UTC time, the commit, the
// category and the conditions' hash. commit returns the commit the read
// runs on, or an error if the worktree differs from it.
func openTestRead(reg, amend, log, category string, n int, conditions string, commit func() (string, error), outs ...string) error {
	for _, f := range []struct{ path, want string }{{reg, registrationSHA256}, {amend, amendmentsSHA256}} {
		data, err := os.ReadFile(f.path)
		if err != nil {
			return err
		}
		if sum := sha256.Sum256(data); hex.EncodeToString(sum[:]) != f.want {
			return fmt.Errorf("%s has SHA-256 %x; the committed one is %s", f.path, sum, f.want)
		}
	}
	if n < 1 || n > testReadsAllowed[category] {
		return fmt.Errorf("read %d of %s: the registration allows %d", n, category, testReadsAllowed[category])
	}
	done, stopped, err := loggedReads(log, category)
	if err != nil {
		return err
	}
	if done != n-1 {
		return fmt.Errorf("%s holds %d reads of %s; read %d follows %d", log, done, category, n, n-1)
	}
	for _, c := range stopped {
		if c != conditions {
			return fmt.Errorf("%s holds a stopped read of %s with conditions %s, not this read's %s", log, category, c, conditions)
		}
	}
	for _, out := range outs {
		if _, err := os.Stat(out); err == nil {
			return fmt.Errorf("output %s exists", out)
		} else if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	head, err := commit()
	if err != nil {
		return err
	}
	f, err := os.OpenFile(log, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(f, "%s %s %s %s\n", time.Now().UTC().Format(time.RFC3339), head, category, conditions)
	return errors.Join(err, f.Close())
}

// loggedReads returns the number of reads of category in the log, but
// failedReads, and the conditions of those in failedReads.
func loggedReads(log, category string) (done int, stopped []string, err error) {
	logged, err := os.ReadFile(log)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return 0, nil, err
	}
	for line := range strings.Lines(string(logged)) {
		if f := strings.Fields(line); len(f) == 4 && f[2] == category {
			if failedReads[strings.TrimSuffix(line, "\n")] {
				stopped = append(stopped, f[3])
			} else {
				done++
			}
		}
	}
	return done, stopped, nil
}

// gitHead returns the commit checked out, or an error if a tracked file
// differs from it: a read runs on committed code and choices.
func gitHead() (string, error) {
	diff, err := exec.Command("git", "status", "--porcelain", "--untracked-files=no").Output()
	if err != nil {
		return "", err
	}
	if len(diff) > 0 {
		return "", fmt.Errorf("tracked files differ from the commit:\n%s", diff)
	}
	head, err := exec.Command("git", "rev-parse", "HEAD").Output()
	return strings.TrimSpace(string(head)), err
}

// Read 2 of Video_Games is refused before read 1, which logs its line;
// then every other read but read 2 is refused, as are another registration, an output that exists and a
// worktree that differs from its commit, and none of them logs a line.
func TestOpenTestRead(t *testing.T) {
	dir := t.TempDir()
	log, out, other := filepath.Join(dir, "test-reads.log"), filepath.Join(dir, "read.md"), filepath.Join(dir, "other.md")
	if err := os.WriteFile(other, []byte("another registration\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	head := func() (string, error) { return "c0ffee", nil }
	if err := openTestRead(registrationPath, amendmentsPath, log, "Video_Games", 2, "h", head, out); err == nil || !strings.Contains(err.Error(), "holds 0 reads") {
		t.Errorf("read 2 before read 1: %v", err)
	}
	if err := openTestRead(registrationPath, amendmentsPath, log, "Video_Games", 1, "h1", head, out); err != nil {
		t.Fatal(err)
	}
	logged, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	if f := strings.Fields(string(logged)); len(f) != 4 || f[1] != "c0ffee" || f[2] != "Video_Games" || f[3] != "h1" {
		t.Fatalf("log %q", logged)
	}
	if _, err := time.Parse(time.RFC3339, strings.Fields(string(logged))[0]); err != nil {
		t.Fatal(err)
	}
	dirty := func() (string, error) { return "", errors.New("tracked files differ from the commit") }
	for _, c := range []struct {
		name, reg, amend, category string
		n                          int
		commit                     func() (string, error)
		outs                       []string
		want                       string
	}{
		{"another registration", other, amendmentsPath, "Video_Games", 2, head, nil, "SHA-256"},
		{"other amendments", registrationPath, other, "Video_Games", 2, head, nil, "SHA-256"},
		{"read 1 again", registrationPath, amendmentsPath, "Video_Games", 1, head, nil, "holds 1 reads of Video_Games; read 1 follows 0"},
		{"read 3", registrationPath, amendmentsPath, "Video_Games", 3, head, nil, "allows 2"},
		{"Musical_Instruments' read 2", registrationPath, amendmentsPath, "Musical_Instruments", 2, head, nil, "allows 1"},
		{"another category", registrationPath, amendmentsPath, "Software", 1, head, nil, "allows 0"},
		{"output exists", registrationPath, amendmentsPath, "Video_Games", 2, head, []string{out, log}, "exists"},
		{"worktree differs", registrationPath, amendmentsPath, "Video_Games", 2, dirty, nil, "differ"},
	} {
		err := openTestRead(c.reg, c.amend, log, c.category, c.n, "h", c.commit, c.outs...)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want %q", c.name, err, c.want)
		}
		if now, err := os.ReadFile(log); err != nil || !slices.Equal(now, logged) {
			t.Fatalf("%s: the log changed", c.name)
		}
	}
	for _, c := range []struct {
		category string
		n        int
	}{{"Video_Games", 2}, {"Musical_Instruments", 1}} {
		if err := openTestRead(registrationPath, amendmentsPath, log, c.category, c.n, "h", head, out); err != nil {
			t.Errorf("%s read %d: %v", c.category, c.n, err)
		}
	}
}

// A read that stopped before any metric, named in failedReads, does not
// count: read 2 runs once more after it, with the stopped read's
// conditions only, and is refused after that rerun as after any read 2;
// unnamed, the stopped read counts.
func TestOpenTestReadAfterAFailedRead(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, "test-reads.log")
	read1 := "2026-11-14T22:01:40Z c0ffee Video_Games h1"
	failed := "2026-11-14T22:51:31Z c0ffee Video_Games h2"
	if err := os.WriteFile(log, []byte(read1+"\n"+failed+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	head := func() (string, error) { return "c0ffee", nil }
	if err := openTestRead(registrationPath, amendmentsPath, log, "Video_Games", 2, "h2", head); err == nil || !strings.Contains(err.Error(), "holds 2 reads") {
		t.Fatalf("read 2 after a stopped read 2 not named: %v", err)
	}
	saved := failedReads
	t.Cleanup(func() { failedReads = saved })
	failedReads = map[string]bool{failed: true}
	if err := openTestRead(registrationPath, amendmentsPath, log, "Video_Games", 2, "h3", head); err == nil || !strings.Contains(err.Error(), "stopped read") {
		t.Fatalf("read 2 with other conditions than the stopped read's: %v", err)
	}
	if err := openTestRead(registrationPath, amendmentsPath, log, "Video_Games", 2, "h2", head); err != nil {
		t.Fatalf("read 2 after the named stopped read: %v", err)
	}
	if err := openTestRead(registrationPath, amendmentsPath, log, "Video_Games", 2, "h2", head); err == nil || !strings.Contains(err.Error(), "holds 2 reads") {
		t.Errorf("read 2 after its rerun: %v", err)
	}
}

// gitHead names the commit checked out in full, unless a tracked file
// differs from it.
func TestGitHead(t *testing.T) {
	head, err := gitHead()
	if err != nil && strings.Contains(err.Error(), "tracked files differ") {
		t.Skip(err)
	}
	if err != nil || len(head) != 40 || strings.Trim(head, "0123456789abcdef") != "" {
		t.Fatalf("%q, %v", head, err)
	}
}
