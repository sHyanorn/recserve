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
	amendmentsSHA256   = "046daa450bf090017cda40bc9784cc32503a04313b415e9e32fc7a28e9e1ddaf"
	registrationPath   = "../../docs/registration/m8-freshness.md"
	amendmentsPath     = "../../docs/registration/m8-freshness-amendments.md"
	testReadsLog       = "../../bench-results/m8/test-reads.log"
)

// read3 is the refusal of Video_Games' read 3 to any run but
// TestRealDataQuality with a second bundle (the amendment "2026-11-15:
// Video_Games read 3").
const read3 = "Video_Games' read 3 is TestRealDataQuality with a second bundle, RECSERVE_BUNDLE_B (docs/registration/m8-freshness-amendments.md)"

// testReadsAllowed is each category's number of test reads (section 9).
var testReadsAllowed = map[string]int{"Video_Games": 3, "Musical_Instruments": 1}

// failedReads are the log's lines of reads that stopped before they wrote
// or printed any metric, each named by an amendment: they do not count
// (section 9), and the read they stopped, run again, must have their
// conditions. The amendment "2026-11-15: Video_Games read 2 stopped before
// any metric" names Video_Games' read 2.
var failedReads = map[string]bool{
	"2026-11-14T22:51:31Z 654f0f812c31828f6831f542bffdd0729ea6f4f1 Video_Games 04b5dcba8e82381ba1648be798869c22b11caa916b49a138db994a5475890530": true,
}

// openTestRead is section 9's check before read n of category opens the
// test file: the registration at reg and its amendments at amend have
// registrationSHA256 and amendmentsSHA256, the log holds n - 1 reads of
// the category and n is within its number, the conditions are those of
// the category's reads in failedReads that stopped read n, and no output
// path exists. It then appends the read's line to the log: the UTC time,
// the commit, the category and the conditions' hash. commit returns the
// commit the read runs on, or an error if the worktree differs from it.
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
// failedReads, and the conditions of those in failedReads logged after the
// last read counted: they stopped the read that follows it.
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
				done, stopped = done+1, nil
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
// then a read out of order or beyond its category's number is refused, as
// are another registration, an output that exists and a worktree that
// differs from its commit, and none of them logs a line; reads 2 and 3 of
// Video_Games and Musical_Instruments' read follow.
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
		{"read 4", registrationPath, amendmentsPath, "Video_Games", 4, head, nil, "allows 3"},
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
	}{{"Video_Games", 2}, {"Video_Games", 3}, {"Musical_Instruments", 1}} {
		if err := openTestRead(registrationPath, amendmentsPath, log, c.category, c.n, "h", head, out); err != nil {
			t.Errorf("%s read %d: %v", c.category, c.n, err)
		}
	}
}

// A read that stopped before any metric, named in failedReads, does not
// count: read 2 runs once more after it, with the stopped read's
// conditions only, and is refused after that rerun as after any read 2;
// read 3 is not held to those conditions, but to those of a read 3 that
// stopped; unnamed, the stopped read counts.
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
	if err := openTestRead(registrationPath, amendmentsPath, log, "Video_Games", 3, "h3", head); err != nil {
		t.Errorf("read 3 with other conditions than the stopped read 2's: %v", err)
	}
	failed3 := "2026-11-15T01:00:00Z c0ffee Video_Games h4"
	log3 := filepath.Join(dir, "read-3.log")
	if err := os.WriteFile(log3, []byte(read1+"\n"+failed+"\n2026-11-15T00:00:00Z c0ffee Video_Games h2\n"+failed3+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	failedReads[failed3] = true
	if err := openTestRead(registrationPath, amendmentsPath, log3, "Video_Games", 3, "h3", head); err == nil || !strings.Contains(err.Error(), "stopped read") {
		t.Errorf("read 3 with other conditions than the stopped read 3's: %v", err)
	}
	if err := openTestRead(registrationPath, amendmentsPath, log3, "Video_Games", 3, "h4", head); err != nil {
		t.Errorf("read 3 after the named stopped read 3: %v", err)
	}
}

// Video_Games' read 3 is TestRealDataQuality with a second bundle only:
// TestRealDataFreshness refuses it, and TestRealDataQuality refuses it
// without RECSERVE_BUNDLE_B, before either loads a bundle or a split. Each
// runs in a child process with no other RECSERVE_ variable set.
// TestRealDataQuality takes its read's number from the log, so its case
// runs while the log holds two reads of Video_Games.
func TestRead3(t *testing.T) {
	done, _, err := loggedReads(testReadsLog, "Video_Games")
	if err != nil {
		t.Fatal(err)
	}
	none := filepath.Join(t.TempDir(), "none")
	for _, c := range []struct {
		test string
		run  bool
		env  []string
	}{
		{"TestRealDataFreshness", true, []string{"RECSERVE_READ=3"}},
		{"TestRealDataQuality", done == 2, []string{"RECSERVE_DATA=" + none, "RECSERVE_BUNDLE=" + none}},
	} {
		if !c.run {
			t.Logf("%s: the log holds %d reads of Video_Games", c.test, done)
			continue
		}
		cmd := exec.Command(os.Args[0], "-test.run=^"+c.test+"$", "-test.count=1")
		cmd.Env = c.env
		for _, v := range os.Environ() {
			if !strings.HasPrefix(v, "RECSERVE_") {
				cmd.Env = append(cmd.Env, v)
			}
		}
		if out, err := cmd.CombinedOutput(); err == nil || !strings.Contains(string(out), read3) {
			t.Errorf("%s, read 3: %v\n%s", c.test, err, out)
		}
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
