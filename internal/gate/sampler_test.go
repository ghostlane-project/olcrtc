package gate

import (
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// ai-generated: whole file, unit cover for the memory sampler.

// waitSamples blocks until s holds at least n samples. Waiting on the count
// instead of sleeping keeps a loaded runner from starving the ticker into a
// flaky failure.
func waitSamples(t *testing.T, s *Sampler, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for len(s.Samples()) < n {
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d samples after 5 s", len(s.Samples()), n)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSamplerRecordsAndPeaksBetweenMarks(t *testing.T) {
	s := NewSampler(5 * time.Millisecond)
	s.Start()
	t.Cleanup(s.Stop)
	s.Mark("a")
	junk := make([][]byte, 0, 64)
	for range 64 {
		junk = append(junk, make([]byte, 256<<10))
	}
	// The count is read after the mark and the allocation. The next sample
	// may have begun before either; the one after it began later, so it lies
	// between the marks and measured a heap holding the junk.
	waitSamples(t, s, len(s.Samples())+2)
	s.Mark("b")
	runtime.KeepAlive(junk)
	waitSamples(t, s, 5)
	s.Stop()
	heap, rss, ok := s.PeakBetween("a", "b")
	if !ok || heap < 16<<20 {
		t.Fatalf("PeakBetween = %d %d %v, want a heap holding the 16 MiB allocated", heap, rss, ok)
	}
	for _, w := range [][2]string{{"a", "nope"}, {"nope", "b"}} {
		if _, _, ok := s.PeakBetween(w[0], w[1]); ok {
			t.Fatalf("PeakBetween(%s, %s) accepted an unknown mark", w[0], w[1])
		}
	}
	for _, sm := range s.Samples() {
		if sm.Goroutines <= 0 {
			t.Fatal("a sample without goroutines")
		}
	}
}

func TestPeakBetweenTakesTheHighestInsideTheWindowOnly(t *testing.T) {
	s := NewSampler(time.Second)
	t0 := time.Now()
	at := func(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }
	s.samples = []Sample{
		{At: at(0), HeapInuse: 90, RSS: 900, Goroutines: 1},
		{At: at(10), HeapInuse: 30, RSS: 100, Goroutines: 1},
		{At: at(20), HeapInuse: 50, RSS: 300, Goroutines: 1},
		{At: at(30), HeapInuse: 40, RSS: 500, Goroutines: 1},
		{At: at(40), HeapInuse: 99, RSS: 999, Goroutines: 1},
	}
	s.marks["from"], s.marks["to"], s.marks["gap-from"], s.marks["gap-to"] = at(10), at(30), at(31), at(39)
	if heap, rss, ok := s.PeakBetween("from", "to"); !ok || heap != 50 || rss != 500 {
		t.Fatalf("PeakBetween(from, to) = %d %d %v, want 50 500 true", heap, rss, ok)
	}
	for _, w := range [][2]string{{"gap-from", "gap-to"}, {"to", "from"}} {
		if heap, rss, ok := s.PeakBetween(w[0], w[1]); ok {
			t.Fatalf("PeakBetween(%s, %s) = %d %d, want no sample in it", w[0], w[1], heap, rss)
		}
	}
}

// TestAMarkTakesASampleOfItsOwnWhileRunning holds a mark to a reading of its
// own moment, S1's baseline before its burst and not a tick into it. A mark
// on a sampler that is not running only names the time and reads the next
// sample, if there is one.
func TestAMarkTakesASampleOfItsOwnWhileRunning(t *testing.T) {
	s := NewSampler(time.Hour)
	s.Mark("early")
	s.Start()
	t.Cleanup(s.Stop)
	waitSamples(t, s, 1)
	s.Mark("idle")
	got := s.Samples()
	if len(got) != 2 {
		t.Fatalf("%d samples after Start and a mark, want 2", len(got))
	}
	if idle, ok := s.sampleAt("idle"); !ok || !idle.At.Equal(got[1].At) {
		t.Fatalf("sampleAt(idle) = %+v %v, want the mark's own sample", idle, ok)
	}
	if early, ok := s.sampleAt("early"); !ok || !early.At.Equal(got[0].At) {
		t.Fatalf("sampleAt(early) = %+v %v, want the first sample after it", early, ok)
	}
	s.Stop()
	n := len(s.Samples())
	s.Mark("stopped")
	if len(s.Samples()) != n {
		t.Fatal("a mark on a stopped sampler took a sample")
	}
	for _, mark := range []string{"stopped", "unknown"} {
		if sm, ok := s.sampleAt(mark); ok {
			t.Fatalf("sampleAt(%s) = %+v, want none", mark, sm)
		}
	}
}

// TestSamplesStayInTimeOrder races marks against a fast ticker: a mark's
// reading and a tick's may reach the lock in either order, and the samples,
// the CSV and a read at a mark still go by time.
func TestSamplesStayInTimeOrder(t *testing.T) {
	s := NewSampler(time.Millisecond)
	s.Start()
	for range 200 {
		s.Mark("m")
	}
	s.Stop()
	if !slices.IsSortedFunc(s.Samples(), func(a, b Sample) int { return a.At.Compare(b.At) }) {
		t.Fatal("samples out of time order")
	}
}

func TestSamplerWritesCSV(t *testing.T) {
	// An hour between ticks leaves exactly the sample Start takes and the one
	// Stop takes.
	s := NewSampler(time.Hour)
	s.Start()
	s.Stop()
	p := filepath.Join(t.TempDir(), "samples.csv")
	if err := s.WriteCSV(p); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if lines[0] != "t_ms,heap_inuse,rss,goroutines" || len(lines) != 3 {
		t.Fatalf("csv = %q, want the header, a first and a last sample", string(raw))
	}
	for _, row := range lines[1:] {
		fields := strings.Split(row, ",")
		if len(fields) != 4 {
			t.Fatalf("row %q has %d fields", row, len(fields))
		}
		for _, f := range fields {
			if _, err := strconv.ParseInt(f, 10, 64); err != nil {
				t.Fatalf("row %q: %v", row, err)
			}
		}
		if ms, _ := strconv.ParseInt(fields[0], 10, 64); ms < 0 || ms > 60_000 {
			t.Fatalf("row %q: t_ms does not count from Start", row)
		}
	}
	if err := s.WriteCSV(filepath.Join(t.TempDir(), "missing", "samples.csv")); err == nil {
		t.Fatal("WriteCSV into a missing directory returned nil")
	}
}

func TestSamplerStartAndStopAreForgiving(t *testing.T) {
	if got := NewSampler(0).every; got != time.Second {
		t.Fatalf("NewSampler(0) samples every %s, want 1s", got)
	}
	s := NewSampler(time.Millisecond)
	s.Stop()
	s.Start()
	s.Start()
	waitSamples(t, s, 3)
	s.Stop()
	s.Stop()
	n := len(s.Samples())
	time.Sleep(20 * time.Millisecond)
	if got := len(s.Samples()); got != n {
		t.Fatalf("%d samples after Stop, then %d: a second Start left a loop running", n, got)
	}
}

func TestParseVMRSSReadsTheStatusLine(t *testing.T) {
	for _, c := range []struct {
		name, status string
		want         uint64
	}{
		{"kB to bytes", "Name:\tgate.test\nVmPeak:\t  90000 kB\nVmRSS:\t   51200 kB\nThreads:\t9\n", 50 << 20},
		{"no line", "Name:\tgate.test\nThreads:\t9\n", 0},
		{"no number", "VmRSS:\n", 0},
		{"not a number", "VmRSS:\t12x kB\n", 0},
	} {
		if got := parseVMRSS(c.status); got != c.want {
			t.Errorf("%s: parseVMRSS = %d, want %d", c.name, got, c.want)
		}
	}
	if runtime.GOOS == "linux" && readRSS() == 0 {
		t.Fatal("readRSS = 0 on linux")
	}
}
