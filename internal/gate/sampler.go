package gate

import (
	"fmt"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ai-generated: the whole file (the memory sampler).

// Sample is one reading of the test process, which is the client's process:
// the server runs as a child, so what is sampled here is the client alone.
type Sample struct {
	At         time.Time
	HeapInuse  uint64
	RSS        uint64 // 0 where /proc/self/status is not there to read
	Goroutines int
}

// Sampler reads memory and goroutines on a ticker and remembers named
// moments, so a scenario can ask for the peak over a window. It is safe for
// concurrent use.
type Sampler struct {
	every   time.Duration
	mu      sync.Mutex
	samples []Sample
	marks   map[string]time.Time
	start   time.Time
	stop    chan struct{}
	done    chan struct{}
}

// NewSampler samples every interval once started. An interval that is not
// positive means one second, the gate's rate, instead of a ticker panic.
func NewSampler(every time.Duration) *Sampler {
	if every <= 0 {
		every = time.Second
	}
	return &Sampler{every: every, marks: map[string]time.Time{}}
}

// Start begins sampling in a goroutine, one sample at once and one per tick.
// A sampler already running is left alone, so no loop outlives Stop.
func (s *Sampler) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stop != nil {
		return
	}
	if s.start.IsZero() {
		s.start = time.Now()
	}
	s.stop, s.done = make(chan struct{}), make(chan struct{})
	go s.loop(s.stop, s.done)
}

// Stop takes a last sample, ends the loop and waits for it. Calling it again,
// or before Start, does nothing.
func (s *Sampler) Stop() {
	s.mu.Lock()
	stop, done := s.stop, s.done
	s.stop, s.done = nil, nil
	s.mu.Unlock()
	if stop == nil {
		return
	}
	close(stop)
	<-done
}

// loop samples at once, on every tick, and a last time when stop closes.
func (s *Sampler) loop(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	t := time.NewTicker(s.every)
	defer t.Stop()
	s.take()
	for {
		select {
		case <-stop:
			s.take()
			return
		case <-t.C:
			s.take()
		}
	}
}

// take appends one reading of the process.
func (s *Sampler) take() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	sm := Sample{At: time.Now(), HeapInuse: m.HeapInuse, RSS: readRSS(), Goroutines: runtime.NumGoroutine()}
	s.mu.Lock()
	s.samples = append(s.samples, sm)
	s.mu.Unlock()
}

// Mark names now; marking a name again moves it.
func (s *Sampler) Mark(name string) {
	now := time.Now()
	s.mu.Lock()
	s.marks[name] = now
	s.mu.Unlock()
}

// PeakBetween returns the highest heap and the highest RSS sampled between
// two marks, both ends included, each on its own. It reports false when a
// mark is unknown or no sample falls inside the window.
func (s *Sampler) PeakBetween(fromMark, toMark string) (uint64, uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	from, okFrom := s.marks[fromMark]
	to, okTo := s.marks[toMark]
	if !okFrom || !okTo {
		return 0, 0, false
	}
	var heap, rss uint64
	seen := false
	for _, sm := range s.samples {
		if sm.At.Before(from) || sm.At.After(to) {
			continue
		}
		seen = true
		heap = max(heap, sm.HeapInuse)
		rss = max(rss, sm.RSS)
	}
	return heap, rss, seen
}

// Samples returns a copy of everything sampled so far.
func (s *Sampler) Samples() []Sample {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.samples)
}

// WriteCSV stores the samples for the run's artifacts, one row each under the
// header t_ms,heap_inuse,rss,goroutines; t_ms counts from the first Start.
func (s *Sampler) WriteCSV(path string) error {
	if err := os.WriteFile(path, s.renderCSV(), 0o644); err != nil { //nolint:gosec // samples, not a secret
		return fmt.Errorf("write samples csv: %w", err)
	}
	return nil
}

// renderCSV formats the samples under the lock, so WriteCSV writes the file
// outside it.
func (s *Sampler) renderCSV() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]byte, 0, 64*(len(s.samples)+1))
	out = append(out, "t_ms,heap_inuse,rss,goroutines\n"...)
	for _, sm := range s.samples {
		out = fmt.Appendf(out, "%d,%d,%d,%d\n",
			sm.At.Sub(s.start).Milliseconds(), sm.HeapInuse, sm.RSS, sm.Goroutines)
	}
	return out
}

// readRSS reads the process's resident set in bytes from /proc on Linux, and
// is 0 elsewhere.
func readRSS() uint64 {
	raw, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	return parseVMRSS(string(raw))
}

// parseVMRSS finds the VmRSS line of a /proc status file and returns it in
// bytes, or 0 when the line is missing or malformed.
func parseVMRSS(status string) uint64 {
	for line := range strings.Lines(status) {
		rest, ok := strings.CutPrefix(line, "VmRSS:")
		if !ok {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			return 0
		}
		kb, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return 0
		}
		return kb << 10
	}
	return 0
}
