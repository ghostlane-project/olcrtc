package gate

import (
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"os"
	"slices"
	"sync"
	"time"
)

// ai-generated: the whole file (the recorder and gate-report.json).

// ReportSchema is the version of gate-report.json this package writes.
const ReportSchema = 1

// Cell statuses. A cell is planned until it finishes; a report turns a cell
// still planned into a failure.
const (
	StatusPlanned = "planned"
	StatusPass    = "pass"
	StatusFail    = "fail"
)

const (
	reasonDidNotRun     = "did not run"
	reasonNotPlanned    = "cell was not planned"
	reasonFinishedTwice = "cell finished twice"
)

// Cell is one scenario on one pair with one client, and what it measured.
type Cell struct {
	ID         string             `json:"id"`
	Platform   string             `json:"platform"`
	Provider   string             `json:"provider"`
	Transport  string             `json:"transport"`
	Client     string             `json:"client"`
	Scenario   string             `json:"scenario"`
	Status     string             `json:"status"` // planned, pass or fail
	Metrics    map[string]float64 `json:"metrics"`
	Thresholds map[string]float64 `json:"thresholds"`
	Failures   []string           `json:"failures"`
	Log        string             `json:"log,omitempty"`
	DurationS  float64            `json:"duration_s"`
}

// Report is gate-report.json, schema 1.
type Report struct {
	Schema       int     `json:"schema"`
	EngineCommit string  `json:"engine_commit"`
	EngineRef    string  `json:"engine_ref"`
	AppVersion   string  `json:"app_version"`
	Target       string  `json:"target"`
	Runner       string  `json:"runner"`
	StartedAt    string  `json:"started_at"`
	DurationS    float64 `json:"duration_s"`
	Planned      int     `json:"planned"`
	Executed     int     `json:"executed"`
	Passed       int     `json:"passed"`
	Failed       int     `json:"failed"`
	Cells        []Cell  `json:"cells"`
}

// Recorder holds every planned cell to an outcome. It is safe for concurrent
// use, and what it hands out is a copy.
type Recorder struct {
	mu      sync.Mutex
	meta    Report
	started time.Time
	cells   map[string]Cell
}

// NewRecorder starts a report with the run's metadata filled in. Counts and
// cells in meta are ignored: they come from Plan and Finish alone.
func NewRecorder(meta Report) *Recorder {
	meta.Schema = ReportSchema
	if meta.StartedAt == "" {
		meta.StartedAt = time.Now().UTC().Format(time.RFC3339)
	}
	return &Recorder{meta: meta, started: time.Now(), cells: map[string]Cell{}}
}

// Plan registers the cells that must end pass or fail. A plan carries no
// outcome: a new cell starts planned whatever it says, and a cell already
// known keeps what it has, so planning again never erases a result.
func (r *Recorder) Plan(cells []Cell) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range cells {
		if _, known := r.cells[c.ID]; known {
			continue
		}
		r.cells[c.ID] = Cell{
			ID: c.ID, Platform: c.Platform, Provider: c.Provider, Transport: c.Transport,
			Client: c.Client, Scenario: c.Scenario, Status: StatusPlanned,
		}
	}
}

// Finish records a cell's outcome: pass when nothing failed. A cell nobody
// planned, a second outcome for one cell, and a number JSON cannot hold (NaN,
// an infinity) each fail the cell with a reason rather than hide in it: the
// first two are runner bugs, the last would stop the report being written.
// Finish keeps copies, so the caller may reuse what it passed.
func (r *Recorder) Finish(
	id string, m Metrics, thresholds map[string]float64, failures []string, logPath string, took time.Duration,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, known := r.cells[id]
	var reasons []string
	switch {
	case !known:
		c = Cell{ID: id}
		reasons = append(reasons, reasonNotPlanned)
	case c.Status != StatusPlanned:
		reasons = append(reasons, reasonFinishedTwice)
		reasons = append(reasons, c.Failures...)
	}
	metrics, bad := finiteOnly("metric", m)
	reasons = append(reasons, bad...)
	limits, bad := finiteOnly("threshold", thresholds)
	reasons = append(reasons, bad...)
	reasons = append(reasons, failures...)
	c.Metrics, c.Thresholds, c.Failures = metrics, limits, reasons
	c.Log = logPath
	c.DurationS = took.Seconds()
	c.Status = StatusPass
	if len(reasons) > 0 {
		c.Status = StatusFail
	}
	r.cells[id] = c
}

// Report builds the report: every cell by ID, a cell still planned as a
// failure that did not run, and the counts. Planned counts every cell, one
// finished without a plan included, so it is always Passed plus Failed;
// Executed leaves out the cells that never ran.
func (r *Recorder) Report() Report {
	r.mu.Lock()
	defer r.mu.Unlock()
	rep := r.meta
	rep.DurationS = time.Since(r.started).Seconds()
	rep.Planned, rep.Executed, rep.Passed, rep.Failed = len(r.cells), 0, 0, 0
	rep.Cells = make([]Cell, 0, len(r.cells))
	for _, id := range slices.Sorted(maps.Keys(r.cells)) {
		c := snapshot(r.cells[id])
		switch c.Status {
		case StatusPass:
			rep.Executed++
			rep.Passed++
		case StatusFail:
			rep.Executed++
			rep.Failed++
		default:
			c.Status = StatusFail
			c.Failures = []string{reasonDidNotRun}
			rep.Failed++
		}
		rep.Cells = append(rep.Cells, c)
	}
	return rep
}

// Write stores the report as indented JSON.
func (r *Recorder) Write(path string) error {
	raw, err := json.MarshalIndent(r.Report(), "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}
	if err := os.WriteFile(path, append(raw, '\n'), 0o644); err != nil { //nolint:gosec // a report, not a secret
		return fmt.Errorf("write report: %w", err)
	}
	return nil
}

// finiteOnly copies m without the values JSON cannot encode (NaN and the
// infinities) and names each one it left out, in key order.
func finiteOnly(kind string, m map[string]float64) (map[string]float64, []string) {
	out := make(map[string]float64, len(m))
	var dropped []string
	for _, k := range slices.Sorted(maps.Keys(m)) {
		if v := m[k]; math.IsNaN(v) || math.IsInf(v, 0) {
			dropped = append(dropped, fmt.Sprintf("%s %s is %v, not a finite number", kind, k, v))
			continue
		}
		out[k] = m[k]
	}
	return out, dropped
}

// snapshot copies a cell's maps and list, never nil, so a report shares no
// memory with the recorder and its JSON says {} and [] instead of null.
func snapshot(c Cell) Cell {
	out := c
	out.Metrics = make(map[string]float64, len(c.Metrics))
	maps.Copy(out.Metrics, c.Metrics)
	out.Thresholds = make(map[string]float64, len(c.Thresholds))
	maps.Copy(out.Thresholds, c.Thresholds)
	out.Failures = append(make([]string, 0, len(c.Failures)), c.Failures...)
	return out
}
