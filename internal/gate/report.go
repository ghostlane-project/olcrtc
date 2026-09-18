package gate

import (
	"encoding/json"
	"fmt"
	"maps"
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
	reasonDidNotRun  = "did not run"
	reasonNotPlanned = "cell was not planned"
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
// use.
type Recorder struct {
	mu      sync.Mutex
	meta    Report
	started time.Time
	cells   map[string]Cell
}

// NewRecorder starts a report with the run's metadata filled in.
func NewRecorder(meta Report) *Recorder {
	meta.Schema = ReportSchema
	if meta.StartedAt == "" {
		meta.StartedAt = time.Now().UTC().Format(time.RFC3339)
	}
	return &Recorder{meta: meta, started: time.Now(), cells: map[string]Cell{}}
}

// Plan registers the cells that must end pass or fail.
func (r *Recorder) Plan(cells []Cell) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range cells {
		if c.Status == "" {
			c.Status = StatusPlanned
		}
		r.cells[c.ID] = c
	}
}

// Finish records a cell's outcome: pass when failures is empty.
func (r *Recorder) Finish(
	id string, m Metrics, thresholds map[string]float64, failures []string, logPath string, took time.Duration,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.cells[id]
	var reasons []string
	if !ok {
		c = Cell{ID: id}
		reasons = append(reasons, reasonNotPlanned)
	}
	reasons = append(reasons, failures...)
	c.Metrics = orEmpty(m)
	c.Thresholds = orEmpty(thresholds)
	c.Failures = reasons
	if c.Failures == nil {
		c.Failures = []string{}
	}
	c.Log = logPath
	c.DurationS = took.Seconds()
	c.Status = StatusPass
	if len(reasons) > 0 {
		c.Status = StatusFail
	}
	r.cells[id] = c
}

// Report builds the report: counts, and every still-planned cell as a failure.
// Planned counts every cell in it, including one finished without a plan.
func (r *Recorder) Report() Report {
	r.mu.Lock()
	defer r.mu.Unlock()
	rep := r.meta
	rep.DurationS = time.Since(r.started).Seconds()
	rep.Planned, rep.Executed, rep.Passed, rep.Failed = len(r.cells), 0, 0, 0
	rep.Cells = make([]Cell, 0, len(r.cells))
	for _, id := range slices.Sorted(maps.Keys(r.cells)) {
		c := r.cells[id]
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
			c.Metrics = orEmpty(c.Metrics)
			c.Thresholds = orEmpty(c.Thresholds)
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

// orEmpty returns m, or an empty map for nil so the JSON says {} not null.
func orEmpty[M ~map[string]float64](m M) map[string]float64 {
	if m == nil {
		return map[string]float64{}
	}
	return m
}
