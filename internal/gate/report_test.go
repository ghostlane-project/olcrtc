package gate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ai-generated: whole file, unit cover for the recorder and the report writer.

func TestRecorderCountsAndMarksUnrunCellsFailed(t *testing.T) {
	r := NewRecorder(Report{EngineCommit: "abc", Target: "local"})
	r.Plan([]Cell{{ID: "p/a/b/cli/S0"}, {ID: "p/a/b/cli/S1"}, {ID: "p/a/b/cli/S2"}})
	r.Finish("p/a/b/cli/S0", Metrics{"pull_ok": 1}, map[string]float64{}, nil, "", 2*time.Second)
	r.Finish("p/a/b/cli/S1", Metrics{"connect_ok": 3}, map[string]float64{"connect_p95_ms": 5000},
		[]string{"connect_p95_ms 6100 > 5000"}, "logs/x.log", time.Second)
	rep := r.Report()
	if rep.Planned != 3 || rep.Executed != 2 || rep.Passed != 1 || rep.Failed != 2 {
		t.Fatalf("counts = planned %d executed %d passed %d failed %d",
			rep.Planned, rep.Executed, rep.Passed, rep.Failed)
	}
	byID := map[string]Cell{}
	for _, c := range rep.Cells {
		byID[c.ID] = c
	}
	if byID["p/a/b/cli/S2"].Status != "fail" || byID["p/a/b/cli/S2"].Failures[0] != "did not run" {
		t.Fatalf("unrun cell = %+v", byID["p/a/b/cli/S2"])
	}
	if byID["p/a/b/cli/S1"].Status != "fail" || byID["p/a/b/cli/S1"].DurationS != 1 {
		t.Fatalf("failed cell = %+v", byID["p/a/b/cli/S1"])
	}
	if byID["p/a/b/cli/S0"].Status != "pass" || byID["p/a/b/cli/S0"].Metrics["pull_ok"] != 1 {
		t.Fatalf("passed cell = %+v", byID["p/a/b/cli/S0"])
	}
}

func TestRecorderFinishUnknownCellIsRecordedAsFailure(t *testing.T) {
	r := NewRecorder(Report{})
	r.Plan(nil)
	r.Finish("p/x/y/cli/S9", nil, nil, nil, "", 0)
	rep := r.Report()
	if rep.Failed != 1 || rep.Cells[0].Failures[0] != "cell was not planned" {
		t.Fatalf("unplanned cell = %+v", rep.Cells)
	}
}

func TestWriteProducesSchemaOne(t *testing.T) {
	r := NewRecorder(Report{EngineCommit: "abc", Target: "local", Runner: "ubuntu"})
	r.Plan([]Cell{{ID: "p/a/b/cli/S0"}})
	r.Finish("p/a/b/cli/S0", Metrics{"pull_ok": 1}, nil, nil, "", time.Second)
	path := filepath.Join(t.TempDir(), "gate-report.json")
	if err := r.Write(path); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var back Report
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if back.Schema != 1 || back.EngineCommit != "abc" || back.Passed != 1 || len(back.Cells) != 1 {
		t.Fatalf("written report = %+v", back)
	}
}
