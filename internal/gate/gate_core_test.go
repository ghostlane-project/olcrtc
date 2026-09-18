package gate

import (
	"context"
	"testing"
)

// ai-generated: whole file, unit cover for the scenario registry and the plan.

type fakeTarget struct{ pairs []Pair }

func (fakeTarget) Name() string     { return "fake" }
func (fakeTarget) Platform() string { return "engine-test" }
func (f fakeTarget) Pairs() []Pair  { return f.pairs }
func (fakeTarget) Load() LoadURLs   { return LoadURLs{} }
func (fakeTarget) Open(context.Context, Pair, string, OpenOptions) (Endpoint, func(), error) {
	return Endpoint{}, func() {}, nil
}

// resetRegistryForTest gives a test an empty registry and puts the real one
// back when it ends.
func resetRegistryForTest(t *testing.T) {
	t.Helper()
	registryMu.Lock()
	saved := registry
	registry = nil
	registryMu.Unlock()
	t.Cleanup(func() {
		registryMu.Lock()
		registry = saved
		registryMu.Unlock()
	})
}

func TestPlanCellsEnumeratesEveryApplicableCell(t *testing.T) {
	resetRegistryForTest(t)
	Register(Scenario{ID: "S1", Name: "one", Applies: func(Target, Pair, string) bool { return true }})
	Register(Scenario{ID: "S0", Name: "zero", Applies: func(_ Target, _ Pair, c string) bool { return c == "mobile" }})
	target := fakeTarget{pairs: []Pair{{"jitsi", "datachannel"}, {"telemost", "vp8channel"}}}
	cells := PlanCells(target, []string{"cli", "mobile"})
	want := []string{
		"engine-test/jitsi/datachannel/mobile/S0",
		"engine-test/telemost/vp8channel/mobile/S0",
		"engine-test/jitsi/datachannel/cli/S1",
		"engine-test/jitsi/datachannel/mobile/S1",
		"engine-test/telemost/vp8channel/cli/S1",
		"engine-test/telemost/vp8channel/mobile/S1",
	}
	if len(cells) != len(want) {
		t.Fatalf("PlanCells returned %d cells, want %d: %+v", len(cells), len(want), cells)
	}
	for i, c := range cells {
		if c.ID != want[i] || c.Status != "planned" {
			t.Fatalf("cell %d = %q (%s), want %q (planned)", i, c.ID, c.Status, want[i])
		}
	}
}

func TestScenariosAreSortedByID(t *testing.T) {
	resetRegistryForTest(t)
	Register(Scenario{ID: "S7"})
	Register(Scenario{ID: "S2"})
	if got := Scenarios(); got[0].ID != "S2" || got[1].ID != "S7" {
		t.Fatalf("Scenarios() order = %s, %s", got[0].ID, got[1].ID)
	}
}
