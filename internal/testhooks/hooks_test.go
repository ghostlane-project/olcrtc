package testhooks

import (
	"testing"
	"time"
)

// ai-generated: whole file, cover for what the hooks do in every build.

// atOnce bounds how long a hook with nothing to do may take.
const atOnce = 50 * time.Millisecond

// Whatever the build, BeforeBridgeOpen must return at once when no delay is
// configured.
func TestBeforeBridgeOpenIsFreeWithoutDelay(t *testing.T) {
	t.Setenv("OLCRTC_TEST_BRIDGE_DELAY", "")
	start := time.Now()
	BeforeBridgeOpen()
	if d := time.Since(start); d > atOnce {
		t.Fatalf("BeforeBridgeOpen took %v with no delay configured", d)
	}
}

func TestEnabledMatchesTheBuild(t *testing.T) {
	if Enabled != tagged {
		t.Fatalf("Enabled = %v, want %v for this build", Enabled, tagged)
	}
}
