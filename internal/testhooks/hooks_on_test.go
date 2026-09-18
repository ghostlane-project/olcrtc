//go:build olcrtc_testhooks

package testhooks

import (
	"testing"
	"time"
)

// ai-generated: whole file, cover for the tagged build's bridge delay.

const tagged = true

func TestDelayIsHonoured(t *testing.T) {
	t.Setenv("OLCRTC_TEST_BRIDGE_DELAY", "300ms")
	start := time.Now()
	BeforeBridgeOpen()
	if d := time.Since(start); d < 300*time.Millisecond {
		t.Fatalf("BeforeBridgeOpen returned after %v, want >= 300ms", d)
	}
}

// The gate starts every local server with the variable set, to "0s" when a
// scenario wants no delay; that, and anything that is not a positive
// duration, must leave the bridge undelayed.
func TestNoDelayUnlessPositive(t *testing.T) {
	for _, v := range []string{"0s", "-2s", "2"} {
		t.Run(v, func(t *testing.T) {
			t.Setenv("OLCRTC_TEST_BRIDGE_DELAY", v)
			start := time.Now()
			BeforeBridgeOpen()
			if d := time.Since(start); d > atOnce {
				t.Fatalf("BeforeBridgeOpen slept %v on %q", d, v)
			}
		})
	}
}
