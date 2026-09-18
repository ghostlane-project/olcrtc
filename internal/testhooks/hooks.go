//go:build olcrtc_testhooks

// Package testhooks holds knobs a test build may turn that a release build
// does not even contain. With the olcrtc_testhooks tag the functions here read
// the environment; without it they are empty and the compiler drops them.
package testhooks

import (
	"os"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
)

// ai-generated: the whole file.

// Enabled reports whether this binary was built with the hooks.
const Enabled = true

// bridgeDelayEnv names the variable BeforeBridgeOpen reads.
const bridgeDelayEnv = "OLCRTC_TEST_BRIDGE_DELAY"

// BeforeBridgeOpen sleeps for OLCRTC_TEST_BRIDGE_DELAY (a Go duration) before
// a Jitsi session opens its bridge. The gate's S6 uses it to make a server
// come up after the client, the ordering that lost the hello in olcbox#22.
// An unset, zero or negative value delays nothing. So does one that is not a
// duration, but that one is logged: a mistyped delay must not pass unnoticed
// for a run that tested a late bridge.
func BeforeBridgeOpen() {
	v := os.Getenv(bridgeDelayEnv)
	if v == "" {
		return
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		logger.Warnf("testhooks: %s=%q ignored: %v", bridgeDelayEnv, v, err)
		return
	}
	if d <= 0 {
		return
	}
	logger.Infof("testhooks: bridge opens %s late", d)
	time.Sleep(d)
}
