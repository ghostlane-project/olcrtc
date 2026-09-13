//go:build !olcrtc_lean

package builtin

import (
	"github.com/openlibrecommunity/olcrtc/internal/engine"
	"github.com/openlibrecommunity/olcrtc/internal/engine/livekit"
)

// registerLivekitEngine adds the livekit engine, the default for the "none"
// provider. See engine_livekit_lean.go for the build that leaves it out.
func registerLivekitEngine() {
	engine.Register("livekit", livekit.New)
}
