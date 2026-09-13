//go:build olcrtc_lean

package builtin

// The olcrtc_lean build tag (see internal/app/session/transport_video_lean.go)
// leaves the livekit engine out: no provider a phone talks to is served by
// it, and its SDK registers protobuf descriptors and a CEL validator at
// init, heap that a packet tunnel extension paid for and never used. The
// "none" provider still defaults to the livekit name and fails at session
// start with ErrEngineNotFound unless another engine is named.
func registerLivekitEngine() {}
