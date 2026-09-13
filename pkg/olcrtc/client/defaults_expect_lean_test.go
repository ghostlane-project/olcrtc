//go:build olcrtc_lean

package client_test

// What a lean build registers: no livekit engine, no videochannel. This is
// the contract the mobile bind relies on when it passes the tag.
func expectedEngines() []string { return []string{"goolom", "jitsi"} }

func expectedTransports() []string { return []string{"datachannel", "seichannel", "vp8channel"} }
