//go:build olcrtc_lean

package client_test

// What a lean build registers: every engine, no videochannel transport. This
// is the contract the mobile bind relies on when it passes the tag. livekit
// stays because the wbstream provider is served by it (olcbox#22).
func expectedEngines() []string { return []string{"goolom", "jitsi", "livekit"} }

func expectedTransports() []string { return []string{"datachannel", "seichannel", "vp8channel"} }
