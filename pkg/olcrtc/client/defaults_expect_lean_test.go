//go:build olcrtc_lean

package client_test

// What a lean build registers: every engine, no videochannel transport. This
// is the contract the mobile bind relies on when it passes the tag. livekit
// stays because the wbstream provider is served by it (olcbox#22); salutejazz
// stays too - it depends only on pion and the livekit/protocol wire types,
// never the livekit SDK, so the lean build has no reason to drop it.
func expectedEngines() []string { return []string{"goolom", "jitsi", "livekit", "salutejazz"} }

func expectedTransports() []string { return []string{"datachannel", "seichannel", "vp8channel"} }
