package salutejazz

import (
	"testing"

	"github.com/openlibrecommunity/olcrtc/internal/engine"
)

// ai-generated: the whole file (olcrtc#49).

// What the external test package takes from this one. The tunnel test there
// runs the client and the server, which reach this package through the engine
// registry, so it cannot be a test of this package; it gets the fake SFU and
// the window's bound from here.

// RelayWindowBound is the most a leg may hold toward one destination while
// the relay window works (windowBound).
const RelayWindowBound = windowBound

// RelayWindow is what a sender may have in flight toward one destination
// (relayWindow).
const RelayWindow = relayWindow

// FakeRoom is the fake SFU (fakeconnector_test.go) and the one room on it the
// tunnel test joins.
type FakeRoom struct {
	url  string
	fake *fakeSFU
}

// NewFakeRoom starts the fake SFU for t.
func NewFakeRoom(t *testing.T) *FakeRoom {
	t.Helper()
	url, fake := newFakeConnector(t)
	return &FakeRoom{url: url, fake: fake}
}

// Join is cfg with what the auth provider would have issued for the room:
// the connector's URL, the room's password and its code.
func (r *FakeRoom) Join(cfg engine.Config) engine.Config {
	cfg.URL, cfg.Token = r.url, "passw0rd"
	cfg.Extra = map[string]string{credentialKeyRoomID: "abc123"}
	return cfg
}

// SlowLeg drains the leg toward identity at rate bytes a second (see
// fakeSFU.slowLeg). An identity the fake has not admitted fails t: the fake
// would put the leg nowhere, and a test that meant to run on it would pass
// without one.
func (r *FakeRoom) SlowLeg(t *testing.T, identity string, rate int) {
	t.Helper()
	if r.fake.peer(identity) == nil {
		t.Fatalf("no participant %q in the fake room to put a slow leg in front of", identity)
	}
	r.fake.slowLeg(identity, rate)
}

// QueuedTo is what the leg toward identity holds now.
func (r *FakeRoom) QueuedTo(identity string) int { return r.fake.queuedTo(identity) }

// PeakQueuedTo is the most the leg toward identity has held.
func (r *FakeRoom) PeakQueuedTo(identity string) int { return r.fake.peakQueuedTo(identity) }

// LastError is what the fake itself tripped over, or "".
func (r *FakeRoom) LastError() string { return r.fake.lastError() }
