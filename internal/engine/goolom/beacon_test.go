package goolom

import (
	"context"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/openlibrecommunity/olcrtc/internal/engine"
)

// newBeaconSession is a session built far enough to carry (or not carry) a
// beacon: New stores the beacon track before any connection exists, which is
// all these tests need of it.
func newBeaconSession(t *testing.T, beacon bool) *Session {
	t.Helper()
	s, err := New(context.Background(), engine.Config{
		URL:   "wss://example.invalid",
		Token: "peer",
		Extra: map[string]string{
			credentialKeyRoomID:      "room",
			credentialKeyCredentials: "creds",
		},
		Beacon: beacon,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s.(*Session)
}

// A session asked for a beacon carries exactly one video track - the beacon's
// - stored before the publisher connection exists, so it is attached when
// that connection is negotiated. A session not asked carries none.
func TestTheBeaconRidesOnlyWhenAsked(t *testing.T) {
	on := newBeaconSession(t, true)
	tracks := 0
	on.RangeVideoTracks(func(_ webrtc.TrackLocal, _ bool) { tracks++ })
	if tracks != 1 {
		t.Fatalf("a beacon session holds %d video tracks, want the one beacon", tracks)
	}
	off := newBeaconSession(t, false)
	off.RangeVideoTracks(func(_ webrtc.TrackLocal, _ bool) { tracks++ })
	if n := tracks - 1; n != 0 {
		t.Fatalf("a session without the beacon holds %d video tracks, want none", n)
	}
}

// Closing the session leaves the beacon's writer no goroutine behind.
func TestTheBeaconStopsWithTheSession(t *testing.T) {
	s := newBeaconSession(t, true)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	done := make(chan struct{})
	go func() { s.beaconWg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("the beacon's writer did not leave within a second of Close")
	}
}
