package salutejazz

import (
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

// TestEveryTrickledCandidateCarriesItsUfrag pins what the capture shows: all
// 21 client rtc:ice frames carry a usernameFragment, and pion's
// ICECandidate.ToJSON does not fill one in - it renders the candidate line,
// the mid and the m-line index and nothing else. The engine has to read the
// fragment off the transport that gathered the candidate, and it has to be
// that transport's: a subscriber candidate under the publisher's fragment
// names an ICE session the SFU is not running.
func TestEveryTrickledCandidateCarriesItsUfrag(t *testing.T) {
	url, fake := newFakeConnector(t)
	sess := connectSession(t, url, "ufrag")

	waitFor(t, 10*time.Second, "candidates for both peer connections", func() bool {
		seen := map[string]bool{}
		for _, cand := range fake.clientICE() {
			seen[cand.target] = true
		}
		return seen[targetSubscriber] && seen[targetPublisher]
	})

	gen := sess.current()
	want := map[string]string{
		targetSubscriber: localICEUfrag(gen.subPC.Load()),
		targetPublisher:  localICEUfrag(gen.pubPC.Load()),
	}
	for target, ufrag := range want {
		if ufrag == "" {
			t.Fatalf("the %s peer connection reports no local ICE fragment", target)
		}
	}
	for _, cand := range fake.clientICE() {
		if cand.ufrag == "" {
			t.Fatalf("a %s candidate reached the connector with no usernameFragment", cand.target)
		}
		if cand.ufrag != want[cand.target] {
			t.Fatalf("a %s candidate carried the fragment %q, want the one that peer connection gathered under",
				cand.target, cand.ufrag)
		}
	}
	if failure := fake.lastError(); failure != "" {
		t.Fatalf("fake SFU error: %s", failure)
	}
}

// TestTheUfragIsReadOffThePeerConnection covers the accessor on its own: a
// peer connection that has one answers with it, and every step of the walk
// down to the ICE transport is allowed to be missing.
func TestTheUfragIsReadOffThePeerConnection(t *testing.T) {
	if got := localICEUfrag(nil); got != "" {
		t.Fatalf("localICEUfrag(nil) = %q, want empty", got)
	}
	api, err := newWebRTCAPI(nil)
	if err != nil {
		t.Fatal(err)
	}
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	if got := localICEUfrag(pc); got == "" {
		t.Fatal("a live peer connection reports no local ICE fragment")
	}
}
