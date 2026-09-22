package salutejazz

import (
	"context"
	"encoding/json"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/engine"
)

// TestJoinConfigOfferAnswerPing walks the whole join handshake against the
// connector fake: the frames that drive it arrive in the order the capture
// recorded, the join this engine actually built has the captured shape, the
// SFU's subscriber offer is answered, and a ping is answered with a pong.
func TestJoinConfigOfferAnswerPing(t *testing.T) {
	url, fake := newFakeConnector(t)
	sess, err := New(context.Background(), engine.Config{
		URL: url, Token: "passw0rd", Name: "test",
		Extra: map[string]string{"roomID": "abc123"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sess.Close() })
	// Sized past the handshake: a hook that blocks would stall the reader.
	got := make(chan string, 8)
	sess.(*Session).onJoinPayload = func(ev string) { got <- ev }
	if err := sess.Connect(context.Background()); err != nil {
		t.Fatalf("connect: %v (fake: %s)", err, fake.lastError())
	}
	for _, want := range []string{"join-response", "rtc:config", "rtc:offer"} {
		select {
		case ev := <-got:
			if ev != want {
				t.Fatalf("got %q want %q", ev, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for %q", want)
		}
	}
	// ping answered inside 2 s
	if err := sess.(*Session).pingOnce(); err != nil {
		t.Fatal(err)
	}
	if !fake.sawAnswer() {
		t.Fatal("fake SFU never got the SDP answer")
	}

	// What the connector received, not what a literal in this file says.
	joins := fake.joins()
	if len(joins) != 1 {
		t.Fatalf("joins = %d, want 1", len(joins))
	}
	if joins[0].group != "" {
		t.Fatalf("the join carried a group id %q; the capture has none", joins[0].group)
	}
	if payload := redactPassword(joins[0].payload); payload != wantJoinPayload("test") {
		t.Fatalf("join payload\n got %s\nwant %s", payload, wantJoinPayload("test"))
	}
	if failure := fake.lastError(); failure != "" {
		t.Fatalf("fake SFU error: %s", failure)
	}
}

// TestPublisherPeerConnectionNegotiates pins the finding that shaped this
// engine: the channels the SFU creates on the subscriber PC carry nothing
// out, so the client offers a publisher PC of its own and sends on that.
// Both peer connections must reach connected and both publisher channels
// must open.
func TestPublisherPeerConnectionNegotiates(t *testing.T) {
	url, fake := newFakeConnector(t)
	sess := connectSession(t, url, "publisher")

	if !fake.sawPublisherOffer() {
		t.Fatal("fake SFU never got the client's publisher offer")
	}
	if !fake.sawAnswer() {
		t.Fatal("fake SFU never got the SDP answer")
	}
	waitFor(t, 5*time.Second, "both peer connections connected", func() bool {
		return sess.subscriberConnected() && sess.publisherConnected()
	})
	waitFor(t, 5*time.Second, "both publisher channels open", func() bool {
		return channelOpen(sess, labelReliable) && channelOpen(sess, labelLossy)
	})
	if !sess.SubscriberCanSend() || !sess.CanSend() {
		t.Fatalf("session not ready: subscriber=%v publisher=%v", sess.SubscriberCanSend(), sess.CanSend())
	}
	if failure := fake.lastError(); failure != "" {
		t.Fatalf("fake SFU error: %s", failure)
	}
}

// TestRTCConfigReachesPionNormalised covers the TURN path: what rtc:config
// advertises is what the peer connections are built with, after the shared
// normaliser has been over it.
func TestRTCConfigReachesPionNormalised(t *testing.T) {
	url, _ := newFakeConnector(t)
	sess := connectSession(t, url, "ice")

	servers := sess.iceServers()
	if len(servers) != 1 || len(servers[0].URLs) != 1 || servers[0].URLs[0] != fakeSTUNURL {
		t.Fatalf("ice servers = %+v, want one %s entry", servers, fakeSTUNURL)
	}
}

// TestParticipantsUpdateFillsTheIdentityMap covers rtc:participants:update:
// the roster names everyone else in the room under the identity a data
// packet is addressed to, and never lists this session as its own peer.
func TestParticipantsUpdateFillsTheIdentityMap(t *testing.T) {
	url, fake := newFakeConnector(t)
	first := connectSession(t, url, "first")
	second := connectSession(t, url, "second")

	if first.localIdentity() == "" || first.localIdentity() == second.localIdentity() {
		t.Fatalf("identities %q and %q", first.localIdentity(), second.localIdentity())
	}
	waitFor(t, 5*time.Second, "the first session to see the second", func() bool {
		peers := first.remoteIdentities()
		return len(peers) == 1 && peers[0] == second.localIdentity()
	})
	waitFor(t, 5*time.Second, "the second session to see the first", func() bool {
		peers := second.remoteIdentities()
		return len(peers) == 1 && peers[0] == first.localIdentity()
	})
	if failure := fake.lastError(); failure != "" {
		t.Fatalf("fake SFU error: %s", failure)
	}
}

// TestRejoinCarriesNoStaleGroup pins what the capture says about a rejoin:
// the join frame names no group, because the group belongs to the join that
// is being replaced. Identity and group live on the connection attempt, so a
// second attempt starts without either.
func TestRejoinCarriesNoStaleGroup(t *testing.T) {
	url, fake := newFakeConnector(t)
	sess := connectSession(t, url, "rejoin")

	firstGroup := sess.current().groupID()
	firstIdentity := sess.localIdentity()
	if firstGroup == "" || firstIdentity == "" {
		t.Fatalf("after the join: group %q identity %q", firstGroup, firstIdentity)
	}
	if err := sess.reconnect(context.Background()); err != nil {
		t.Fatalf("reconnect: %v (fake: %s)", err, fake.lastError())
	}

	joins := fake.joins()
	if len(joins) != 2 {
		t.Fatalf("joins = %d, want 2", len(joins))
	}
	for i, join := range joins {
		if join.group != "" {
			t.Fatalf("join %d carried a group id %q", i, join.group)
		}
	}
	if group := sess.current().groupID(); group == "" {
		t.Fatal("the rejoin never learned its own group")
	}
	if identity := sess.localIdentity(); identity == firstIdentity {
		t.Fatalf("the rejoin kept the old identity %q", identity)
	}
	// The fake fails a join that carries a group and a later frame that
	// carries the wrong one.
	if failure := fake.lastError(); failure != "" {
		t.Fatalf("fake SFU error: %s", failure)
	}
}

// TestTeardownRunsOnce pins the lifecycle invariant the review found broken:
// Close and a Connect that is giving up both own the same generation, and a
// teardown that only closes its signal channel when it looks open closes it
// twice the moment the two overlap.
func TestTeardownRunsOnce(t *testing.T) {
	url, _ := newFakeConnector(t)
	sess := connectSession(t, url, "teardown")
	gen := sess.current()

	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 16 {
		wg.Go(func() {
			<-start
			sess.teardown(gen)
		})
	}
	close(start)
	wg.Wait()
}

// TestCloseDuringConnectTearsDownOnce is the same race through the public
// surface: Close lands while Connect is still waiting for a join-response
// the connector is holding, so both end the same generation.
func TestCloseDuringConnectTearsDownOnce(t *testing.T) {
	url, fake := newFakeConnector(t)
	release := fake.holdJoins()
	t.Cleanup(release)

	sess, err := New(context.Background(), engine.Config{
		URL: url, Token: "passw0rd", Name: "race",
		Extra: map[string]string{"roomID": "abc123"},
	})
	if err != nil {
		t.Fatal(err)
	}
	sj := sess.(*Session)
	sj.joinTimeout = 300 * time.Millisecond

	connected := make(chan error, 1)
	go func() { connected <- sess.Connect(context.Background()) }()
	waitFor(t, 5*time.Second, "the join to reach the connector", func() bool {
		return len(fake.joins()) == 1
	})

	closed := make(chan error, 1)
	go func() { closed <- sess.Close() }()

	select {
	case err := <-connected:
		if err == nil {
			t.Fatal("connect succeeded while the join was held")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("connect never returned")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("close never returned")
	}
	// Closing again, and connecting again, must both be safe.
	if err := sess.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if err := sess.Connect(context.Background()); err == nil {
		t.Fatal("connect succeeded on a closed session")
	}
}

// TestClientFramesMatchTheCapturedShape pins the bytes this engine puts on
// the connector. What the service sees has to be what its own web client
// sends, field for field and in its order: the capture of that client is
// where every literal below comes from.
func TestClientFramesMatchTheCapturedShape(t *testing.T) {
	sess, err := New(context.Background(), engine.Config{
		URL: "wss://example.invalid/connector", Token: "passw0rd", Name: "Spike Tester",
		Extra: map[string]string{"roomID": "abc123"},
	})
	if err != nil {
		t.Fatal(err)
	}
	sj := sess.(*Session)
	gen := newGeneration(nil)

	// The join carries no group id: the connector names the group in its
	// answer to this very frame.
	got := frameJSON(t, sj, gen, eventJoin, joinRequest{
		Password:        "passw0rd",
		ParticipantName: "Spike Tester",
		SupportedFeatures: supportedFeatures{
			AttachedRooms: true, SessionGroups: true, Transcription: true, Interpretation: true,
		},
	})
	want := `{"roomId":"abc123","payload":` + wantJoinPayload("Spike Tester") +
		`,"event":"join","requestId":"REQ"}`
	if got != want {
		t.Fatalf("join frame\n got %s\nwant %s", got, want)
	}

	// Everything after the join echoes the group id.
	storeString(&gen.group, "8a7046f5-27b8-427a-abda-5ee6ceb2c205")

	got = frameJSON(t, sj, gen, eventMediaIn, mediaIn{
		Method:      methodAnswer,
		Description: &sdpOut{SDP: "v=0\r\n", Type: sdpTypeAnswer},
	})
	want = `{"roomId":"abc123","payload":{"method":"rtc:answer","description":{"sdp":"v=0\r\n",` +
		`"type":"answer"}},"event":"media-in","groupId":"8a7046f5-27b8-427a-abda-5ee6ceb2c205",` +
		`"requestId":"REQ"}`
	if got != want {
		t.Fatalf("answer frame\n got %s\nwant %s", got, want)
	}

	mid, index, ufrag := "0", uint16(0), "+E5u"
	got = frameJSON(t, sj, gen, eventMediaIn, mediaIn{
		Method: methodICE,
		Candidates: []iceCandidate{{
			Candidate:        "candidate:984223960 1 udp 2122260223 10.66.66.6 55404 typ host",
			SDPMid:           &mid,
			SDPMLineIndex:    &index,
			UsernameFragment: &ufrag,
			Target:           targetSubscriber,
		}},
	})
	want = `{"roomId":"abc123","payload":{"method":"rtc:ice","rtcIceCandidates":[{"candidate":` +
		`"candidate:984223960 1 udp 2122260223 10.66.66.6 55404 typ host","sdpMid":"0",` +
		`"sdpMLineIndex":0,"usernameFragment":"+E5u","target":"SUBSCRIBER"}]},"event":"media-in",` +
		`"groupId":"8a7046f5-27b8-427a-abda-5ee6ceb2c205","requestId":"REQ"}`
	if got != want {
		t.Fatalf("ice frame\n got %s\nwant %s", got, want)
	}

	got = frameJSON(t, sj, gen, eventMediaIn, mediaIn{
		Method:  methodPing,
		PingReq: &pingRequest{Timestamp: 1790035953804, RTT: 0},
	})
	want = `{"roomId":"abc123","payload":{"method":"rtc:ping","ping_req":{"timestamp":1790035953804,` +
		`"rtt":0}},"event":"media-in","groupId":"8a7046f5-27b8-427a-abda-5ee6ceb2c205","requestId":"REQ"}`
	if got != want {
		t.Fatalf("ping frame\n got %s\nwant %s", got, want)
	}
}

// connectSession joins the fake and hands back the connected session.
func connectSession(t *testing.T, url, name string) *Session {
	t.Helper()
	sess, err := New(context.Background(), engine.Config{
		URL: url, Token: "passw0rd", Name: name,
		Extra: map[string]string{"roomID": "abc123"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sess.Close() })
	if err := sess.Connect(context.Background()); err != nil {
		t.Fatalf("connect %s: %v", name, err)
	}
	return sess.(*Session)
}

// wantJoinPayload is the join payload the web client sends, field for field
// and in its order, with the password redacted.
func wantJoinPayload(name string) string {
	return `{"password":"REDACTED","participantName":"` + name + `",` +
		`"supportedFeatures":{"attachedRooms":true,"sessionGroups":true,"transcription":true,` +
		`"interpretation":true},"isSilent":false}`
}

// passwordField matches the one field a recorded frame must never be
// compared - or printed - with its value.
var passwordField = regexp.MustCompile(`"password":"[^"]*"`)

func redactPassword(frame string) string {
	return passwordField.ReplaceAllString(frame, `"password":"REDACTED"`)
}

func channelOpen(s *Session, label string) bool {
	dc := s.publisherChannel(label == labelReliable)
	return dc != nil && dc.ReadyState().String() == "open"
}

// frameJSON marshals one outgoing frame with its random request id replaced,
// so a test can compare the rest byte for byte. The password is redacted:
// the shape is what is pinned, never the credential.
func frameJSON(t *testing.T, s *Session, gen *generation, event string, payload any) string {
	t.Helper()
	frame, err := s.envelope(gen, event, payload)
	if err != nil {
		t.Fatal(err)
	}
	frame.RequestID = "REQ"
	raw, err := json.Marshal(frame)
	if err != nil {
		t.Fatal(err)
	}
	return redactPassword(string(raw))
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout after %s waiting for %s", timeout, what)
}
