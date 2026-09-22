package salutejazz

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/engine"
)

// TestJoinConfigOfferAnswerPing walks the whole join handshake against the
// connector fake: the frames that drive it arrive in the order the capture
// recorded, the SFU's subscriber offer is answered, and a ping is answered
// with a pong.
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
	got := make(chan string, 4)
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
}

// TestPublisherPeerConnectionNegotiates pins the finding that shaped this
// engine: the channels the SFU creates on the subscriber PC carry nothing
// out, so the client offers a publisher PC of its own and sends on that.
// Both peer connections must reach connected and both publisher channels
// must open.
func TestPublisherPeerConnectionNegotiates(t *testing.T) {
	url, fake := newFakeConnector(t)
	sess, err := New(context.Background(), engine.Config{
		URL: url, Token: "passw0rd", Name: "publisher",
		Extra: map[string]string{"roomID": "abc123"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sess.Close() })
	if err := sess.Connect(context.Background()); err != nil {
		t.Fatalf("connect: %v (fake: %s)", err, fake.lastError())
	}
	sj := sess.(*Session)

	if !fake.sawPublisherOffer() {
		t.Fatal("fake SFU never got the client's publisher offer")
	}
	if !fake.sawAnswer() {
		t.Fatal("fake SFU never got the SDP answer")
	}
	waitFor(t, 5*time.Second, "both peer connections connected", func() bool {
		return sj.subscriberConnected() && sj.publisherConnected()
	})
	waitFor(t, 5*time.Second, "both publisher channels open", func() bool {
		return channelOpen(sj, labelReliable) && channelOpen(sj, labelLossy)
	})
	if !sess.SubscriberCanSend() || !sess.CanSend() {
		t.Fatalf("session not ready: subscriber=%v publisher=%v", sess.SubscriberCanSend(), sess.CanSend())
	}
	if err := fake.lastError(); err != "" {
		t.Fatalf("fake SFU error: %s", err)
	}
}

// TestRTCConfigReachesPionNormalised covers the TURN path: what rtc:config
// advertises is what the peer connections are built with, after the shared
// normaliser has been over it.
func TestRTCConfigReachesPionNormalised(t *testing.T) {
	url, fake := newFakeConnector(t)
	sess, err := New(context.Background(), engine.Config{
		URL: url, Token: "passw0rd", Name: "ice",
		Extra: map[string]string{"roomID": "abc123"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sess.Close() })
	if err := sess.Connect(context.Background()); err != nil {
		t.Fatalf("connect: %v (fake: %s)", err, fake.lastError())
	}
	servers := sess.(*Session).iceServers()
	if len(servers) != 1 || len(servers[0].URLs) != 1 || servers[0].URLs[0] != fakeSTUNURL {
		t.Fatalf("ice servers = %+v, want one %s entry", servers, fakeSTUNURL)
	}
}

func channelOpen(s *Session, label string) bool {
	dc := s.publisherChannel(label == labelReliable)
	return dc != nil && dc.ReadyState().String() == "open"
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

	// The join carries no group id: the connector names the group in its
	// answer to this very frame.
	got := frameJSON(t, sj, eventJoin, joinRequest{
		Password:        "passw0rd",
		ParticipantName: "Spike Tester",
		SupportedFeatures: supportedFeatures{
			AttachedRooms: true, SessionGroups: true, Transcription: true, Interpretation: true,
		},
	})
	want := `{"roomId":"abc123","payload":{"password":"passw0rd","participantName":"Spike Tester",` +
		`"supportedFeatures":{"attachedRooms":true,"sessionGroups":true,"transcription":true,` +
		`"interpretation":true},"isSilent":false},"event":"join","requestId":"REQ"}`
	if got != want {
		t.Fatalf("join frame\n got %s\nwant %s", got, want)
	}

	// Everything after the join echoes the group id.
	storeString(&sj.group, "8a7046f5-27b8-427a-abda-5ee6ceb2c205")

	got = frameJSON(t, sj, eventMediaIn, mediaIn{
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
	got = frameJSON(t, sj, eventMediaIn, mediaIn{
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

	got = frameJSON(t, sj, eventMediaIn, mediaIn{
		Method:  methodPing,
		PingReq: &pingRequest{Timestamp: 1790035953804, RTT: 0},
	})
	want = `{"roomId":"abc123","payload":{"method":"rtc:ping","ping_req":{"timestamp":1790035953804,` +
		`"rtt":0}},"event":"media-in","groupId":"8a7046f5-27b8-427a-abda-5ee6ceb2c205","requestId":"REQ"}`
	if got != want {
		t.Fatalf("ping frame\n got %s\nwant %s", got, want)
	}
}

// frameJSON marshals one outgoing frame with its random request id replaced,
// so a test can compare the rest byte for byte.
func frameJSON(t *testing.T, s *Session, event string, payload any) string {
	t.Helper()
	frame, err := s.envelope(event, payload)
	if err != nil {
		t.Fatal(err)
	}
	frame.RequestID = "REQ"
	raw, err := json.Marshal(frame)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
