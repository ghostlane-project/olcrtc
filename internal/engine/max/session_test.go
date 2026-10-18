package max

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"

	"github.com/openlibrecommunity/olcrtc/internal/engine"
)

// ai-generated: the whole file (the engine's relay negotiation against a
// fake of the SFU's signaling socket).

// wsConn is one side of the fake SFU's socket. gorilla allows one writer at
// a time, so every write goes through the mutex.
type wsConn struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

// write sends one frame. A write after the engine hung up is no failure of
// the test, which judges what the engine sent.
func (c *wsConn) write(frame map[string]any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.conn.WriteJSON(frame)
}

// connectionFrame is the SFU's first frame on a new socket: the peerId it
// assigns and the conversation's topology.
func connectionFrame(peerID int64, topology string) map[string]any {
	return map[string]any{
		"type": "notification", "notification": "connection",
		"peerId":       map[string]any{"id": peerID},
		"conversation": map[string]any{"id": "165c1eae-0000-4000-8000-000000000000", "topology": topology},
	}
}

// relayedFrame is what the SFU delivers for a participant's transmit-data:
// the sender's uid and peer ride the frame beside its data.
func relayedFrame(from, fromPeer int64, data map[string]any) map[string]any {
	return map[string]any{
		"type": "notification", "notification": "transmitted-data",
		"participantId": from, "participantType": "USER",
		"peerId": map[string]any{"id": fromPeer}, "data": data,
	}
}

// carrierOffer is an offer of the carrier's own shape (recvonly video x2,
// recvonly audio, the "datagram" channel) from a plain pion peer.
func carrierOffer(t *testing.T) string {
	t.Helper()
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	for _, kind := range []webrtc.RTPCodecType{webrtc.RTPCodecTypeVideo, webrtc.RTPCodecTypeVideo, webrtc.RTPCodecTypeAudio} {
		if _, err := pc.AddTransceiverFromKind(kind,
			webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pc.CreateDataChannel("datagram", nil); err != nil {
		t.Fatal(err)
	}
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	return stripCandidates(offer.SDP)
}

func wsURL(srv *httptest.Server) string {
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws2"
}

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

// TestAnOfferRelayedBeforeAcceptCallIsAnsweredToItsSender is the order the
// live SFU delivers a joiner's first frames in (measured 2026-10-18): the
// offer of the participant already in the room lands before the response to
// the joiner's own accept-call, the one frame that names that participant.
// The answer must still reach the offerer: the relayed frame names its
// sender. An answer addressed from the accept-call list went nowhere, and the
// pair never met.
func TestAnOfferRelayedBeforeAcceptCallIsAnsweredToItsSender(t *testing.T) {
	const offererUID, offererPeer, selfUID = int64(1001), int64(4242), "2002"
	offer := carrierOffer(t)
	sent := make(chan map[string]any, 64)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		side := &wsConn{conn: conn}
		defer func() { _ = conn.Close() }()
		side.write(connectionFrame(7, "DIRECT"))
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var frame map[string]any
			if json.Unmarshal(data, &frame) != nil {
				continue
			}
			command, _ := frame["command"].(string)
			switch command {
			case "accept-call":
				side.write(relayedFrame(offererUID, offererPeer, map[string]any{
					"sdp": map[string]any{"type": "offer", "sdp": offer}, "animojiVersion": 1,
				}))
				side.write(map[string]any{
					"type": "response", "response": "accept-call", "sequence": frame["sequence"],
					"participantIds": []int64{offererUID},
				})
			case "transmit-data":
				side.write(map[string]any{"type": "response", "response": command, "sequence": frame["sequence"]})
				sent <- frame
			}
		}
	}))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sess, err := New(ctx, engine.Config{URL: wsURL(srv), Extra: map[string]string{"userId": selfUID}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sess.Close() })
	if err := sess.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	for {
		select {
		case frame := <-sent:
			data, _ := frame["data"].(map[string]any)
			sdp, _ := data["sdp"].(map[string]any)
			if sdp["type"] != "answer" {
				continue
			}
			if to, _ := frame["participantId"].(float64); int64(to) != offererUID {
				t.Fatalf("the answer went to participant %v, want the offer's sender %d", frame["participantId"], offererUID)
			}
			if s, _ := sdp["sdp"].(string); !strings.Contains(s, "a=setup:") || strings.Contains(s, "a=candidate:") {
				t.Fatalf("the relayed answer is not a candidate-free SDP answer:\n%s", s)
			}
			return
		case <-ctx.Done():
			t.Fatal("no answer was relayed to the offer's sender")
		}
	}
}
