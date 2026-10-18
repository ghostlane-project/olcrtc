// Command max-live is a one-shot test: connects to the MAX SFU, sends the
// accept-call handshake, and waits for the binary SDP offer to arrive.
package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/websocket"
	msgpack "github.com/vmihailenco/msgpack/v5"
	"github.com/pion/webrtc/v4"
)

func main() {
	wsURL := mustRead("/tmp/max-test-ws.txt")
	turnURL := mustRead("/tmp/max-test-turn-url.txt")
	turnUser := mustRead("/tmp/max-test-turn-user.txt")
	turnPass := mustRead("/tmp/max-test-turn-pass.txt")

	// 1. Connect to the SFU
	header := http.Header{}
	header.Set("Origin", "https://web.max.ru")
	header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		fatal("dial: %v", err)
	}
	defer conn.Close()
	fmt.Println("OK: signaling connected")

	// 2. Read initial notifications to get peerId
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var peerID int
	for i := 0; i < 5; i++ {
		_, data, err := conn.ReadMessage()
		if err != nil {
			break
		}
		text := string(data)
		if text == "ping" {
			_ = conn.WriteMessage(websocket.TextMessage, []byte("pong"))
			continue
		}
		fmt.Printf("INIT_RAW[%d] (%d bytes): %.200s\n", i, len(data), text)
		var m map[string]any
		if json.Unmarshal(data, &m) == nil {
			if pid, ok := m["peerId"].(map[string]any); ok {
				if id, ok := pid["id"].(float64); ok {
					peerID = int(id)
					fmt.Printf("INIT: peerId=%d\n", peerID)
				}
			}
		}
	}
	if peerID == 0 {
		fatal("no peerId received from SFU")
	}

	// 3. Create PeerConnection with TURN
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{{
			URLs:       []string{turnURL},
			Username:   turnUser,
			Credential: turnPass,
		}},
	})
	if err != nil {
		fatal("pc: %v", err)
	}
	defer pc.Close()

	// Add a VP8 track (the tunnel's carrier)
	track, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8}, "video", "video")
	if err != nil {
		fatal("track: %v", err)
	}
	if _, err := pc.AddTrack(track); err != nil {
		fatal("addTrack: %v", err)
	}
	pc.OnTrack(func(t *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		fmt.Printf("TRACK: %s %s\n", t.Codec().MimeType, t.ID())
	})
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		fmt.Printf("PC: %s\n", s)
	})

	// 4. Send accept-call + enable video
	seq := 0
	sendJSON := func(v any) {
		seq++
		conn.WriteJSON(v)
	}
	sendJSON(map[string]any{
		"command":  "accept-call",
		"sequence": 1,
	})
	sendJSON(map[string]any{
		"command":  "change-media-settings",
		"sequence": 2,
		"mediaSettings": map[string]bool{
			"isAudioEnabled": false, "isVideoEnabled": true,
			"isScreenSharingEnabled": false, "isFastScreenSharingEnabled": false,
			"isAudioSharingEnabled": false, "isAnimojiEnabled": false,
		},
	})
	fmt.Println("OK: accept-call + video sent")

	// 5. Now CREATE an offer ourselves (we're the producer) and send as binary TRANSMIT_DATA
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		fatal("offer: %v", err)
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		fatal("local: %v", err)
	}
	fmt.Printf("OK: offer created (%d bytes SDP)\n", len(offer.SDP))

	// Try multiple SDP message formats to find what the SFU accepts
	formats := []map[string]any{
		// SDK _serializeJson format
		{
			"command":       "transmit-data",
			"sequence":      3,
			"participantId": peerID,
			"data": map[string]any{
				"sdp":  offer.SDP,
				"type": "offer",
			},
		},
		// Without wrapper
		{
			"type": "offer",
			"sdp":  offer.SDP,
		},
		// Direct SDP string
		{
			"command": "sdp",
			"sdp":     offer.SDP,
			"type":    "offer",
		},
	}
	
	for i, msg := range formats {
		if err := conn.WriteJSON(msg); err != nil {
			fmt.Printf("WARN: format[%d] send: %v\n", i, err)
		} else {
			fmt.Printf("OK: format[%d] sent\n", i)
		}
		time.Sleep(2 * time.Second)
	}

	// 6. Read all responses
	conn.SetReadDeadline(time.Now().Add(20 * time.Second))
	for i := 0; i < 30; i++ {
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			fmt.Printf("READ end: %v\n", err)
			break
		}
		switch msgType {
		case websocket.TextMessage:
			text := string(data)
			if text == "ping" {
				_ = conn.WriteMessage(websocket.TextMessage, []byte("pong"))
				continue
			}
			var m map[string]any
			if json.Unmarshal(data, &m) == nil {
				ds := string(data)
				if len(ds) > 300 {
					ds = ds[:300] + "..."
				}
				fmt.Printf("TEXT[%d]: %s\n", i, ds)
			}
		case websocket.BinaryMessage:
			if len(data) < 10 {
				continue
			}
			opcode := binary.BigEndian.Uint16(data[4:6])
			plen := int(data[7])<<16 | int(data[8])<<8 | int(data[9])
			payload := data[10 : 10+plen]
			fmt.Printf("BIN[%d]: opcode=%d len=%d\n", i, opcode, plen)
			// Try to decode as msgpack
			var td map[string]any
			if err := msgpack.Unmarshal(payload, &td); err == nil {
				ds := fmt.Sprintf("%v", td)
				if len(ds) > 500 {
					ds = ds[:500] + "..."
				}
				fmt.Printf("  msgpack: %s\n", ds)
				// Check for SDP answer
				if params, ok := td["params"].(map[string]any); ok {
					if inner, ok := params["data"].(map[string]any); ok {
						if sdp, ok := inner["sdp"].(string); ok && sdp != "" {
							fmt.Printf("\n*** GOT SDP ANSWER (%d bytes)! ***\n", len(sdp))
							// Apply the answer
							if err := pc.SetRemoteDescription(webrtc.SessionDescription{
								Type: webrtc.SDPTypeAnswer, SDP: sdp,
							}); err != nil {
								fmt.Printf("FAIL setRemote: %v\n", err)
							} else {
								fmt.Println("OK: remote description set!")
							}
							goto done
						}
					}
				}
			}
		}
	}
done:
	// Wait for connection
	fmt.Println("\nwaiting 15s for connection...")
	time.Sleep(15 * time.Second)
	fmt.Println("done")
}

func mustRead(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: read %s: %v\n", path, err)
		os.Exit(1)
	}
	return string(b[:len(b)-1]) // strip newline
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "FATAL: "+format+"\n", args...)
	os.Exit(1)
}
