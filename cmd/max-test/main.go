package main

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/openlibrecommunity/olcrtc/internal/protect"
)

func main() {
	wsURL := os.Args[1]
	turnURL := os.Args[2]
	turnUser := os.Args[3]
	turnPass := os.Args[4]

	// 1. Connect to signaling
	dialer := protect.NewWebSocketDialer(15*time.Second, nil)
	header := http.Header{}
	header.Set("Origin", "https://web.max.ru")
	header.Set("User-Agent", "Mozilla/5.0")
	conn, _, err := dialer.Dial(wsURL, header)
	if err != nil {
		fmt.Printf("FAIL ws: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	fmt.Println("OK: signaling connected")

	// Read initial notification
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err == nil {
		fmt.Printf("RECV: %.200s\n", msg)
	}

	// 2. Create PeerConnection with TURN
	cfg := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{{
			URLs:       []string{turnURL},
			Username:   turnUser,
			Credential: turnPass,
		}},
	}
	pc, err := webrtc.NewPeerConnection(cfg)
	if err != nil {
		fmt.Printf("FAIL pc: %v\n", err)
		os.Exit(1)
	}
	defer pc.Close()

	// Add a VP8 track
	track, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8}, "video", "video")
	if err != nil {
		fmt.Printf("FAIL track: %v\n", err)
		os.Exit(1)
	}
	if _, err := pc.AddTrack(track); err != nil {
		fmt.Printf("FAIL addTrack: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("OK: peer connection + VP8 track")

	// 3. Create offer and send to signaling
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		fmt.Printf("FAIL offer: %v\n", err)
		os.Exit(1)
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		fmt.Printf("FAIL local: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("OK: offer created (%d bytes SDP)\n", len(offer.SDP))

	// Send offer as JSON
	offerMsg := fmt.Sprintf(`{"type":"offer","data":{"type":"offer","sdp":%q}}`, offer.SDP)
	if err := conn.WriteMessage(1, []byte(offerMsg)); err != nil {
		fmt.Printf("FAIL send offer: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("OK: offer sent to SFU")

	// 4. Wait for answer
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		fmt.Printf("PC: %s\n", s)
	})

	conn.SetReadDeadline(time.Now().Add(20 * time.Second))
	for i := 0; i < 10; i++ {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			fmt.Printf("READ timeout/err: %v\n", err)
			break
		}
		text := string(msg)
		if text == "ping" {
			_ = conn.WriteMessage(1, []byte("ping"))
			continue
		}
		if len(text) > 500 {
			fmt.Printf("RECV[%d]: %.300s... (%d bytes)\n", i, msg, len(msg))
		} else {
			fmt.Printf("RECV[%d]: %.300s\n", i, msg)
		}
		// Check for answer
		if contains(text, "answer") && contains(text, "sdp") {
			fmt.Println("*** GOT ANSWER! ***")
			break
		}
	}

	fmt.Println("done")
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
