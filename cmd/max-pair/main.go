// Command max-pair is the MAX carrier verdict test: two anonymous
// identities join one room, both signal over WebTransport, two pion peers
// exchange SDP through the SFU and the VP8 flow between them is measured.
package main

import (
	"bytes"
	"compress/flate"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/quic-go/webtransport-go"
)

// okcdnTrust pins the SFU's served chain (HARICA -> Hellenic 2015), which
// this container's CA bundle predates.
func okcdnTrust() *tls.Config {
	pool := x509.NewCertPool()
	pem, err := os.ReadFile("/root/okcdn-chain.pem")
	if err != nil {
		fmt.Println("okcdnTrust: chain file missing:", err)
	}
	if ok := pool.AppendCertsFromPEM(pem); !ok {
		fmt.Println("okcdnTrust: no certs parsed from chain")
	}
	return &tls.Config{RootCAs: pool}
}

var _ = okcdnTrust // keep the pinned trust referenced

const (
	apiBase         = "https://calls.okcdn.ru"
	applicationKey  = "CNHIJPLGDIHBABABA"
	methodAnonLogin = "auth.anonymLogin"
	methodJoin      = "vchat.joinConversationByLink"
)

func main() {
	hold := flag.Duration("hold", 45*time.Second, "media measurement window")
	linkFile := flag.String("link-file", "/tmp/max-multi-link.txt", "join link file")
	tokenFile := flag.String("calls-token-file", "/root/max-calls-token.txt", "calls auth token file")
	flag.Parse()

	linkBytes, err := os.ReadFile(*linkFile)
	if err != nil {
		fatal("link: %v", err)
	}
	joinHash := strings.TrimRight(strings.TrimSpace(string(linkBytes)), "/?#")
	if i := strings.LastIndex(joinHash, "/joincall/"); i >= 0 {
		joinHash = joinHash[i+len("/joincall/"):]
	}
	tokenBytes, err := os.ReadFile(*tokenFile)
	if err != nil {
		fatal("calls token: %v (start the host so the SDK mints one)", err)
	}
	callsToken := strings.TrimSpace(string(tokenBytes))
	fmt.Printf("room=%s callsToken=%dB\n", joinHash, len(callsToken))

	sides := make([]*side, 2)
	var wg sync.WaitGroup
	for i := range sides {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			s, err := newSide(idx, callsToken, joinHash)
			if err != nil {
				fatal("side %d: %v", idx, err)
			}
			sides[idx] = s
		}(i)
	}
	wg.Wait()
	fmt.Printf("side0 peer=%d  side1 peer=%d\n", sides[0].peerID, sides[1].peerID)

	// wire the SDP exchange: each side forwards its offers/answers to the peer
	sdpCh := make(chan sdpMsg, 8)
	for _, s := range sides {
		go s.readLoop(sdpCh)
	}

	// side 0 offers, side 1 answers
	offer, err := sides[0].pc.CreateOffer(nil)
	if err != nil {
		fatal("offer: %v", err)
	}
	if err := sides[0].pc.SetLocalDescription(offer); err != nil {
		fatal("local0: %v", err)
	}
	fmt.Printf("side0 offer created (%dB SDP)\n", len(offer.SDP))
	sides[0].sendSDP(offer.SDP, "offer")

	deadline := time.Now().Add(20 * time.Second)
	var answer webrtc.SessionDescription
	gotAnswer := false
	for !gotAnswer && time.Now().Before(deadline) {
		select {
		case m := <-sdpCh:
			fmt.Printf("[%d] signal: type=%s len=%d from=%d\n", m.side, m.typ, len(m.sdp), m.from)
			switch {
			case m.typ == "offer" && m.side == 1:
				if err := sides[1].pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: m.sdp}); err != nil {
					fmt.Printf("side1 remote: %v\n", err)
					continue
				}
				ans, err := sides[1].pc.CreateAnswer(nil)
				if err != nil {
					fmt.Printf("side1 answer: %v\n", err)
					continue
				}
				if err := sides[1].pc.SetLocalDescription(ans); err != nil {
					fmt.Printf("side1 local: %v\n", err)
					continue
				}
				sides[1].sendSDP(ans.SDP, "answer")
				fmt.Println("side1 answered")
			case m.typ == "answer" && m.side == 0:
				answer = webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: m.sdp}
				_ = sides[0].pc.SetRemoteDescription(answer)
				gotAnswer = true
			}
		case <-time.After(time.Second):
		}
	}
	if !gotAnswer {
		fmt.Println("NO ANSWER relayed through SFU in 20s")
	}

	// wait for ICE + measure
	waitConnected(sides, 30*time.Second)
	measure(sides, *hold)
	for _, s := range sides {
		s.close()
	}
}

func waitConnected(sides []*side, window time.Duration) {
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		if sides[0].state.Load() && sides[1].state.Load() {
			fmt.Println("BOTH PEERS CONNECTED")
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Printf("peers not fully connected: side0=%v side1=%v\n", sides[0].state.Load(), sides[1].state.Load())
}

func measure(sides []*side, window time.Duration) {
	fmt.Printf("measuring %s...\n", window)
	start := time.Now().Add(3 * time.Second) // let ICE settle
	time.Sleep(3 * time.Second)
	b0 := sides[0].bytesReceived()
	b1 := sides[1].bytesReceived()
	time.Sleep(window - 3*time.Second)
	d := time.Since(start)
	r0 := sides[0].bytesReceived() - b0
	r1 := sides[1].bytesReceived() - b1
	fmt.Printf("received: side0 %d B (%.1f kbit/s), side1 %d B (%.1f kbit/s) over %s\n",
		r0, float64(r0)*8/d.Seconds()/1000, r1, float64(r1)*8/d.Seconds()/1000, d.Truncate(time.Second))
	if r0+r1 > 100_000 {
		fmt.Println("MEDIA FLOW: PASS")
	} else {
		fmt.Println("MEDIA FLOW: FAIL/THROTTLED")
	}
}

type sdpMsg struct {
	side int
	from int64
	typ  string
	sdp  string
}

type side struct {
	idx     int
	peerID  int64
	sess    *anonymSession
	params  *convParams
	pc      *webrtc.PeerConnection
	wt      *webtransport.Session
	stream  *webtransport.Stream
	writeMu sync.Mutex
	seq     int
	state   atomicBool
	recvN   atomicInt64
}

type atomicBool struct{ v int32 }

func (b *atomicBool) Store(v bool) {
	if v {
		b.v = 1
	} else {
		b.v = 0
	}
}
func (b *atomicBool) Load() bool { return b.v == 1 }

type atomicInt64 struct{ v int64 }

func (i *atomicInt64) Add(n int64) { i.v += n }
func (i *atomicInt64) load() int64 { return i.v }

func newSide(idx int, callsToken, joinHash string) (*side, error) {
	s := &side{idx: idx}
	var err error
	s.sess, err = anonymLogin(callsToken, fmt.Sprintf("olcrtc-pair-%d", idx))
	if err != nil {
		return nil, fmt.Errorf("anonymLogin: %w", err)
	}
	s.params, err = joinByLink(s.sess, joinHash)
	if err != nil {
		return nil, fmt.Errorf("join: %w", err)
	}
	s.peerID = s.params.PeerID

	s.pc, err = webrtc.NewPeerConnection(webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{{
			URLs:       s.params.TURN.URLs,
			Username:   s.params.TURN.Username,
			Credential: s.params.TURN.Credential,
		}},
	})
	if err != nil {
		return nil, fmt.Errorf("pc: %w", err)
	}
	track, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8}, "video", "video")
	if err != nil {
		return nil, fmt.Errorf("track: %w", err)
	}
	if _, err := s.pc.AddTrack(track); err != nil {
		return nil, fmt.Errorf("addTrack: %w", err)
	}
	s.pc.OnConnectionStateChange(func(st webrtc.PeerConnectionState) {
		fmt.Printf("[side%d] PC %s\n", idx, st)
		if st == webrtc.PeerConnectionStateConnected {
			s.state.Store(true)
		}
	})
	s.pc.OnTrack(func(t *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		buf := make([]byte, 1500)
		for {
			n, _, err := t.Read(buf)
			if err != nil {
				return
			}
			s.recvN.Add(int64(n))
		}
	})

	// WebTransport signaling
	ua := "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15"
	q := url.Values{}
	q.Set("platform", "web")
	q.Set("appVersion", "26.10.3")
	q.Set("version", "3")
	q.Set("device", "web")
	q.Set("capabilities", "webrtc")
	q.Set("clientType", "SDK_JS")
	q.Set("compression", "deflate-raw")
	q.Set("ua", ua)
	q.Set("peerId", fmt.Sprint(s.peerID))
	wtURL := s.params.WTEndpoint + "?" + q.Encode()
	fmt.Printf("[side%d] WT %s\n", idx, wtURL)
	d := &webtransport.Transport{TLSClientConfig: okcdnTrust()}
	_, sess, err := d.Dial(context.Background(), wtURL, nil)
	if err != nil {
		return nil, fmt.Errorf("wt dial: %w", err)
	}
	s.wt = sess
	s.stream, err = sess.OpenStreamSync(context.Background())
	if err != nil {
		return nil, fmt.Errorf("wt stream: %w", err)
	}
	s.seq++
	_ = s.send(map[string]any{"command": "accept-call", "sequence": s.seq})
	s.seq++
	_ = s.send(map[string]any{
		"command":  "change-media-settings",
		"sequence": s.seq,
		"mediaSettings": map[string]bool{
			"isAudioEnabled": false, "isVideoEnabled": true,
			"isScreenSharingEnabled": false, "isFastScreenSharingEnabled": false,
			"isAudioSharingEnabled": false, "isAnimojiEnabled": false,
		},
	})
	return s, nil
}

func (s *side) send(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return s.sendRaw(b)
}

func (s *side) sendSDP(sdp, typ string) {
	s.seq++
	payload, _ := json.Marshal(map[string]any{
		"command":  "transmit-data",
		"sequence": s.seq,
		"data":     map[string]string{"sdp": sdp, "type": typ},
	})
	if err := s.sendRaw(payload); err != nil {
		fmt.Printf("[side%d] sendSDP: %v\n", s.idx, err)
	}
}

func (s *side) sendRaw(b []byte) error {
	deflated, err := deflateRaw(b)
	if err != nil {
		return err
	}
	n := len(deflated)
	var head []byte
	switch {
	case n < 1<<6:
		head = []byte{byte(n)}
	case n < 1<<14:
		head = []byte{byte(n>>8) | 0x40, byte(n)}
	case n < 1<<30:
		head = []byte{byte(n>>24) | 0x80, byte(n >> 16), byte(n >> 8), byte(n)}
	default:
		head = append([]byte{0xc0}, make([]byte, 7)...)
		binary.BigEndian.PutUint64(head[1:], uint64(n))
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err = s.stream.Write(append(head, deflated...))
	return err
}

func (s *side) readLoop(out chan<- sdpMsg) {
	buf := make([]byte, 0, 16384)
	tmp := make([]byte, 8192)
	for {
		n, err := s.stream.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
			for {
				payload, rest, ok := takeFrame(buf)
				if !ok {
					break
				}
				buf = rest
				plain, err := inflateRaw(payload)
				if err != nil {
					continue
				}
				text := strings.TrimSpace(string(plain))
				if text == "ping" {
					_ = s.sendRaw([]byte("pong"))
					continue
				}
				var m map[string]any
				if json.Unmarshal(plain, &m) != nil {
					d := text
					if len(d) > 300 {
						d = d[:300] + "..."
					}
					fmt.Printf("[side%d] < %s\n", s.idx, d)
					continue
				}
				if cmd, _ := m["command"].(string); cmd == "transmit-data" || m["data"] != nil {
					if data, ok := m["data"].(map[string]any); ok {
						if sd, _ := data["sdp"].(string); sd != "" {
							typ, _ := data["type"].(string)
							var from int64
							if p, ok := m["participantId"].(float64); ok {
								from = int64(p)
							}
							out <- sdpMsg{side: s.idx, from: from, typ: typ, sdp: sd}
						}
					}
					continue
				}
				d := text
				if len(d) > 300 {
					d = d[:300] + "..."
				}
				fmt.Printf("[side%d] < %s\n", s.idx, d)
			}
		}
		if err != nil {
			fmt.Printf("[side%d] read end: %v\n", s.idx, err)
			return
		}
	}
}

func (s *side) bytesReceived() int64 { return s.recvN.load() }
func (s *side) close() {
	if s.pc != nil {
		_ = s.pc.Close()
	}
	if s.wt != nil {
		_ = s.wt.CloseWithError(0, "")
	}
}

// ---- calls API ----

type anonymSession struct {
	UID        string `json:"uid"`
	SessionKey string `json:"session_key"`
}

type convParams struct {
	Endpoint   string `json:"endpoint"`
	WTEndpoint string `json:"wt_endpoint"`
	Token      string `json:"token"`
	PeerID     int64  `json:"peerId"`
	TURN       struct {
		URLs       []string `json:"urls"`
		Username   string   `json:"username"`
		Credential string   `json:"credential"`
	} `json:"turn_server"`
}

func fbDo(method string, params url.Values) ([]byte, error) {
	params.Set("method", method)
	params.Set("format", "JSON")
	params.Set("application_key", applicationKey)
	req, err := http.NewRequest(http.MethodPost, apiBase+"/fb.do", strings.NewReader(params.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15")
	req.Header.Set("Origin", "https://web.max.ru")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func anonymLogin(callsToken, device string) (*anonymSession, error) {
	sessionData, _ := json.Marshal(map[string]any{
		"version":        3,
		"device_id":      device,
		"client_version": "26.10.3",
		"client_type":    "SDK_JS",
		"auth_token":     callsToken,
	})
	body, err := fbDo(methodAnonLogin, url.Values{"session_data": {string(sessionData)}})
	if err != nil {
		return nil, err
	}
	var s anonymSession
	_ = json.Unmarshal(body, &s)
	if s.SessionKey == "" {
		return nil, fmt.Errorf("no session key: %.200s", body)
	}
	return &s, nil
}

func joinByLink(s *anonymSession, joinHash string) (*convParams, error) {
	body, err := fbDo(methodJoin, url.Values{
		"session_key": {s.SessionKey},
		"joinLink":    {joinHash},
		"isVideo":     {"true"},
		"isAudio":     {"true"},
	})
	if err != nil {
		return nil, err
	}
	var p convParams
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, fmt.Errorf("parse: %w (%.100s)", err, body)
	}
	if p.Endpoint == "" {
		return nil, fmt.Errorf("no endpoint: %.200s", body)
	}
	return &p, nil
}

// ---- WT framing ----

func takeFrame(buf []byte) ([]byte, []byte, bool) {
	if len(buf) == 0 {
		return nil, nil, false
	}
	marker := buf[0] & 0xc0
	var size, hdr int
	switch marker {
	case 0x00:
		size, hdr = int(buf[0]&0x3f), 1
	case 0x40:
		if len(buf) < 2 {
			return nil, nil, false
		}
		size, hdr = int(buf[0]&0x3f)<<8|int(buf[1]), 2
	case 0x80:
		if len(buf) < 4 {
			return nil, nil, false
		}
		size, hdr = int(buf[0]&0x3f)<<24|int(buf[1])<<16|int(buf[2])<<8|int(buf[3]), 4
	case 0xc0:
		if len(buf) < 8 {
			return nil, nil, false
		}
		v := binary.BigEndian.Uint64(buf[1:9]) & ((1 << 62) - 1)
		size, hdr = int(v), 8
	}
	if len(buf) < hdr+size {
		return nil, nil, false
	}
	return buf[hdr : hdr+size], buf[hdr+size:], true
}

func deflateRaw(b []byte) ([]byte, error) {
	var out bytes.Buffer
	w, _ := flate.NewWriter(&out, flate.DefaultCompression)
	if _, err := w.Write(b); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func inflateRaw(b []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(b))
	defer r.Close()
	return io.ReadAll(r)
}

func fatal(f string, args ...any) {
	fmt.Fprintf(os.Stderr, "FATAL: "+f+"\n", args...)
	os.Exit(1)
}
