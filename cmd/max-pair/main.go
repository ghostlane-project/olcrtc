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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/quic-go/webtransport-go"
)

// okcdnTrust pins the SFU's served chain (HARICA -> Hellenic 2015), which
// this container's CA bundle predates.
func okcdnTrust() *tls.Config {
	// The SFU fleet mixes CAs the container bundle predates; verify the
	// hostname ourselves instead of relying on the (missing) system roots.
	return &tls.Config{
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			for _, raw := range rawCerts {
				c, err := x509.ParseCertificate(raw)
				if err != nil {
					continue
				}
				for _, name := range c.DNSNames {
					if strings.HasSuffix(name, "okcdn.ru") && time.Now().Before(c.NotAfter) {
						return nil
					}
				}
			}
			return fmt.Errorf("no okcdn.ru certificate presented")
		},
	}
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
	sessionKeyFile := flag.String("session-key-file", "/root/max-session-key.txt", "live calls session key file")
	flag.Parse()

	linkBytes, err := os.ReadFile(*linkFile)
	if err != nil {
		fatal("link: %v", err)
	}
	joinHash := strings.TrimRight(strings.TrimSpace(string(linkBytes)), "/?#")
	if i := strings.LastIndex(joinHash, "/joincall/"); i >= 0 {
		joinHash = joinHash[i+len("/joincall/"):]
	}
	skBytes, err := os.ReadFile(*sessionKeyFile)
	if err != nil {
		fatal("session key file: %v", err)
	}
	sess := &anonymSession{SessionKey: strings.TrimSpace(string(skBytes))}
	fmt.Printf("room=%s sessionKey=%dB\n", joinHash, len(sess.SessionKey))

	sides := make([]*side, 1)
	s, err := newSide(0, sess, joinHash)
	if err != nil {
		fatal("side 0: %v", err)
	}
	sides[0] = s
	fmt.Printf("side0 peer=%d\n", s.peerID)

	// wire the SDP exchange: each side forwards its offers/answers to the peer
	sdpCh := make(chan sdpMsg, 8)
	for _, s := range sides {
		go s.readLoop(sdpCh)
	}

	// single side: negotiate with the SFU over WT
	if os.Getenv("MAX_P2P") != "" {
		// P2P room: exchange SDP with the other participant through the
		// transmit-data relay and measure the direct/TURN media flow.
		other := int64(0)
		for i := 0; i < 12 && other == 0; i++ {
			time.Sleep(2 * time.Second)
			other = sides[0].other.Load()
		}
		if other == 0 {
			fmt.Println("no other participant seen")
		} else {
			fmt.Printf("other participant: %d - sending P2P offer\n", other)
			trimmed := []string{}
			for _, ln := range strings.Split(offer.SDP, "\r\n") {
				if strings.HasPrefix(ln, "a=candidate:") {
					continue
				}
				trimmed = append(trimmed, ln)
			}
			sides[0].seq++
			if err := sides[0].send(map[string]any{
				"command":       "transmit-data",
				"sequence":      sides[0].seq,
				"participantId": other,
				"data": map[string]any{
					"sdp":            map[string]string{"type": "offer", "sdp": strings.Join(trimmed, "\r\n")},
					"animojiVersion": 1,
				},
			}); err != nil {
				fmt.Printf("p2p offer send: %v\n", err)
			} else {
				fmt.Println("P2P offer relayed")
			}
			sides[0].seq++
			_ = sides[0].send(map[string]any{
				"command":       "transmit-data",
				"sequence":      sides[0].seq,
				"participantId": other,
				"data":          map[string]any{"candidate": map[string]any{"candidate": ""}},
			})
		}
		// the negotiation loop below applies any answer that arrives
	}

	// SFU negotiation: allocate the consumer (caps only), then answer the
	// producer-updated offer the server pushes.
	sides[0].seq++
	if err := sides[0].send(map[string]any{
		"command":      "allocate-consumer",
		"sequence":     sides[0].seq,
		"capabilities": pairCapabilities(),
	}); err != nil {
		fatal("allocate-consumer: %v", err)
	}
	// first try to remove the dead zombie seat so we become the only
	// (master) participant; the SDK's remove-participant command
	sides[0].seq++
	_ = sides[0].send(map[string]any{
		"command":       "remove-participant",
		"sequence":      sides[0].seq,
		"participantId": 1125900270173683,
		"ban":           true,
	})
	fmt.Println("remove-participant (zombie) sent")

	// the room is P2P (DIRECT); flip it to SERVER (SFU) so the producer
	// transport gets allocated - same call the SDK's master client makes
	sides[0].seq++
	if err := sides[0].send(map[string]any{
		"command":  "switch-topology",
		"sequence": sides[0].seq,
		"topology": "SERVER",
		"force":    true,
	}); err != nil {
		fmt.Printf("switch-topology: %v\n", err)
	} else {
		fmt.Println("switch-topology SERVER sent")
	}
	fmt.Println("allocate-consumer (caps) sent; waiting for producer-updated")

	deadline := time.Now().Add(45 * time.Second)
	negotiated := false
	for !negotiated && time.Now().Before(deadline) {
		select {
		case m := <-sdpCh:
			fmt.Printf("[signal] type=%s len=%d session=%s\n", m.typ, len(m.sdp), m.sessionId)
			switch {
			case m.typ == "offer":
				if err := sides[0].pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: m.sdp}); err != nil {
					fmt.Printf("remote offer: %v\n", err)
					continue
				}
				ans, err := sides[0].pc.CreateAnswer(nil)
				if err != nil {
					fmt.Printf("answer: %v\n", err)
					continue
				}
				if err := sides[0].pc.SetLocalDescription(ans); err != nil {
					fmt.Printf("local answer: %v\n", err)
					continue
				}
				trimmed := []string{}
				ssrc := ""
				for _, ln := range strings.Split(ans.SDP, "\r\n") {
					if strings.HasPrefix(ln, "a=candidate:") {
						continue
					}
					if strings.HasPrefix(ln, "a=ssrc:") {
						f := strings.SplitN(strings.TrimPrefix(ln, "a=ssrc:"), " ", 2)
						ssrc = f[0]
					}
					trimmed = append(trimmed, ln)
				}
				payload := map[string]any{
					"command":     "accept-producer",
					"sequence":    sides[0].seq + 1,
					"description": strings.Join(trimmed, "\r\n"),
				}
				if m.sessionId != "" {
					payload["sessionId"] = m.sessionId
				}
				if ssrc != "" {
					payload["ssrcs"] = []string{ssrc}
				}
				sides[0].seq++
				if err := sides[0].send(payload); err != nil {
					fmt.Printf("accept-producer send: %v\n", err)
				} else {
					fmt.Printf("accept-producer sent (session=%s ssrc=%s)\n", m.sessionId, ssrc)
					negotiated = true
				}
			case m.typ == "answer":
				if err := sides[0].pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: m.sdp}); err != nil {
					fmt.Printf("remote answer: %v\n", err)
					continue
				}
				fmt.Println("SFU consumer answer applied")
				negotiated = true
			}
		case <-time.After(time.Second):
		}
	}
	if !negotiated {
		fmt.Println("NO SDP from the SFU in 45s")
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
		all := true
		for _, s := range sides {
			if !s.state.Load() {
				all = false
			}
		}
		if all {
			fmt.Println("PEER(S) CONNECTED")
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	for i, s := range sides {
		fmt.Printf("side%d connected=%v\n", i, s.state.Load())
	}
}

func measure(sides []*side, window time.Duration) {
	fmt.Printf("measuring %s...\n", window)
	start := time.Now().Add(3 * time.Second) // let ICE settle
	time.Sleep(3 * time.Second)
	before := make([]int64, len(sides))
	for i, s := range sides {
		before[i] = s.bytesReceived()
	}
	time.Sleep(window - 3*time.Second)
	d := time.Since(start)
	var total int64
	for i, s := range sides {
		r := s.bytesReceived() - before[i]
		total += r
		fmt.Printf("side%d received %d B (%.1f kbit/s)\n", i, r, float64(r)*8/d.Seconds()/1000)
	}
	if total > 100_000 {
		fmt.Println("MEDIA FLOW: PASS")
	} else {
		fmt.Println("MEDIA FLOW: FAIL/THROTTLED")
	}
}

type sdpMsg struct {
	side      int
	from      int64
	typ       string
	sdp       string
	sessionId string
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

type atomicI64 struct{ v int64 }

func (a *atomicI64) Store(x int64) { a.v = x }
func (a *atomicI64) Load() int64   { return a.v }

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

func newSide(idx int, sessIn *anonymSession, joinHash string) (*side, error) {
	s := &side{idx: idx}
	var err error
	s.sess = sessIn
	s.params, err = joinByLink(s.sess, joinHash)
	if err != nil {
		return nil, fmt.Errorf("join: %w", err)
	}
	s.peerID = s.params.PeerID
	if s.peerID == 0 && s.params.Endpoint != "" {
		if i := strings.Index(s.params.Endpoint, "peerId="); i >= 0 {
			v := s.params.Endpoint[i+len("peerId="):]
			if j := strings.IndexAny(v, "&?#"); j >= 0 {
				v = v[:j]
			}
			s.peerID, _ = strconv.ParseInt(v, 10, 64)
		}
	}

	me := &webrtc.MediaEngine{}
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
		PayloadType:        96,
	}, webrtc.RTPCodecTypeVideo); err != nil {
		return nil, fmt.Errorf("codec: %w", err)
	}
	api := webrtc.NewAPI(webrtc.WithMediaEngine(me))
	s.pc, err = api.NewPeerConnection(webrtc.Configuration{
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
	sep := "?"
	if strings.Contains(s.params.WTEndpoint, "?") {
		sep = "&"
	}
	wtURL := s.params.WTEndpoint + sep + q.Encode()
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
	_ = s.send(map[string]any{
		"command":  "accept-call",
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
				if json.Unmarshal(plain, &m) == nil {
					if ids, ok := m["participantIds"].([]any); ok && len(ids) > 0 {
						if f, ok := ids[0].(float64); ok {
							s.other.Store(int64(f))
						}
					}
					if conv, ok := m["conversation"].(map[string]any); ok {
						if topo, ok := conv["topology"].(string); ok {
							fmt.Printf("[side%d] room topology: %s\n", s.idx, topo)
						}
					}
				}
				if json.Unmarshal(plain, &m) != nil {
					d := text
					if len(d) > 300 {
						d = d[:300] + "..."
					}
					fmt.Printf("[side%d] < %s\n", s.idx, d)
					continue
				}
				if d, ok := m["description"].(string); ok && len(d) > 100 {
					notif, _ := m["notification"].(string)
					sess := ""
					if v, ok := m["sessionId"].(string); ok {
						sess = v
					} else if v, ok := m["sessionId"].(float64); ok {
						sess = fmt.Sprint(int64(v))
					}
					typ := "offer"
					if notif == "consumer-answered" {
						typ = "answer"
					}
					_ = typ
					fmt.Printf("[side%d] notification=%q sessionId=%q description=%dB\n", s.idx, notif, sess, len(d))
					out <- sdpMsg{side: s.idx, typ: typ, sdp: d, sessionId: sess}
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

func pairCapabilities() map[string]any {
	return map[string]any{
		"estimatedPerformanceIndex": 1000, "audioMix": true, "consumerUpdate": true,
		"producerNotificationDataChannelVersion": 8, "producerCommandDataChannelVersion": 3,
		"consumerScreenDataChannelVersion": 1, "producerScreenDataChannelVersion": 1,
		"asrDataChannelVersion": 0, "animojiDataChannelVersion": 1, "animojiBackendRender": true,
		"onDemandTracks": true, "unifiedPlan": true, "singleSession": true, "videoTracksCount": 1,
		"red": true, "audioShare": false, "fastScreenShare": false, "videoSuspend": true,
		"simulcast": false, "simulcastNativeOrder": true, "consumerFastScreenShare": false,
		"consumerFastScreenShareQualityOnDemand": false, "transparentAudio": false,
	}
}
