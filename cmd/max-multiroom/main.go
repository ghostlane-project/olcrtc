// Command max-multiroom answers the deployment question: how many
// simultaneous call rooms can one MAX account hold? It performs N
// independent joins of one room link (each with its own anonymous calls
// session), dials each SFU WebSocket, sends the accept-call handshake and
// reports which survive the hold window.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/ice/v4"
	"github.com/pion/webrtc/v4"
)

const (
	apiBase          = "https://calls.okcdn.ru"
	applicationKey   = "CNHIJPLGDIHBABABA"
	methodAnonLogin  = "auth.anonymLogin"
	methodJoinByLink = "vchat.joinConversationByLink"
)

type joinResult struct {
	Idx              int
	OtherParticipant int64
	UID              string
	PeerID           int64
	Endpoint         string
	WSError          string
	AliveAt          time.Duration
	Frames           int
	Err              string
}

func main() {
	n := flag.Int("n", 5, "parallel joins")
	hold := flag.Duration("hold", 60*time.Second, "how long to hold the connections")
	linkFile := flag.String("link-file", "/tmp/max-multi-link.txt", "file with the join link")
	mode := flag.String("mode", "join", "join: by link + web token; params: session key + conversation id")
	sessionKey := flag.String("session-key", "", "calls session key (params mode)")
	stagger := flag.Duration("stagger", 0, "delay between joins (0 = all at once)")
	conversationID := flag.String("conversation-id", "", "conversation uuid (params mode)")
	flag.Parse()

	var joinHash, webToken string
	if *mode == "joinsk" {
		if *sessionKey == "" {
			fatal("joinsk mode needs -session-key (a live calls session key)")
		}
		linkBytes, err := os.ReadFile(*linkFile)
		if err != nil {
			fatal("link file: %v", err)
		}
		joinHash = strings.TrimSpace(string(linkBytes))
		if i := strings.LastIndex(joinHash, "/joincall/"); i >= 0 {
			joinHash = joinHash[i+len("/joincall/"):]
		}
		joinHash = strings.TrimRight(joinHash, "/?# \n")
	} else if *mode == "params" {
		if *sessionKey == "" || *conversationID == "" {
			fatal("params mode needs -session-key and -conversation-id")
		}
	} else {
		linkBytes, err := os.ReadFile(*linkFile)
		if err != nil {
			fatal("link file: %v", err)
		}
		joinHash = strings.TrimSpace(string(linkBytes))
		if i := strings.LastIndex(joinHash, "/joincall/"); i >= 0 {
			joinHash = joinHash[i+len("/joincall/"):]
		}
		joinHash = strings.TrimRight(joinHash, "/?# \n")
		authRaw, err := os.ReadFile("/root/max-auth.txt")
		if err != nil {
			fatal("auth file: %v", err)
		}
		var auth struct {
			Token struct {
				Value string `json:"value"`
			} `json:"token"`
		}
		if err := json.Unmarshal(authRaw, &auth); err != nil {
			fatal("auth parse: %v", err)
		}
		webToken = auth.Token.Value
	}
	fmt.Printf("mode=%s room=%s joins=%d hold=%s\n", *mode, *conversationID, *n, *hold)

	results := make([]*joinResult, *n)
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < *n; i++ {
		wg.Add(1)
		idx := i
		if *stagger > 0 && idx > 0 {
			time.Sleep(*stagger)
		}
		go func(idx int) {
			defer wg.Done()
			r := &joinResult{Idx: idx}
			results[idx] = r
			var params *convParams
			if *mode == "params" {
				p, err := getConversationParams(*sessionKey, *conversationID)
				if err != nil {
					r.Err = "getConversationParams: " + err.Error()
					return
				}
				params = p
			} else if *mode == "joinsk" {
				p, err := joinByLink(&anonymSession{SessionKey: *sessionKey}, joinHash)
				if err != nil {
					r.Err = "joinByLink: " + err.Error()
					return
				}
				params = p
			} else {
				sess, err := anonymLogin(webToken, fmt.Sprintf("olcrtc-mr-%d", idx))
				if err != nil {
					r.Err = "anonymLogin: " + err.Error()
					return
				}
				r.UID = sess.UID
				p, err := joinByLink(sess, joinHash)
				if err != nil {
					r.Err = "joinByLink: " + err.Error()
					return
				}
				params = p
			}
			r.Endpoint = params.Endpoint
			holdOne(r, params, *conversationID, *hold, start)
		}(i)
	}
	wg.Wait()

	fmt.Println("\n=== RESULTS ===")
	alive := 0
	for _, r := range results {
		status := "DEAD"
		if r.WSError == "" && r.Err == "" && r.AliveAt > 0 {
			status = "ALIVE"
			alive++
		}
		fmt.Printf("[%d] %-5s uid=%s peer=%d frames=%d aliveAt=%s err=%q ws=%q\n",
			r.Idx, status, r.UID, r.PeerID, r.Frames, r.AliveAt, r.Err, r.WSError)
	}
	fmt.Printf("\n%d/%d sessions alive after %s\n", alive, *n, *hold)
	if alive == *n {
		fmt.Println("MULTI-ROOM: PASS")
	} else {
		fmt.Println("MULTI-ROOM: FAIL")
		os.Exit(1)
	}
}

// holdOne dials the SFU, handshakes and keeps the session up for the hold window.
func holdOne(r *joinResult, params *convParams, conversationID string, hold time.Duration, start time.Time) {
	header := http.Header{}
	header.Set("Origin", "https://web.max.ru")
	header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15")
	// The join answer already carries the full query (conversationId, peerId,
	// token, userId); only the params-mode endpoint is bare and needs them.
	wsURL := params.Endpoint
	if uid := params.TURN.Username; uid != "" && !strings.Contains(wsURL, "?") {
		if i := strings.Index(uid, ":"); i > 0 {
			wsURL += "?userId=" + uid[i+1:] + "&entityType=USER&conversationId=" + conversationID + "&token=" + url.QueryEscape(params.Token)
		}
	}
	if !strings.Contains(wsURL, "clientType") {
		sep := "&"
		if !strings.Contains(wsURL, "?") {
			sep = "?"
		}
		wsURL += sep + "platform=web&appVersion=26.10.3&version=3&device=web&capabilities=webrtc&clientType=SDK_JS"
	}
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		r.WSError = err.Error()
		return
	}
	defer conn.Close()

	// initial notifications carry our peerId
	deadline := time.Now().Add(8 * time.Second)
	_ = conn.SetReadDeadline(deadline)
	seq := 0
	for time.Now().Before(deadline) {
		_, data, err := conn.ReadMessage()
		if err != nil {
			r.WSError = fmt.Sprintf("init read: %v", err)
			return
		}
		r.Frames++
		if string(data) == "ping" {
			_ = conn.WriteMessage(websocket.TextMessage, []byte("pong"))
			continue
		}
		if r.Idx == 0 || os.Getenv("MAX_DEBUG") != "" {
			d := string(data)
			if len(d) > 500 {
				d = d[:500] + "..."
			}
			fmt.Printf("[%d] INIT FRAME: %s\n", r.Idx, d)
		}
		var m map[string]any
		if json.Unmarshal(data, &m) == nil {
			if pid, ok := m["peerId"].(map[string]any); ok {
				if id, ok := pid["id"].(float64); ok {
					r.PeerID = int64(id)
				}
			}
			if t, _ := m["type"].(string); t == "notification" {
				break // connection assignment received
			}
		}

	}
	seq++
	_ = conn.WriteJSON(map[string]any{"command": "accept-call", "sequence": seq})
	seq++
	_ = conn.WriteJSON(map[string]any{
		"command":  "change-media-settings",
		"sequence": seq,
		"mediaSettings": map[string]bool{
			"isAudioEnabled": false, "isVideoEnabled": true,
			"isScreenSharingEnabled": false, "isFastScreenSharingEnabled": false,
			"isAudioSharingEnabled": false, "isAnimojiEnabled": false,
		},
	})
	if os.Getenv("MAX_ALLOC") != "" {
		seq++
		_ = conn.WriteJSON(map[string]any{
			"command":  "allocate-consumer",
			"sequence": seq,
			"capabilities": map[string]any{
				"estimatedPerformanceIndex":              1000,
				"audioMix":                               true,
				"consumerUpdate":                         true,
				"producerNotificationDataChannelVersion": 8,
				"producerCommandDataChannelVersion":      3,
				"consumerScreenDataChannelVersion":       1,
				"producerScreenDataChannelVersion":       1,
				"asrDataChannelVersion":                  0,
				"animojiDataChannelVersion":              1,
				"animojiBackendRender":                   true,
				"onDemandTracks":                         true,
				"unifiedPlan":                            true,
				"singleSession":                          true,
				"videoTracksCount":                       1,
				"red":                                    true,
				"audioShare":                             false,
				"fastScreenShare":                        false,
				"videoSuspend":                           true,
				"simulcast":                              false,
				"simulcastNativeOrder":                   true,
				"consumerFastScreenShare":                false,
				"consumerFastScreenShareQualityOnDemand": false,
				"transparentAudio":                       false,
			},
		})
		fmt.Println("[alloc] allocate-consumer sent (full capabilities)")
	}

	// keepalive loop for the rest of the hold window
	_ = conn.SetReadDeadline(time.Now().Add(hold))
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			r.AliveAt = time.Since(start)
			if strings.Contains(err.Error(), "timeout") && time.Since(start) >= hold {
				return // held the full window
			}
			r.WSError = fmt.Sprintf("hold: %v", err)
			return
		}
		r.Frames++
		if string(data) == "ping" {
			_ = conn.WriteMessage(websocket.TextMessage, []byte("pong"))
			continue
		}
		var hm map[string]any
		if json.Unmarshal(data, &hm) == nil {
			if ids, ok := hm["participantIds"].([]any); ok && len(ids) > 0 && r.OtherParticipant == 0 {
				r.OtherParticipant = int64(ids[0].(float64))
				fmt.Printf("[%d] other participant: %d\n", r.Idx, r.OtherParticipant)
				if os.Getenv("MAX_MEDIA") != "" && mediaOnce.TryClaim() {
					runMedia(r, conn, params, &seq)
				}
			}
		}
		if os.Getenv("MAX_DEBUG") != "" {
			d := string(data)
			if len(d) > 350 {
				d = d[:350] + "..."
			}
			fmt.Printf("[%d] HOLD FRAME: %s\n", r.Idx, d)
		}
	}
}

type anonymSession struct {
	UID        string `json:"uid"`
	SessionKey string `json:"session_key"`
}

type convParams struct {
	Endpoint string `json:"endpoint"`
	Token    string `json:"token"`
	TURN     struct {
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
	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 4096)
	for {
		n, rerr := resp.Body.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if rerr != nil {
			break
		}
	}
	return buf, nil
}

func anonymLogin(webToken, device string) (*anonymSession, error) {
	sessionData, _ := json.Marshal(map[string]any{
		"version":        3,
		"device_id":      device,
		"client_version": "26.10.3",
		"client_type":    "SDK_JS",
		"auth_token":     webToken,
	})
	body, err := fbDo(methodAnonLogin, url.Values{"session_data": {string(sessionData)}})
	if err != nil {
		return nil, err
	}
	var s anonymSession
	if err := json.Unmarshal(body, &s); err != nil {
		return nil, fmt.Errorf("parse: %w (%.100s)", err, body)
	}
	if s.SessionKey == "" {
		return nil, fmt.Errorf("no session key (%.200s)", body)
	}
	return &s, nil
}

func getConversationParams(sessionKey, conversationID string) (*convParams, error) {
	body, err := fbDo("vchat.getConversationParams", url.Values{
		"session_key":    {sessionKey},
		"conversationId": {conversationID},
	})
	if err != nil {
		return nil, err
	}
	var p convParams
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, fmt.Errorf("parse: %w (%.100s)", err, body)
	}
	if p.Endpoint == "" {
		return nil, fmt.Errorf("no endpoint (%.200s)", body)
	}
	return &p, nil
}

func joinByLink(s *anonymSession, joinHash string) (*convParams, error) {
	vals := url.Values{
		"session_key": {s.SessionKey},
		"joinLink":    {joinHash},
		"isVideo":     {"true"},
		"isAudio":     {"true"},
	}
	if v := os.Getenv("MAX_PROTO"); v != "" {
		vals.Set("protocolVersion", v)
		vals.Set("capabilities", "203F")
	}
	if v := os.Getenv("MAX_OBSERVE"); v != "" {
		vals.Set("observedIds", v)
	}
	body, err := fbDo(methodJoinByLink, vals)
	if err != nil {
		return nil, err
	}
	var p convParams
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, fmt.Errorf("parse: %w (%.100s)", err, body)
	}
	if p.Endpoint == "" {
		return nil, fmt.Errorf("no endpoint (%.200s)", body)
	}
	return &p, nil
}

func fatal(f string, args ...any) {
	fmt.Fprintf(os.Stderr, "FATAL: "+f+"\n", args...)
	os.Exit(1)
}

// runMedia negotiates the consumer leg with the ok-calls SFU: the offer
// rides allocate-consumer with our candidates inline (the ice-lite server
// does not trickle first), the server answers with its description, and
// any further candidates arrive as transmit-data frames.
type mediaOnceGuard struct{ done bool }

func (g *mediaOnceGuard) TryClaim() bool {
	if g.done {
		return false
	}
	g.done = true
	return true
}

var mediaOnce mediaOnceGuard

func fullCapabilities() map[string]any {
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

func runMedia(r *joinResult, conn *websocket.Conn, params *convParams, seq *int) {
	me := &webrtc.MediaEngine{}
	_ = me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
		PayloadType:        96,
	}, webrtc.RTPCodecTypeVideo)
	_ = me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		PayloadType:        111,
	}, webrtc.RTPCodecTypeAudio)

	se := webrtc.SettingEngine{}
	se.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4, webrtc.NetworkTypeTCP4})
	if ln, err := net.Listen("tcp", "0.0.0.0:0"); err == nil {
		se.SetICETCPMux(ice.NewTCPMuxDefault(ice.TCPMuxParams{Listener: ln, ReadBufferSize: 1500}))
	}
	api := webrtc.NewAPI(webrtc.WithMediaEngine(me), webrtc.WithSettingEngine(se))

	pc, err := api.NewPeerConnection(webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{{
			URLs:       params.TURN.URLs,
			Username:   params.TURN.Username,
			Credential: params.TURN.Credential,
		}},
	})
	if err != nil {
		fmt.Printf("[media] pc: %v\n", err)
		return
	}
	defer pc.Close()

	// consumer shape: recvonly audio + video, plus a data m-line
	for i := 0; i < 2; i++ {
		_, _ = pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{
			Direction: webrtc.RTPTransceiverDirectionRecvonly})
	}
	_, _ = pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionRecvonly})
	_, _ = pc.CreateDataChannel("datagram", nil)

	var received int64
	var mu sync.Mutex
	var remoteSet = make(chan struct{}, 1)
	pc.OnTrack(func(t *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		buf := make([]byte, 1500)
		for {
			n, _, err := t.Read(buf)
			if err != nil {
				return
			}
			mu.Lock()
			received += int64(n)
			mu.Unlock()
		}
	})
	pc.OnConnectionStateChange(func(st webrtc.PeerConnectionState) { fmt.Printf("[media] PC %s\n", st) })
	pc.OnICEConnectionStateChange(func(st webrtc.ICEConnectionState) { fmt.Printf("[media] ICE %s\n", st) })

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		fmt.Printf("[media] offer: %v\n", err)
		return
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		fmt.Printf("[media] local: %v\n", err)
		return
	}
	deadline := time.Now().Add(6 * time.Second)
	for pc.ICEGatheringState() != webrtc.ICEGatheringStateComplete && time.Now().Before(deadline) {
		time.Sleep(150 * time.Millisecond)
	}
	local := pc.LocalDescription()
	kept := []string{}
	for _, ln := range strings.Split(local.SDP, "\r\n") {
		if strings.HasPrefix(ln, "a=candidate:") {
			if strings.Contains(ln, " tcp ") {
				continue
			}
			f := strings.Split(ln, " ")
			if len(f) > 4 && (strings.HasPrefix(f[4], "10.") || strings.HasPrefix(f[4], "172.") || strings.HasPrefix(f[4], "192.168.")) {
				continue
			}
		}
		kept = append(kept, ln)
	}
	sdpFull := strings.Join(kept, "\r\n")

	*seq++
	if err := conn.WriteJSON(map[string]any{
		"command":      "allocate-consumer",
		"sequence":     *seq,
		"description":  sdpFull,
		"capabilities": fullCapabilities(),
	}); err != nil {
		fmt.Printf("[media] send: %v\n", err)
		return
	}
	fmt.Printf("[media] allocate-consumer with candidates-in-SDP (%d bytes)\n", len(sdpFull))

	// read loop: apply the server answer, then keep applying trickled
	// candidates and count RTP until the window closes
	window := time.Now().Add(40 * time.Second)
	_ = conn.SetReadDeadline(window)
	for time.Now().Before(window) {
		mt, data, err := conn.ReadMessage()
		if err != nil {
			break
		}
		if mt == websocket.TextMessage && string(data) == "ping" {
			_ = conn.WriteMessage(websocket.TextMessage, []byte("pong"))
			continue
		}
		var m map[string]any
		if json.Unmarshal(data, &m) != nil {
			continue
		}
		if d, ok := m["description"].(string); ok && len(d) > 100 {
			_ = os.WriteFile("/tmp/max-sfu-answer.sdp", []byte(d), 0o644)
			if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: d}); err != nil {
				fmt.Printf("[media] remote: %v\n", err)
				continue
			}
			fmt.Printf("[media] SFU answer applied (%d bytes)\n", len(d))
			select {
			case remoteSet <- struct{}{}:
			default:
			}
			continue
		}
		if dm, ok := m["data"].(map[string]any); ok {
			if cand, ok := dm["candidate"].(map[string]any); ok {
				c, _ := cand["candidate"].(string)
				if c != "" {
					mid, _ := cand["sdpMid"].(string)
					mline := uint16(0)
					mlinePtr := &mline
					if v, ok := cand["sdpMLineIndex"].(float64); ok {
						mline = uint16(v)
					}
					_ = pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: c, SDPMid: &mid, SDPMLineIndex: mlinePtr})
					fmt.Printf("[media] +server candidate %.70s\n", c)
				}
				continue
			}
		}
	}
	mu.Lock()
	total := received
	mu.Unlock()
	fmt.Printf("[media] received %d bytes RTP (%.1f kbit/s over the window)\n", total, float64(total)*8/40/1000)
	if total > 100_000 {
		fmt.Println("[media] FLOW: PASS")
	} else {
		fmt.Println("[media] FLOW: NO/LOW DATA")
	}
}

func turnURL(p *convParams) string {
	if len(p.TURN.URLs) > 0 {
		return p.TURN.URLs[0]
	}
	return "turn:155.212.199.178:19302"
}
