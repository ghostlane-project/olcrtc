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
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	apiBase         = "https://calls.okcdn.ru"
	applicationKey  = "CNHIJPLGDIHBABABA"
	methodAnonLogin = "auth.anonymLogin"
	methodJoinByLink = "vchat.joinConversationByLink"
)

type joinResult struct {
	Idx        int
	UID        string
	PeerID     int64
	Endpoint   string
	WSError    string
	AliveAt    time.Duration
	Frames     int
	Err        string
}

func main() {
	n := flag.Int("n", 5, "parallel joins")
	hold := flag.Duration("hold", 60*time.Second, "how long to hold the connections")
	linkFile := flag.String("link-file", "/tmp/max-multi-link.txt", "file with the join link")
	mode := flag.String("mode", "join", "join: by link + web token; params: session key + conversation id")
	sessionKey := flag.String("session-key", "", "calls session key (params mode)")
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
	wsURL := params.Endpoint
	if uid := params.TURN.Username; uid != "" {
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
		Username string `json:"username"`
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
	body, err := fbDo(methodJoinByLink, url.Values{
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
		return nil, fmt.Errorf("no endpoint (%.200s)", body)
	}
	return &p, nil
}

func fatal(f string, args ...any) {
	fmt.Fprintf(os.Stderr, "FATAL: "+f+"\n", args...)
	os.Exit(1)
}
