// Package max is the auth provider for the VK MAX messenger's calls.
//
// MAX is the VK ecosystem's messenger (web.max.ru), and its calls ride
// the same ok-calls infrastructure Odnoklassniki uses (oneme.ru / okcdn.ru).
// The service is in the mobile operators' whitelists as part of "all VK
// services" and "all MTS services" - the reason this carrier matters.
//
// The auth flow:
//
//   - the caller's calls token (a short-lived token the MAX web client
//     receives over its messaging WebSocket; NOT the __oneme_auth
//     localStorage entry, which the calls API rejects) is exchanged for
//     an anonymous calls session at auth.anonymLogin, which answers a
//     session_key and a session_secret;
//   - that session key then asks vchat.getConversationParams for the room's
//     connection: a WebSocket endpoint on the SFU, TURN credentials, and a
//     signaling token - everything the engine joins the call with.
//
// The token is carried through auth.Config.Token (auth.token in the yaml).
// The provisioner keeps the messaging WebSocket alive to refresh it; a
// bare token lives about half an hour.
package max

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/openlibrecommunity/olcrtc/internal/auth"
	"github.com/openlibrecommunity/olcrtc/internal/protect"
)

const (
	// defaultAPIBase is where the calls API lives: the ok-calls CDN.
	defaultAPIBase = "https://calls.okcdn.ru"
	// applicationKey is the calls SDK's public application key (the same
	// constant the web client sends with every fb.do call).
	applicationKey = "CNHIJPLGDIHBABABA"

	// methodAnonymLogin exchanges a web auth token for a calls session.
	methodAnonymLogin = "auth.anonymLogin"
	// methodGetConversationParams asks for a room's connection parameters.
	methodGetConversationParams = "vchat.getConversationParams"
	// methodCreateConversation creates a group call and answers its join link.
	methodJoinByLink = "vchat.joinConversationByLink"
)

// Provider produces call credentials for the MAX messenger.
type Provider struct{}

// Engine reports which engine consumes credentials from this provider.
func (Provider) Engine() string { return "max" }

// DefaultServiceURL returns the service's web base.
func (Provider) DefaultServiceURL() string { return "https://max.ru" }

// Issue exchanges the caller's MAX web token for call credentials: an
// anonymous calls session first, then the room's connection parameters.
func (p Provider) Issue(ctx context.Context, cfg auth.Config) (auth.Credentials, error) {
	if cfg.Token == "" {
		return auth.Credentials{}, fmt.Errorf("%w: a MAX web token (auth.token) is required", auth.ErrRoomIDRequired)
	}
	resolver := cfg.Resolver
	if resolver == nil {
		resolver = protect.NewResolver(cfg.DNSServer)
	}
	client := protect.NewHTTPClient(resolver)

	// The gate and the provisioner hand out long-lived calls sessions
	// (session keys, 20h+) as the token; a browser-issued calls token
	// (auth_token, "An_..") goes through the anonymLogin exchange instead.
	session := &anonymSession{SessionKey: cfg.Token}
	if isCallsToken(cfg.Token) {
		exchanged, err := anonymLogin(ctx, client, cfg.Token, cfg.Name)
		if err != nil {
			return auth.Credentials{}, fmt.Errorf("max: anonymous login: %w", err)
		}
		session = exchanged
	}

	// A room link names the call to join; without one a fresh group call is
	// created and its link returned for the other side.
	roomURL := cfg.RoomURL
	conversationID := ""
	if i := strings.LastIndex(roomURL, "/joincall/"); i >= 0 {
		conversationID = roomURL[i+len("/joincall/"):]
		conversationID = strings.TrimRight(conversationID, "/?#")
	}

	// A join link names the call; without one the provider cannot start one
	// from here (creating calls needs the messenger API, not the calls one).
	if conversationID == "" {
		return auth.Credentials{}, fmt.Errorf("%w: a MAX join link (room.id) is required", auth.ErrRoomIDRequired)
	}

	params, err := joinByLink(ctx, client, session, conversationID)
	if err != nil {
		return auth.Credentials{}, fmt.Errorf("max: conversation params: %w", err)
	}

	// The engine's pair rule (the lower uid offers) needs our uid; the
	// join answer's endpoint query carries it, so both token shapes are
	// covered without a second API call.
	selfUID := session.UID
	if m := uidFromEndpoint(params.Endpoint); m != "" && (selfUID == "" || selfUID == cfg.Name) {
		selfUID = m
	}
	extra := map[string]string{
		"sessionId":      conversationID,
		"sessionKey":     session.SessionKey,
		"endpoint":       params.Endpoint,
		"turnUrl":        strings.Join(params.TURN.URLs, ","),
		"turnUsername":   params.TURN.Username,
		"turnCredential": params.TURN.Credential,
		"stunUrl":        strings.Join(params.STUN.URLs, ","),
		"userId":         selfUID,
	}
	if params.WTEndpoint != "" {
		extra["wtEndpoint"] = params.WTEndpoint
	}
	return auth.Credentials{
		URL:   params.Endpoint,
		Token: params.Token,
		Extra: extra,
	}, nil
}

// anonymSession is what auth.anonymLogin answers.
type anonymSession struct {
	UID              string `json:"uid"`
	SessionKey       string `json:"session_key"`
	SessionSecretKey string `json:"session_secret_key"`
	APIServer        string `json:"api_server"`
	ExternalUserID   string `json:"external_user_id"`
}

// convParams is what vchat.getConversationParams answers.
type convParams struct {
	Endpoint   string `json:"endpoint"`
	WTEndpoint string `json:"wt_endpoint"`
	Token      string `json:"token"`
	TURN       struct {
		URLs       []string `json:"urls"`
		Username   string   `json:"username"`
		Credential string   `json:"credential"`
	} `json:"turn_server"`
	STUN struct {
		URLs []string `json:"urls"`
	} `json:"stun_server"`
	DeviceIdx  int    `json:"device_idx"`
	ClientType string `json:"client_type"`
}

// fbDo is one call to the service's fb.do method endpoint.
func fbDo(ctx context.Context, client *http.Client, method string, params url.Values) ([]byte, error) {
	params.Set("method", method)
	params.Set("format", "JSON")
	params.Set("application_key", applicationKey)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		defaultAPIBase+"/fb.do", strings.NewReader(params.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15")
	req.Header.Set("Origin", "https://web.max.ru")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body := make([]byte, 0, 4096)
	buf := make([]byte, 4096)
	for {
		n, rerr := resp.Body.Read(buf)
		body = append(body, buf[:n]...)
		if rerr != nil {
			break
		}
	}
	return body, nil
}

// anonymLogin exchanges the MAX web token for a calls session.
func anonymLogin(ctx context.Context, client *http.Client, webToken, deviceID string) (*anonymSession, error) {
	sessionData, _ := json.Marshal(map[string]any{
		"version":        3,
		"device_id":      deviceID,
		"client_version": "1.1",
		"client_type":    "SDK_JS",
		"auth_token":     webToken,
	})
	params := url.Values{"session_data": {string(sessionData)}}
	body, err := fbDo(ctx, client, methodAnonymLogin, params)
	if err != nil {
		return nil, err
	}
	var s anonymSession
	if err := json.Unmarshal(body, &s); err != nil {
		return nil, fmt.Errorf("parse: %w (body: %.100s)", err, body)
	}
	if s.SessionKey == "" {
		return nil, fmt.Errorf("no session key (body: %.200s)", body)
	}
	return &s, nil
}

// joinByLink joins a call by its join link and answers the connection.
func joinByLink(ctx context.Context, client *http.Client, s *anonymSession, joinLink string) (*convParams, error) {
	params := url.Values{
		"session_key": {s.SessionKey},
		"joinLink":    {joinLink},
		"isVideo":     {"true"},
		"isAudio":     {"true"},
	}
	body, err := fbDo(ctx, client, methodJoinByLink, params)
	if err != nil {
		return nil, err
	}
	var p convParams
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, fmt.Errorf("parse: %w (body: %.100s)", err, body)
	}
	if p.Endpoint == "" {
		return nil, fmt.Errorf("no endpoint (body: %.200s)", body)
	}
	return &p, nil
}

// isCallsToken says the token is a browser-issued calls auth_token (the
// anonymLogin exchange) rather than a ready session key.
func isCallsToken(token string) bool {
	return strings.HasPrefix(token, "An_") || strings.HasPrefix(token, "$")
}

// uidFromEndpoint reads userId= out of the join endpoint's query.
func uidFromEndpoint(endpoint string) string {
	i := strings.Index(endpoint, "userId=")
	if i < 0 {
		return ""
	}
	v := endpoint[i+len("userId="):]
	if j := strings.IndexAny(v, "&?#"); j >= 0 {
		v = v[:j]
	}
	return v
}

var _ = strconv.Itoa
