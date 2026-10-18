// Package max provides the engine for VK MAX calls (olcrtc carrier).
//
// The flow: the auth provider supplies a WebSocket endpoint (already
// carrying auth, userId, and conversationId as query parameters) and TURN
// credentials; the engine opens the binary signaling, creates a pion
// PeerConnection with the SFU's TURN, and carries the tunnel as a VP8
// video track - the same vp8channel shape the other carriers ride.
package max

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"

	"github.com/openlibrecommunity/olcrtc/internal/engine"
	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/internal/protect"
)

// Errors.
var (
	ErrSessionClosed = fmt.Errorf("max: session closed")
	ErrNoEndpoint    = fmt.Errorf("max: no signaling endpoint")
)

// Session is one MAX call.
type Session struct {
	*engine.Reconnector

	endpoint string
	cookies  string
	turn     webrtc.ICEServer

	pc    atomic.Pointer[webrtc.PeerConnection]
	track atomic.Pointer[webrtc.TrackLocalStaticSample]
	sig   atomic.Pointer[signaling]

	onData  func([]byte)
	closed  atomic.Bool
	closeCh chan struct{}
	wg      sync.WaitGroup
	mu      sync.Mutex
}

// New creates a MAX call session from the auth provider's credentials.
func New(_ context.Context, cfg engine.Config) (engine.Session, error) {
	endpoint := cfg.URL
	if endpoint == "" {
		return nil, ErrNoEndpoint
	}

	// TURN from the credentials.
	turnURLs := strings.Split(cfg.Extra["turnUrl"], ",")
	var user, pass string
	if len(turnURLs) > 0 && turnURLs[0] != "" {
		user = cfg.Extra["turnUsername"]
		pass = cfg.Extra["turnCredential"]
	}
	turn := webrtc.ICEServer{
		URLs:       turnURLs,
		Username:   user,
		Credential: pass,
	}

	rec := &engine.Reconnector{}
	s := &Session{
		Reconnector: rec,
		endpoint:    endpoint,
		turn:        turn,
		onData:      cfg.OnData,
		closeCh:     make(chan struct{}),
	}
	rec.Configure(engine.ReconnectorConfig{
		MaxAttempts: 10,
		Reconnect:   s.reconnect,
		OnError: func(err error) {
			logger.Debugf("max: reconnect failed: %v", err)
		},
		LimitReason: "reconnect limit reached",
	})
	return s, nil
}

// Connect opens the signaling WebSocket and the peer connection.
func (s *Session) Connect(ctx context.Context) error {
	if s.closed.Load() {
		return ErrSessionClosed
	}

	// Open the signaling WebSocket with the web client's Origin.
	dialer := protect.NewWebSocketDialer(15*time.Second, nil)
	header := http.Header{}
	header.Set("Origin", "https://web.max.ru")
	header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15")
	conn, _, err := dialer.Dial(s.endpoint, header)
	if err != nil {
		return fmt.Errorf("max: signaling dial: %w", err)
	}
	sig := newSignaling(conn)
	s.sig.Store(sig)

	// Build the PeerConnection with TURN.
	api, err := newAPI()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("max: api: %w", err)
	}
	cfg := webrtc.Configuration{ICEServers: []webrtc.ICEServer{s.turn}}
	pc, err := api.NewPeerConnection(cfg)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("max: peer connection: %w", err)
	}
	s.pc.Store(pc)

	// The tunnel track: VP8 video.
	track, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8},
		"olcrtc", "olcrtc",
	)
	if err != nil {
		_ = pc.Close()
		_ = conn.Close()
		return fmt.Errorf("max: track: %w", err)
	}
	s.track.Store(track)
	if _, err := pc.AddTrack(track); err != nil {
		_ = pc.Close()
		_ = conn.Close()
		return fmt.Errorf("max: add track: %w", err)
	}

	// Handle the SFU's media.
	pc.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		logger.Infof("max: remote track: %s %s", remote.Codec().MimeType, remote.ID())
		s.wg.Add(1)
		defer s.wg.Done()
		buf := make([]byte, 1500)
		for {
			n, _, err := remote.Read(buf)
			if err != nil {
				return
			}
			if s.onData != nil {
				data := make([]byte, n)
				copy(data, buf[:n])
				s.onData(data)
			}
		}
	})

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		logger.Debugf("max: pc state: %s", state)
		if state == webrtc.PeerConnectionStateFailed {
			s.Reconnect("peer connection failed")
		}
	})

	// Read signaling messages: JSON commands, binary SDP, and "ping" strings.
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			json, binary, err := sig.recv()
			if err != nil {
				if !s.closed.Load() {
					logger.Debugf("max: signaling read: %v", err)
					s.Reconnect("signaling closed")
				}
				return
			}
			if json != nil {
				s.handleMessage(json)
			}
			if binary != nil && binary.Params.Data.SDP != "" {
				logger.Infof("max: received SDP %s (%d bytes)", binary.Params.Data.Type, len(binary.Params.Data.SDP))
				if binary.Params.Data.Type == "offer" {
					s.handleOffer(binary.Params.Data.SDP)
				}
			}
		}
	}()

	logger.Infof("max: connected to %s", strings.SplitN(s.endpoint, "?", 2)[0])
	return nil
}

// handleMessage processes one JSON signaling message from the SFU.
func (s *Session) handleMessage(msg *sigMessage) {
	switch msg.Type {
	case "ping":
		// Answer the keepalive.
		if sig := s.sig.Load(); sig != nil {
			_ = sig.sendPong()
		}
	case "notification":
		var n notification
		if err := json.Unmarshal(msg.Data, &n); err == nil && n.PeerID.ID != 0 {
			logger.Infof("max: assigned peerId=%d", n.PeerID.ID)
		}
	case "offer", "answer":
		// JSON SDP (fallback path); binary TRANSMIT_DATA is the primary.
		var sd sdpExchange
		if err := json.Unmarshal(msg.Data, &sd); err == nil && sd.SDP != "" {
			if msg.Type == "offer" {
				s.handleOffer(sd.SDP)
			}
		}
	case "response":
		// Command acknowledged.
	default:
		logger.Debugf("max: signal: type=%s", msg.Type)
	}
}

// handleOffer answers the SFU's SDP offer.
func (s *Session) handleOffer(sdp string) {
	pc := s.pc.Load()
	if pc == nil {
		return
	}
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer, SDP: sdp,
	}); err != nil {
		logger.Debugf("max: set remote: %v", err)
		return
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		logger.Debugf("max: create answer: %v", err)
		return
	}
	if err := pc.SetLocalDescription(answer); err != nil {
		logger.Debugf("max: set local: %v", err)
		return
	}
	// Send the answer back on the signaling as binary TRANSMIT_DATA.
	sig := s.sig.Load()
	if sig != nil {
		_ = sig.sendBinary(0, answer.SDP, "answer")
	}
}

// Send writes to the tunnel track.
func (s *Session) Send(data []byte) error {
	track := s.track.Load()
	if track == nil {
		return ErrSessionClosed
	}
	return track.WriteSample(media.Sample{Data: data, Duration: time.Millisecond * 20})
}

// Close ends the session.
func (s *Session) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	close(s.closeCh)
	if pc := s.pc.Load(); pc != nil {
		_ = pc.Close()
	}
	if sig := s.sig.Load(); sig != nil {
		_ = sig.conn.Close()
	}
	s.wg.Wait()
	return nil
}

// CanSend reports whether the session can carry data.
func (s *Session) CanSend() bool {
	return !s.closed.Load() && s.track.Load() != nil
}

// reconnect re-establishes the session.
func (s *Session) reconnect(ctx context.Context) error {
	_ = s.Close()
	s.closed.Store(false)
	s.closeCh = make(chan struct{})
	return s.Connect(ctx)
}

// newAPI builds a pion API with the default interceptors.
func newAPI() (*webrtc.API, error) {
	mediaEngine := &webrtc.MediaEngine{}
	if err := mediaEngine.RegisterDefaultCodecs(); err != nil {
		return nil, err
	}
	registry := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptorsWithOptions(
		mediaEngine, registry, engine.DefaultInterceptorOptions()...,
	); err != nil {
		return nil, err
	}
	return webrtc.NewAPI(
		webrtc.WithMediaEngine(mediaEngine),
		webrtc.WithInterceptorRegistry(registry),
	), nil
}

// Reconnect triggers a reconnection (engine.Session).
func (s *Session) Reconnect(reason string) {
	logger.Infof("max: reconnect: %s", reason)
	go func() {
		if err := s.reconnect(context.Background()); err != nil {
			logger.Debugf("max: reconnect: %v", err)
		}
	}()
}

// GetBufferedAmount reports the track's buffered amount.
func (s *Session) GetBufferedAmount() uint64 {
	if track := s.track.Load(); track != nil {
		return 0 // TODO: wire the track's buffered amount
	}
	return 0
}

// WatchConnection reports the connection's health.
func (s *Session) WatchConnection(ctx context.Context) {}

// SubscriberCanSend reports whether the subscriber can send.
func (s *Session) SubscriberCanSend() bool {
	return s.CanSend()
}
