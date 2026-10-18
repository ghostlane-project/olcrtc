// Package max provides the engine for VK MAX calls (olcrtc carrier).
//
// The measured carrier path: the join endpoint's WebSocket speaks the
// ok-calls JSON protocol; after accept-call the session offers a recvonly
// PC (two video transceivers, one audio, the "datagram" datachannel - a
// shape the counterparty's SDK answers) to the other participant through
// the transmit-data relay, applies its answer and the trickled ICE, and
// the tunnel rides the datachannel. Candidates trickle one per relayed
// frame (the ~3.5KB relay cap), the send side is gated on the SCTP buffer
// (an ungated pump OOMs), and the PC carries UDP and TCP network types
// with a local TCP mux for the server's passive ICE-TCP candidate.
package max

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/ice/v4"
	"github.com/pion/webrtc/v4"

	"github.com/openlibrecommunity/olcrtc/internal/engine"
	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/internal/protect"
)

// Errors.
var (
	ErrSessionClosed = fmt.Errorf("max: session closed")
	ErrNoEndpoint    = fmt.Errorf("max: no signaling endpoint")
)

// webrtcCandidate aliases the pion init struct so signaling.go needs no
// webrtc import of its own.
type webrtcCandidate = webrtc.ICECandidateInit

// The SCTP buffer ceiling for the send side: an ungated writer drains RAM
// into the SCTP queue (measured OOM at 9GB); hold at this watermark and
// let the reader drain it.
const sctpHighWater = 4 * 1024 * 1024

// Session is one MAX call.
type Session struct {
	*engine.Reconnector

	endpoint string
	turn     []webrtc.ICEServer

	peer    atomic.Int64 // the other participant's uid, from accept-call
	selfUID int64
	pc      atomic.Pointer[webrtc.PeerConnection]
	dc      atomic.Pointer[webrtc.DataChannel]
	sig     atomic.Pointer[signaling]

	onData  func([]byte)
	closed  atomic.Bool
	closeCh chan struct{}
	wg      sync.WaitGroup
}

// New creates a MAX call session from the auth provider's credentials.
func New(_ context.Context, cfg engine.Config) (engine.Session, error) {
	endpoint := cfg.URL
	if endpoint == "" {
		return nil, ErrNoEndpoint
	}
	var servers []webrtc.ICEServer
	if urls := strings.Split(cfg.Extra["turnUrl"], ","); len(urls) > 0 && urls[0] != "" {
		servers = append(servers, webrtc.ICEServer{
			URLs:       urls,
			Username:   cfg.Extra["turnUsername"],
			Credential: cfg.Extra["turnCredential"],
		})
	}
	if stun := cfg.Extra["stunUrl"]; stun != "" {
		servers = append(servers, webrtc.ICEServer{URLs: strings.Split(stun, ",")})
	}
	rec := &engine.Reconnector{}
	s := &Session{
		Reconnector: rec,
		endpoint:    endpoint,
		turn:        servers,
		selfUID:     parseUID(cfg.Extra["userId"]),
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

// Connect opens the signaling WebSocket and negotiates the P2P leg.
func (s *Session) Connect(ctx context.Context) error {
	if s.closed.Load() {
		return ErrSessionClosed
	}
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

	// First frame: the connection notification assigns our peerId.
	msg, _, err := sig.recv()
	if err != nil || msg == nil {
		_ = conn.Close()
		return fmt.Errorf("max: no connection notification: %v", err)
	}
	var notif connectionNotification
	if err := parseNotification(msg, &notif); err != nil || notif.PeerID.ID == 0 {
		_ = conn.Close()
		return fmt.Errorf("max: unexpected first frame: %v", err)
	}
	if len(s.turn) == 0 && len(notif.ConversationParams.TURN.URLs) > 0 {
		s.turn = append(s.turn, webrtc.ICEServer{
			URLs:       notif.ConversationParams.TURN.URLs,
			Username:   notif.ConversationParams.TURN.Username,
			Credential: notif.ConversationParams.TURN.Credential,
		})
	}
	logger.Infof("max: joined %s (topology %s) as peer %d",
		shortConv(notif.Conversation.ID), notif.Conversation.Topology, notif.PeerID.ID)

	pc, dc, err := s.buildPeerConnection(sig)
	if err != nil {
		_ = pc.Close()
		_ = conn.Close()
		return err
	}
	s.pc.Store(pc)
	s.dc.Store(dc)

	if err := sig.sendCommand("accept-call", map[string]any{
		"mediaSettings": map[string]bool{
			"isAudioEnabled": false, "isVideoEnabled": true,
			"isScreenSharingEnabled": false, "isFastScreenSharingEnabled": false,
			"isAudioSharingEnabled": false, "isAnimojiEnabled": false,
		},
	}); err != nil {
		_ = pc.Close()
		_ = conn.Close()
		return fmt.Errorf("max: accept-call: %w", err)
	}

	s.wg.Add(1)
	go s.readLoop(sig, pc, dc)
	logger.Infof("max: connected to %s", strings.SplitN(s.endpoint, "?", 2)[0])
	return nil
}

// buildPeerConnection assembles the measured carrier shape: recvonly
// audio and video plus the "datagram" channel, UDP and TCP ICE with a
// local TCP mux, and the candidate trickle hooked to the relay.
func (s *Session) buildPeerConnection(sig *signaling) (*webrtc.PeerConnection, *webrtc.DataChannel, error) {
	se := webrtc.SettingEngine{}
	se.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4, webrtc.NetworkTypeTCP4})
	if ln, err := net.Listen("tcp", "0.0.0.0:0"); err == nil {
		se.SetICETCPMux(ice.NewTCPMuxDefault(ice.TCPMuxParams{Listener: ln, ReadBufferSize: 1500}))
	}
	api := webrtc.NewAPI(webrtc.WithSettingEngine(se))

	pc, err := api.NewPeerConnection(webrtc.Configuration{ICEServers: s.turn})
	if err != nil {
		return nil, nil, fmt.Errorf("max: peer connection: %w", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo,
			webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
			return nil, nil, fmt.Errorf("max: video transceiver: %w", err)
		}
	}
	if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
		return nil, nil, fmt.Errorf("max: audio transceiver: %w", err)
	}
	dc, err := pc.CreateDataChannel("datagram", nil)
	if err != nil {
		return nil, nil, fmt.Errorf("max: datachannel: %w", err)
	}
	dc.OnMessage(func(m webrtc.DataChannelMessage) {
		if s.onData != nil {
			s.onData(m.Data)
		}
	})
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		peer := s.peer.Load()
		if peer == 0 {
			return
		}
		init := c.ToJSON()
		_ = sig.sendCandidate(peer, init)
	})
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		logger.Debugf("max: pc state: %s", state)
		if state == webrtc.PeerConnectionStateFailed {
			s.Reconnect("peer connection failed")
		}
	})
	return pc, dc, nil
}

// readLoop drives the negotiation: accept-call's response names the other
// participant, the lower uid offers, and answers, offers and candidates
// flow through the relay until the datachannel is up.
func (s *Session) readLoop(sig *signaling, pc *webrtc.PeerConnection, dc *webrtc.DataChannel) {
	defer s.wg.Done()
	for {
		msg, resp, err := sig.recv()
		if err != nil {
			if !s.closed.Load() {
				logger.Debugf("max: signaling read: %v", err)
				s.Reconnect("signaling closed")
			}
			return
		}
		if msg == nil {
			continue
		}
		switch {
		case msg.Type == "ping":
			_ = sig.sendPong()

		case resp != nil && resp.Response == "accept-call":
			if len(resp.ParticipantIDs) > 0 {
				s.handlePeerSeen(pc, sig, resp.ParticipantIDs[0])
			}

		case resp != nil && resp.Error != "":
			logger.Debugf("max: server error: %s (%s)", resp.Error, resp.Message)

		case msg.Data != nil:
			var payload transmitPayload
			if err := jsonUnmarshal(msg.Data, &payload); err != nil {
				continue
			}
			peer := s.peer.Load()
			switch {
			case payload.SDP != nil:
				if payload.SDP.Type == "answer" {
					s.handleAnswer(pc, payload.SDP.SDP)
				} else if payload.SDP.Type == "offer" {
					s.handleIncomingOffer(pc, sig, peer, payload.SDP.SDP)
				}
			case payload.Candidate != nil && peer != 0:
				init := webrtc.ICECandidateInit{Candidate: payload.Candidate.Candidate}
				if payload.Candidate.SDPMid != nil {
					init.SDPMid = payload.Candidate.SDPMid
				}
				if payload.Candidate.SDPMLineIndex != nil {
					init.SDPMLineIndex = payload.Candidate.SDPMLineIndex
				}
				_ = pc.AddICECandidate(init)
			}
		}
	}
}

// handlePeerSeen records the counterparty and offers once: the lower uid
// speaks first so a pair of sessions never races. The offer shape is the
// one the measured SDK answers (recvonly + datachannel), with candidates
// stripped from the SDP and trickled separately under the relay cap.
func (s *Session) handlePeerSeen(pc *webrtc.PeerConnection, sig *signaling, other int64) {
	if !s.peer.CompareAndSwap(0, other) {
		return
	}
	logger.Infof("max: peer participant %d", other)
	if s.selfUID != 0 && s.selfUID > other {
		return // the lower uid offers; we answer instead
	}
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		logger.Debugf("max: offer: %v", err)
		return
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		logger.Debugf("max: local offer: %v", err)
		return
	}
	stripped := stripCandidates(offer.SDP)
	if err := sig.sendSDPExchange(other, "offer", stripped); err != nil {
		logger.Debugf("max: offer relay: %v", err)
		return
	}
	logger.Infof("max: offer relayed to %d (%d bytes SDP)", other, len(stripped))
}

// handleAnswer applies the peer's answer.
func (s *Session) handleAnswer(pc *webrtc.PeerConnection, sdp string) {
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer, SDP: sdp,
	}); err != nil {
		logger.Debugf("max: remote answer: %v", err)
		return
	}
	logger.Infof("max: answer applied (%d bytes)", len(sdp))
}

// handleIncomingOffer answers a peer that offered first.
func (s *Session) handleIncomingOffer(pc *webrtc.PeerConnection, sig *signaling, peer int64, sdp string) {
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer, SDP: sdp,
	}); err != nil {
		logger.Debugf("max: remote offer: %v", err)
		return
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		logger.Debugf("max: answer create: %v", err)
		return
	}
	if err := pc.SetLocalDescription(answer); err != nil {
		logger.Debugf("max: local answer: %v", err)
		return
	}
	stripped := stripCandidates(answer.SDP)
	if peer != 0 {
		if err := sig.sendSDPExchange(peer, "answer", stripped); err != nil {
			logger.Debugf("max: answer relay: %v", err)
		}
	}
	logger.Infof("max: answered incoming offer (%d bytes)", len(stripped))
}

// Send writes to the datachannel, holding at the SCTP watermark so the
// buffer cannot run away.
func (s *Session) Send(data []byte) error {
	dc := s.dc.Load()
	if dc == nil || dc.ReadyState() != webrtc.DataChannelStateOpen {
		return ErrSessionClosed
	}
	for dc.BufferedAmount() > sctpHighWater {
		if s.closed.Load() {
			return ErrSessionClosed
		}
		time.Sleep(2 * time.Millisecond)
		dc = s.dc.Load()
		if dc == nil {
			return ErrSessionClosed
		}
	}
	return dc.Send(data)
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
		_ = sig.close()
	}
	s.wg.Wait()
	return nil
}

// CanSend reports whether the datachannel carries data.
func (s *Session) CanSend() bool {
	dc := s.dc.Load()
	return !s.closed.Load() && dc != nil && dc.ReadyState() == webrtc.DataChannelStateOpen
}

// reconnect re-establishes the session on the same credentials.
func (s *Session) reconnect(ctx context.Context) error {
	_ = s.Close()
	s.closed.Store(false)
	s.closeCh = make(chan struct{})
	s.peer.Store(0)
	return s.Connect(ctx)
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

// GetBufferedAmount reports the SCTP queue depth.
func (s *Session) GetBufferedAmount() uint64 {
	if dc := s.dc.Load(); dc != nil {
		return uint64(dc.BufferedAmount())
	}
	return 0
}

// WatchConnection reports the connection's health.
func (s *Session) WatchConnection(ctx context.Context) {}

// SubscriberCanSend reports whether the subscriber can send.
func (s *Session) SubscriberCanSend() bool {
	return s.CanSend()
}
