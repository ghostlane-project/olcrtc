package vkcalls

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/openlibrecommunity/olcrtc/internal/engine"
	"github.com/openlibrecommunity/olcrtc/internal/logger"
)

// Session errors.
var (
	ErrSessionClosed   = errors.New("vkcalls: session closed")
	ErrNoEndpoint      = errors.New("vkcalls: signaling endpoint missing")
	ErrOfferTimeout    = errors.New("vkcalls: no producer offer arrived")
	ErrConnectTimeout  = errors.New("vkcalls: peer connection did not connect")
	ErrSendUnsupported = errors.New("vkcalls: send requires the video transport")
)

const (
	offerWait    = 30 * time.Second
	connectWait  = 30 * time.Second
	layoutSeqMod = 1 << 20
)

// referenceCapabilities is the allocate-consumer feature report: only what
// this engine implements. The SDK's own mask is a diagnostic reference, not a
// constant to copy (spec 7.4).
func referenceCapabilities() map[string]any {
	return map[string]any{
		"estimatedPerformanceIndex": 0, "audioMix": true, "consumerUpdate": true,
		"producerNotificationDataChannelVersion": 8, "producerCommandDataChannelVersion": 3,
		"consumerScreenDataChannelVersion": 1, "producerScreenDataChannelVersion": 1,
		"asrDataChannelVersion": 1, "animojiDataChannelVersion": 2, "animojiBackendRender": true,
		"onDemandTracks": true, "unifiedPlan": true, "singleSession": true, "videoTracksCount": 36,
		"red": true, "audioShare": true, "fastScreenShare": true, "videoSuspend": true,
		"simulcast": true, "simulcastNativeOrder": true, "consumerFastScreenShare": false,
		"consumerFastScreenShareQualityOnDemand": false, "transparentAudio": false,
	}
}

// Session is an engine.Session over one VK Calls SERVER conversation. Payload
// rides the video track (the videochannel transport on top); the SFU bridges
// no arbitrary data channel, so Send reports unsupported.
type Session struct {
	cfg      engine.Config
	endpoint string
	peerID   string
	iceJSON  string

	video engine.VideoTrackState

	mu      sync.Mutex
	gen     *generation
	closed  bool
	endOnce sync.Once
	ended   func(string)
}

// generation bundles everything one connection attempt owns, so closing it
// tears the whole attempt down (spec 7.3: state never crosses generations).
type generation struct {
	signal  *signalingClient
	pc      *webrtc.PeerConnection
	command *webrtc.DataChannel
	notify  *webrtc.DataChannel
	shape   *ShapeState

	registryMu sync.Mutex
	registry   Registry
	// participants are the others in the conversation, whose cameras the
	// layout asks for (by string key until the registry names them).
	participants map[string]bool
	layoutSeq    int
	sessionID    string
	connected    chan struct{}
	fireOnce     sync.Once
	reconnect    func()
	shouldRecn   func() bool
}

// New builds a VK Calls session from provider credentials.
func New(_ context.Context, cfg engine.Config) (engine.Session, error) {
	if cfg.URL == "" {
		return nil, ErrNoEndpoint
	}
	s := &Session{cfg: cfg, endpoint: cfg.URL, peerID: cfg.Extra["peer_id_hint"], iceJSON: cfg.Extra["ice_servers"]}
	return s, nil
}

// AddVideoTrack records a local track for the next connection.
func (s *Session) AddVideoTrack(track webrtc.TrackLocal) error { //nolint:unparam // the VideoTrackCapable contract
	s.video.StoreVideoTrack(track)
	return nil
}

// SetVideoTrackHandler registers the remote-track callback.
func (s *Session) SetVideoTrackHandler(cb func(*webrtc.TrackRemote, *webrtc.RTPReceiver)) {
	s.video.SetVideoTrackHandler(cb)
}

// Connect joins the conversation and brings the bundled peer connection to
// the connected state with an active media subscription.
func (s *Session) Connect(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrSessionClosed
	}
	s.mu.Unlock()

	// A fresh join carries no peer id: the hint names this client only to a
	// retry join (tgt=retry), which the reconnect path issues.
	signal, err := dialSignaling(ctx, s.endpoint, "", s.cfg.Resolver)
	if err != nil {
		return err
	}
	connection, err := signal.waitConnection(ctx)
	if err != nil {
		signal.close()
		return err
	}
	if _, err := signal.command(ctx, "allocate-consumer",
		map[string]any{"capabilities": referenceCapabilities()}); err != nil {
		signal.close()
		return err
	}
	if _, err := signal.command(ctx, "update-media-modifiers", map[string]any{
		"mediaModifiers": map[string]any{"denoise": true, "denoiseAnn": true},
	}); err != nil {
		signal.close()
		return err
	}

	gen := &generation{
		signal:    signal,
		shape:     NewShapeState(),
		registry:  Registry{},
		connected: make(chan struct{}),
		// The reconnect hook outlives this context; Reconnect owns its own.
		reconnect:  func() { go s.Reconnect("signaling") }, //nolint:contextcheck // deliberate detached reconnect
		shouldRecn: func() bool { return true },
	}
	offerNote := waitOffer(ctx, signal)
	if offerNote.Description == "" {
		signal.close()
		return ErrOfferTimeout
	}
	gen.sessionID = offerNote.SessionID
	if _, err := signal.command(ctx, "change-media-settings", map[string]any{
		"mediaSettings": map[string]any{"isAudioEnabled": false, "isVideoEnabled": false,
			"isScreenSharingEnabled": false, "isFastScreenSharingEnabled": false,
			"isAudioSharingEnabled": false, "isAnimojiEnabled": true},
	}); err != nil {
		signal.close()
		return err
	}
	if err := s.negotiate(ctx, gen, offerNote.Description); err != nil {
		gen.teardown()
		return err
	}

	s.mu.Lock()
	s.gen = gen
	s.mu.Unlock()
	go s.run(gen, connection) //nolint:contextcheck // the session owns its post-connect lifetime
	return nil
}

// negotiate answers one producer offer: the bundled peer connection mirrors
// the offer's codecs, the shaped answer passes the transport gate and
// accept-producer completes the session.
func (s *Session) negotiate(ctx context.Context, gen *generation, offer string) error {
	api, err := newWebRTCAPI(offer, s.cfg, s.cfg.Resolver)
	if err != nil {
		return err
	}
	pc, err := api.NewPeerConnection(webrtc.Configuration{ICEServers: iceServersOf(s.iceJSON)})
	if err != nil {
		return fmt.Errorf("vkcalls: new peer connection: %w", err)
	}
	gen.pc = pc

	// The remote description is set before tracks attach: AddTrack then
	// reuses the offer's publish transceiver instead of opening a fresh one
	// that would pair with a receive-only section (the worker's proven order).
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: offer}); err != nil {
		return fmt.Errorf("vkcalls: set remote description: %w", err)
	}
	// The publish slot carries the video transport's track; its SSRC lands in
	// the native answer and the shaper declares it to the SFU.
	s.video.RangeVideoTracks(func(track webrtc.TrackLocal, _ bool) {
		if _, err := pc.AddTrack(track); err != nil {
			logger.Debugf("vkcalls: add track: %v", err)
		}
	})
	pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		if handler := s.video.VideoTrackHandler(); handler != nil {
			handler(track, receiver)
		}
	})
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		logger.Debugf("vkcalls: peer connection %s", state)
		switch state {
		case webrtc.PeerConnectionStateConnected:
			select {
			case <-gen.connected:
			default:
				close(gen.connected)
			}
		case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
			gen.maybeReconnect()
		case webrtc.PeerConnectionStateNew, webrtc.PeerConnectionStateConnecting,
			webrtc.PeerConnectionStateDisconnected, webrtc.PeerConnectionStateUnknown:
		}
	})

	if err := s.openControlChannels(gen, pc); err != nil {
		return err
	}
	return s.acceptOffer(ctx, gen, offer)
}

// openControlChannels creates the two service data channels the engine
// speaks: producerCommand carries the layout subscription, and
// producerNotification delivers the stream registry.
func (s *Session) openControlChannels(gen *generation, pc *webrtc.PeerConnection) error {
	command, err := pc.CreateDataChannel("producerCommand", nil)
	if err != nil {
		return fmt.Errorf("vkcalls: producer command channel: %w", err)
	}
	gen.command = command
	// The participants known at connect, and a registry that arrived first,
	// are subscribed as soon as the channel can carry the layout.
	command.OnOpen(gen.subscribeAll)
	notify, err := pc.CreateDataChannel("producerNotification", nil)
	if err != nil {
		return fmt.Errorf("vkcalls: producer notification channel: %w", err)
	}
	gen.notify = notify
	notify.OnMessage(func(message webrtc.DataChannelMessage) {
		s.onServiceFrame(gen, message.Data)
	})
	return nil
}

// acceptOffer shapes the answer for the current offer and completes the
// accept-producer handshake.
func (s *Session) acceptOffer(ctx context.Context, gen *generation, offer string) error {
	native, err := gen.pc.CreateAnswer(nil)
	if err != nil {
		return fmt.Errorf("vkcalls: create answer: %w", err)
	}
	if localErr := gen.pc.SetLocalDescription(native); localErr != nil {
		return fmt.Errorf("vkcalls: set local description: %w", localErr)
	}
	shaped, err := ShapeAnswer(offer, native.SDP, gen.shape)
	if err != nil {
		return err
	}
	dumpNegotiation(offer, native.SDP, shaped)
	if _, err := gen.signal.command(ctx, "accept-producer", acceptPayload(offer, shaped, gen.sessionID)); err != nil {
		return err
	}
	select {
	case <-gen.connected:
		return nil
	case <-time.After(connectWait):
		return ErrConnectTimeout
	case <-ctx.Done():
		return fmt.Errorf("vkcalls: connect wait: %w", ctx.Err())
	}
}

// dumpNegotiation writes this exchange's three SDP documents to
// VKCALLS_DUMP_DIR when the operator set it: the spike's diagnostic capture,
// never on by default.
func dumpNegotiation(offer, native, shaped string) {
	dir := os.Getenv("VKCALLS_DUMP_DIR")
	if dir == "" {
		return
	}
	for name, body := range map[string]string{
		"engine-offer.sdp": offer, "engine-native.sdp": native, "engine-shaped.sdp": shaped,
	} {
		_ = os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600) //nolint:gosec // diagnostic artifact
	}
}

// acceptPayload builds the accept-producer command body: the shaped answer,
// the session the offer belonged to and the offer's SSRC attributions.
func acceptPayload(offer, shaped, sessionID string) map[string]any {
	ssrcs := remoteSSRCs(offer)
	ids := make([]any, 0, len(ssrcs))
	for _, ssrc := range ssrcs {
		ids = append(ids, ssrc)
	}
	return map[string]any{fieldDescription: shaped, "sessionId": sessionID, "ssrcs": ids}
}

// waitOffer returns the first producer-updated notification, or an empty one
// on timeout.
func waitOffer(ctx context.Context, signal *signalingClient) notification {
	timer := time.NewTimer(offerWait)
	defer timer.Stop()
	for {
		select {
		case note := <-signal.Notifications():
			if note.Notification == "producer-updated" && note.Description != "" {
				return note
			}
		case <-timer.C:
			return notification{}
		case <-signal.Closed():
			return notification{}
		case <-ctx.Done():
			return notification{}
		}
	}
}

// run owns the session after a successful connect: renegotiations and the
// signaling socket's lifetime.
func (s *Session) run(gen *generation, connection notification) {
	others := participantIDs(connection, gen.signal.Self())
	logger.Debugf("vkcalls: connected, %d other participants", len(others))
	for _, id := range others {
		gen.noteParticipant(id)
	}
	gen.subscribeAll()
	for {
		select {
		case note := <-gen.signal.Notifications():
			switch note.Notification {
			case "participant-joined", "media-settings-changed":
				// A camera may appear with either: ask for it by key.
				if id, err := decimalID(note.ParticipantID); err == nil && id != gen.signal.Self() && gen.noteParticipant(id) {
					gen.subscribeAll()
				}
			case "producer-updated":
				if note.Description == "" || note.SessionID == gen.sessionID {
					continue
				}
				gen.sessionID = note.SessionID
				if err := s.reanswer(gen, note.Description); err != nil {
					logger.Debugf("vkcalls: renegotiation: %v", err)
					gen.maybeReconnect()
				}
			case "hungup":
				if id, err := decimalID(note.ParticipantID); err == nil {
					gen.removeStream(id)
				}
			}
		case <-gen.signal.Closed():
			gen.maybeReconnect()
			s.end("signaling_closed")
			return
		}
	}
}

// reanswer handles a renegotiation offer on the established transport: same
// shaping and accept handshake, no second connect wait.
func (s *Session) reanswer(gen *generation, offer string) error {
	if gen.pc == nil {
		return ErrSessionClosed
	}
	offer = strings.ReplaceAll(offer, "\r\n", "\n")
	if err := gen.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: offer}); err != nil {
		return fmt.Errorf("vkcalls: renegotiation remote: %w", err)
	}
	native, err := gen.pc.CreateAnswer(nil)
	if err != nil {
		return fmt.Errorf("vkcalls: renegotiation answer: %w", err)
	}
	if localErr := gen.pc.SetLocalDescription(native); localErr != nil {
		return fmt.Errorf("vkcalls: renegotiation local: %w", localErr)
	}
	shaped, err := ShapeAnswer(offer, native.SDP, gen.shape)
	if err != nil {
		return err
	}
	dumpNegotiation(offer, native.SDP, shaped)
	_, err = gen.signal.command(context.Background(), "accept-producer",
		acceptPayload(offer, shaped, gen.sessionID))
	return err
}

// onServiceFrame folds producerNotification registry frames into the stream
// registry and re-subscribes to everything the SFU offers us.
func (s *Session) onServiceFrame(gen *generation, data []byte) {
	entries, err := ParseRegistry(data)
	if errors.Is(err, ErrNotRegistry) {
		return
	}
	if err != nil {
		logger.Debugf("vkcalls: registry frame: %v", err)
		return
	}
	if len(entries) == 0 {
		return
	}
	gen.registryMu.Lock()
	for key, id := range entries {
		gen.registry[key] = id
	}
	gen.registryMu.Unlock()
	gen.subscribeAll()
}

// removeStream drops a hung-up participant's stream from the subscription.
// Registry keys are typed with a participant-type prefix ("u" for USER)
// before the numeric id.
func (gen *generation) removeStream(participant string) {
	prefixes := []string{"u" + participant + ":", participant + ":"}
	gen.registryMu.Lock()
	delete(gen.participants, participant)
	delete(gen.registry, "u"+participant)
	for key := range gen.registry {
		for _, prefix := range prefixes {
			if len(key) > len(prefix) && key[:len(prefix)] == prefix {
				delete(gen.registry, key)
			}
		}
	}
	gen.registryMu.Unlock()
}

// subscribeAll sends the update-display-layout producer command for every
// registry stream. The SFU forwards exactly what the layout lists; without
// it nothing flows (spike hybrid-03…11).
func (gen *generation) subscribeAll() {
	gen.registryMu.Lock()
	entries := layoutEntries(gen.registry, gen.participantList())
	gen.layoutSeq++
	sequence := gen.layoutSeq % layoutSeqMod
	gen.registryMu.Unlock()
	if len(entries) == 0 {
		return
	}
	frame, err := EncodeLayout(sequence, entries)
	if err != nil {
		logger.Debugf("vkcalls: layout encode: %v", err)
		return
	}
	if gen.command == nil || gen.command.ReadyState() != webrtc.DataChannelStateOpen {
		return
	}
	if err := gen.command.Send(frame); err != nil {
		logger.Debugf("vkcalls: layout send: %v", err)
		return
	}
	logger.Debugf("vkcalls: layout %d sent for %d streams", sequence, len(entries))
}

// noteParticipant records another participant and says whether it is new.
func (gen *generation) noteParticipant(id string) bool {
	gen.registryMu.Lock()
	defer gen.registryMu.Unlock()
	if gen.participants[id] {
		return false
	}
	if gen.participants == nil {
		gen.participants = map[string]bool{}
	}
	gen.participants[id] = true
	return true
}

// participantList is the participants noted so far; the caller holds
// registryMu.
func (gen *generation) participantList() []string {
	out := make([]string, 0, len(gen.participants))
	for id := range gen.participants {
		out = append(out, id)
	}
	return out
}

// maybeReconnect triggers one reconnect when the policy allows it.
func (gen *generation) maybeReconnect() {
	if gen.reconnect == nil || (gen.shouldRecn != nil && !gen.shouldRecn()) {
		return
	}
	gen.fireOnce.Do(gen.reconnect)
}

func (gen *generation) teardown() {
	if gen.pc != nil {
		_ = gen.pc.Close()
	}
	if gen.signal != nil {
		gen.signal.close()
	}
}

// Reconnect tears the current generation down and connects again with fresh
// credentials when the provider supplies them.
func (s *Session) Reconnect(reason string) {
	logger.Debugf("vkcalls: reconnect: %s", reason)
	s.mu.Lock()
	gen := s.gen
	s.gen = nil
	closed := s.closed
	s.mu.Unlock()
	if gen != nil {
		gen.teardown()
	}
	if closed {
		return
	}
	if s.cfg.Refresh != nil {
		ctx, cancel := context.WithTimeout(context.Background(), connectWait)
		creds, err := s.cfg.Refresh(ctx)
		cancel()
		if err == nil {
			s.mu.Lock()
			s.endpoint = creds.URL
			s.peerID = creds.Extra["peer_id_hint"]
			s.iceJSON = creds.Extra["ice_servers"]
			s.mu.Unlock()
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), connectWait*2)
	defer cancel()
	if err := s.Connect(ctx); err != nil {
		logger.Debugf("vkcalls: reconnect failed: %v", err)
		s.end("reconnect_failed")
	}
}

// Send is unsupported: the SFU bridges only its known service channels, so
// payload rides the video transport.
func (s *Session) Send([]byte) error { return ErrSendUnsupported }

// CanSend reports whether the bundled peer connection is connected.
func (s *Session) CanSend() bool {
	s.mu.Lock()
	gen := s.gen
	s.mu.Unlock()
	if gen == nil || gen.pc == nil {
		return false
	}
	return gen.pc.ConnectionState() == webrtc.PeerConnectionStateConnected
}

// SubscriberCanSend mirrors CanSend: one bundled peer connection.
func (s *Session) SubscriberCanSend() bool { return s.CanSend() }

// GetBufferedAmount reports the producer command channel's buffered amount.
func (s *Session) GetBufferedAmount() uint64 {
	s.mu.Lock()
	gen := s.gen
	s.mu.Unlock()
	if gen == nil || gen.command == nil {
		return 0
	}
	return gen.command.BufferedAmount()
}

// SetReconnectCallback records the reconnect policy hooks for future
// generations.
func (s *Session) SetReconnectCallback(cb func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gen != nil {
		s.gen.reconnect = cb
	}
}

// SetShouldReconnect records the reconnect policy predicate.
func (s *Session) SetShouldReconnect(fn func() bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gen != nil {
		s.gen.shouldRecn = fn
	}
}

// SetEndedCallback registers the ended notification.
func (s *Session) SetEndedCallback(cb func(string)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ended = cb
}

// WatchConnection blocks until the session ends or ctx is cancelled.
func (s *Session) WatchConnection(ctx context.Context) {
	s.mu.Lock()
	gen := s.gen
	s.mu.Unlock()
	if gen == nil {
		return
	}
	select {
	case <-ctx.Done():
	case <-gen.signal.Closed():
		s.end("signaling_closed")
	}
}

func (s *Session) end(reason string) {
	s.endOnce.Do(func() {
		s.mu.Lock()
		cb := s.ended
		s.mu.Unlock()
		if cb != nil {
			cb(reason)
		}
	})
}

// Close leaves the conversation and frees the peer connection.
func (s *Session) Close() error {
	s.mu.Lock()
	gen := s.gen
	s.closed = true
	s.gen = nil
	s.mu.Unlock()
	if gen != nil {
		gen.teardown()
	}
	return nil
}

// iceServersOf decodes the provider's ice_servers JSON document; an empty or
// invalid document yields an empty list — the SFU's own candidates arrive in
// the offer.
func iceServersOf(raw string) []webrtc.ICEServer {
	type serverJSON struct {
		URLs       []string `json:"urls"`
		Username   string   `json:"username"`
		Credential string   `json:"credential"`
	}
	var servers []serverJSON
	if err := json.Unmarshal([]byte(raw), &servers); err != nil {
		return nil
	}
	out := make([]webrtc.ICEServer, 0, len(servers))
	for _, s := range servers {
		if len(s.URLs) == 0 {
			continue
		}
		ice := webrtc.ICEServer{URLs: s.URLs, Username: s.Username, Credential: s.Credential}
		out = append(out, ice)
	}
	return out
}
