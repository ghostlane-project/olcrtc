package vkcalls

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	offerWait     = 30 * time.Second
	connectWait   = 30 * time.Second
	commandSeqMod = 1 << 20
	// perfStatInterval is the SDK's statisticsInterval: how often the client
	// reports decoded frames to the SFU.
	perfStatInterval = 5 * time.Second
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
		"red": true, "audioShare": true, "fastScreenShare": true, "videoSuspend": expOn("suspcap"),
		"simulcast": !expOn("nosim"), "simulcastNativeOrder": true, "consumerFastScreenShare": false,
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

	mu        sync.Mutex
	gen       *generation
	encodings []webrtc.RTPEncodingParameters // EXPERIMENT: the publish encodings
	closed    bool
	endOnce   sync.Once
	ended     func(string)
}

// generation bundles everything one connection attempt owns, so closing it
// tears the whole attempt down (spec 7.3: state never crosses generations).
type generation struct {
	signal  *signalingClient
	pc      *webrtc.PeerConnection
	command *webrtc.DataChannel
	notify  *webrtc.DataChannel
	extraDC []*webrtc.DataChannel // EXPERIMENT: the SDK's other service channels
	shape   *ShapeState

	registryMu sync.Mutex
	registry   Registry
	// participants are the others in the conversation, whose cameras the
	// layout asks for (by string key until the registry names them).
	participants map[string]bool
	commandSeq   int
	sessionID    string
	connected    chan struct{}
	done         chan struct{}
	doneOnce     sync.Once
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
		done:      make(chan struct{}),
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
		"mediaSettings": map[string]any{"isAudioEnabled": expOn("audioon"), "isVideoEnabled": expOn("vidon"),
			"isScreenSharingEnabled": false, "isFastScreenSharingEnabled": false,
			"isAudioSharingEnabled": false, "isAnimojiEnabled": !expOn("animoff")},
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
	go gen.perfStatLoop()
	go s.run(gen, connection) //nolint:contextcheck // the session owns its post-connect lifetime
	return nil
}

// negotiate answers one producer offer: the bundled peer connection mirrors
// the offer's codecs, the shaped answer passes the transport gate and
// accept-producer completes the session.
func (s *Session) negotiate(ctx context.Context, gen *generation, offer string) error { //nolint:gocyclo,cyclop
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
	// The service channels are created before the tracks, in the SDK's
	// order (channel ids 0,2,4,… before the media sections' SSRCs).
	if err := s.openControlChannels(gen, pc); err != nil {
		return err
	}
	senders := map[string]*webrtc.RTPSender{}
	s.video.RangeVideoTracks(func(track webrtc.TrackLocal, _ bool) {
		// EXPERIMENT: a track with a RID joins the sender of its track id as
		// one more simulcast encoding.
		if base, ok := senders[track.ID()]; ok && track.RID() != "" {
			if err := base.AddEncoding(track); err != nil {
				logger.Debugf("vkcalls: add encoding %s: %v", track.RID(), err)
			}
			return
		}
		sender, err := pc.AddTrack(track)
		if err != nil {
			logger.Debugf("vkcalls: add track: %v", err)
			return
		}
		senders[track.ID()] = sender
		go drainRTCP(sender)
	})
	s.mu.Lock()
	s.encodings = nil
	s.mu.Unlock()
	for _, sender := range senders {
		for _, enc := range sender.GetParameters().Encodings {
			if enc.RID != "" {
				ssrcRID.Store(uint32(enc.SSRC), enc.RID)
				logger.Debugf("vkcalls: encoding rid=%s ssrc=%d", enc.RID, enc.SSRC)
			}
			s.mu.Lock()
			s.encodings = append(s.encodings, enc)
			s.mu.Unlock()
		}
	}
	pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		logger.Debugf("vkcalls: remote track kind=%s codec=%s ssrc=%d", track.Kind(), track.Codec().MimeType, track.SSRC())
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

	if expOn("sixdc") {
		ordered := true
		if dc, err := pc.CreateDataChannel("consumerScreenShare", &webrtc.DataChannelInit{Ordered: &ordered}); err == nil {
			dc.OnOpen(func() { logger.Debugf("vkcalls: service channel consumerScreenShare open") })
			gen.extraDC = append(gen.extraDC, dc)
		}
	}
	return s.acceptOffer(ctx, gen, offer)
}

// openControlChannels creates the two service data channels the engine
// speaks: producerCommand carries the layout subscription, and
// producerNotification delivers the stream registry.
func (s *Session) openControlChannels(gen *generation, pc *webrtc.PeerConnection) error {
	ordered := true
	notify, err := pc.CreateDataChannel("producerNotification", &webrtc.DataChannelInit{Ordered: &ordered})
	if err != nil {
		return fmt.Errorf("vkcalls: producer notification channel: %w", err)
	}
	gen.notify = notify
	command, err := pc.CreateDataChannel("producerCommand", &webrtc.DataChannelInit{Ordered: &ordered})
	if err != nil {
		return fmt.Errorf("vkcalls: producer command channel: %w", err)
	}
	gen.command = command
	if expOn("sixdc") {
		// EXPERIMENT: the SDK's full channel set in its order; the consumer
		// screen share channel follows the tracks (see negotiate).
		for _, label := range []string{"producerScreenShare", "asr", "animoji"} {
			dc, err := pc.CreateDataChannel(label, &webrtc.DataChannelInit{Ordered: &ordered})
			if err != nil {
				return fmt.Errorf("vkcalls: %s channel: %w", label, err)
			}
			l := label
			dc.OnOpen(func() { logger.Debugf("vkcalls: service channel %s open", l) })
			dc.OnMessage(func(m webrtc.DataChannelMessage) { logger.Debugf("vkcalls: %s message %x", l, m.Data) })
			gen.extraDC = append(gen.extraDC, dc)
		}
	}
	// The participants known at connect, and a registry that arrived first,
	// are subscribed as soon as the channel can carry the layout.
	command.OnOpen(func() {
		if expOn("vsusp") {
			// EXPERIMENT: the web app's initVideoSuspend on CONNECTED in
			// SERVER topology: enable-video-suspend(true) (AUTO mode).
			gen.registryMu.Lock()
			gen.commandSeq++
			seq := gen.commandSeq
			gen.registryMu.Unlock()
			frame := append(appendMPInt(appendMPInt(appendMPInt(nil, 5), 0), seq), 0xC3)
			logger.Debugf("vkcalls: enable-video-suspend %x: %v", frame, command.Send(frame))
		}
		// The SDK configures its encodings right after accept-producer
		// (sync({force:true})): without this the SFU accepts the stream but
		// never forwards it, and the command is fire-and-forget, so the
		// mistake is silent. The engine publishes one layer: l at the
		// transport's small frame. Bitrate is written in kbit/s.
		{
			gen.registryMu.Lock()
			gen.commandSeq++
			seq := gen.commandSeq
			gen.registryMu.Unlock()
			w, h, fps, kbps := layerProfile()
			frame := encodeChangeSimulcast(seq, "l", w, h, fps, kbps*1000)
			if expOn("chsim3") {
				// Chrome's rs(1280,720): three layers announced, h inactive.
				layers := [][5]any{{"l", 320, 180, 20, 180000}, {"m", 640, 360, 20, 500000},
					{"h", 1280, 720, 20, 1200000}}
				frame = encodeChangeSimulcastLayers(seq, layers)
			}
			logger.Debugf("vkcalls: change-simulcast %x: %v", frame, command.Send(frame))
		}
		gen.subscribeAll()
	})
	command.OnMessage(func(message webrtc.DataChannelMessage) {
		logger.Debugf("vkcalls: command reply %x", message.Data)
	})

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
	logger.Debugf("vkcalls: connected as <%s>, %d other participants", idTag(gen.signal.Self()), len(others))
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
	if len(data) > 0 && data[0] != registryKind && data[0] != 6 && len(data) <= 48 {
		logger.Debugf("vkcalls: service frame kind=%d hex %x", data[0], data)
	}
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
		logger.Debugf("vkcalls: registry %s -> %d", streamKind(key), id)
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

// nextSeq takes the next producerCommand sequence number; every command on
// that channel shares one counter, as in the SDK.
func (gen *generation) nextSeq() int {
	gen.registryMu.Lock()
	defer gen.registryMu.Unlock()
	gen.commandSeq++
	return gen.commandSeq % commandSeqMod
}

// perfStatLoop reports the consumer's decoded frames every five seconds, the
// SDK's statisticsInterval: the SFU's consumer-leg liveness depends on these
// reports (spike tun-rr-01: without them the forward stalls within a minute).
func (gen *generation) perfStatLoop() {
	ticker := time.NewTicker(perfStatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-gen.done:
			return
		case <-ticker.C:
		}
		if gen.pc == nil || gen.command == nil || gen.command.ReadyState() != webrtc.DataChannelStateOpen {
			continue
		}
		// Pion never populates FramesDecoded (nothing in the stack decodes),
		// so the honest progression signal is received video packets — the
		// report exists to prove the consumer consumes, and packet counts do.
		var received uint32
		for _, entry := range gen.pc.GetStats() {
			inbound, ok := entry.(webrtc.InboundRTPStreamStats)
			if !ok || inbound.Kind != kindVideo {
				continue
			}
			received += inbound.PacketsReceived
		}
		frame := encodePerfStatReport(gen.nextSeq(), received, received)
		if err := gen.command.Send(frame); err != nil {
			logger.Debugf("vkcalls: perf stat: %v", err)
		}
	}
}

// subscribeAll sends the update-display-layout producer command for every
// registry stream. The SFU forwards exactly what the layout lists; without
// it nothing flows (spike hybrid-03…11).
func (gen *generation) subscribeAll() {
	gen.registryMu.Lock()
	entries := layoutEntries(gen.registry, gen.participantList())
	gen.commandSeq++
	sequence := gen.commandSeq % commandSeqMod
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

func (gen *generation) stop() {
	if gen.done == nil {
		return
	}
	gen.doneOnce.Do(func() { close(gen.done) })
}

func (gen *generation) teardown() {
	gen.stop()
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

// Encodings (EXPERIMENT) lists the publish sender's encodings.
func (s *Session) Encodings() []webrtc.RTPEncodingParameters {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]webrtc.RTPEncodingParameters(nil), s.encodings...)
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

// drainRTCP reads a sender's RTCP until the sender closes, so the RTCP
// reaches the interceptors.
func drainRTCP(sender *webrtc.RTPSender) {
	buf := make([]byte, 1500)
	for {
		if _, _, err := sender.Read(buf); err != nil {
			return
		}
	}
}

// streamKind is a registry key with its participant id replaced by a tag.
func streamKind(key string) string {
	head, suffix, _ := strings.Cut(key, ":")
	prefix := strings.TrimRight(head, "0123456789")
	tag := idTag(strings.TrimPrefix(head, prefix))
	if suffix == "" {
		return prefix + "<" + tag + ">"
	}
	return prefix + "<" + tag + ">:" + suffix
}

// idTag tells participant ids apart in a debug log without printing them.
func idTag(id string) string {
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:2])
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
