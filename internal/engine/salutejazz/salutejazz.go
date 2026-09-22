// Package salutejazz implements an engine.Session over the SaluteJazz
// connector, the signaling endpoint of the Sber video service. The connector
// is LiveKit wrapped in a JSON envelope: the client sends
// {roomId, payload, event, groupId, requestId} and the payload of a media
// frame carries a LiveKit signal method (rtc:config, rtc:offer, rtc:ice,
// rtc:ping) instead of protobuf.
//
// The service needs no account. What a guest must know is a room code and
// its password, which the auth package turns into the connector URL
// (engine.Config.URL) and the password (engine.Config.Token).
//
// Two peer connections are required. The SFU offers a subscriber PC and
// creates the _reliable and _lossy channels on it, but data written to those
// channels is dropped without an error: only the channels of a publisher PC
// the client itself offers are relayed to the room. This engine therefore
// answers the SFU's offer, then offers a publisher PC carrying both
// channels, and sends on the publisher while it receives on the subscriber.
package salutejazz

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"

	"github.com/openlibrecommunity/olcrtc/internal/engine"
	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/internal/protect"
)

const (
	// The envelope events this engine speaks. The service has more
	// (chat, transcription); an engine that carries bytes needs none of them.
	eventJoin         = "join"
	eventJoinResponse = "join-response"
	eventMediaIn      = "media-in"
	eventMediaOut     = "media-out"
	eventError        = "error"

	// The LiveKit signal methods a media payload carries.
	methodConfig = "rtc:config"
	methodOffer  = "rtc:offer"
	methodAnswer = "rtc:answer"
	methodICE    = "rtc:ice"
	methodPing   = "rtc:ping"
	methodPong   = "rtc:pong"
	// methodParticipants carries the room roster: who else is here, and
	// under which identity a data packet reaches them.
	methodParticipants = "rtc:participants:update"

	// Which peer connection a description or a candidate belongs to. The SFU
	// offers the subscriber; the client offers the publisher.
	targetSubscriber = "SUBSCRIBER"
	targetPublisher  = "PUBLISHER"

	// The data channel labels LiveKit uses on both peer connections.
	labelReliable = "_reliable"
	labelLossy    = "_lossy"

	// The two description types a session description carries.
	sdpTypeOffer  = "offer"
	sdpTypeAnswer = "answer"

	// credentialKeyRoomID is the Extra key the auth provider puts the room
	// code under.
	credentialKeyRoomID = "roomID"

	// webOrigin and webUserAgent are what the official web client sends on
	// the connector handshake. The service sits behind a bot filter that
	// sees them.
	webOrigin    = "https://salutejazz.ru"
	webUserAgent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 " +
		"(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"

	wsHandshakeTimeout = 15 * time.Second
	wsWriteTimeout     = 15 * time.Second
	// wsReadTimeout is five ping intervals: the SFU answers every ping, so a
	// connector that says nothing for that long is gone.
	wsReadTimeout = 25 * time.Second
	// wsReadLimit caps one signaling frame. gorilla reads without a limit by
	// default, so the connector could otherwise name any size and have it
	// buffered and decoded in full.
	wsReadLimit = 8 << 20

	// pingInterval is the cadence the web client pings at, and pongTimeout
	// is what rtc:join announces as the server's own ping timeout.
	pingInterval = 5 * time.Second
	pongTimeout  = 15 * time.Second

	// answerTimeout bounds the wait for the SFU's answer to our publisher
	// offer. Measured at ~90 ms against the live service.
	answerTimeout = 20 * time.Second
	// joinTimeout bounds a whole join: socket, join-response, both peer
	// connections and the publisher channels. All media is relay-forced, so
	// this covers a full TURN allocation on a slow uplink.
	joinTimeout = 30 * time.Second
	// pcCloseTimeout bounds teardown of a peer connection. Closing one frees
	// its TURN allocation, and an unreachable relay makes pion retransmit
	// for tens of seconds; teardown must not wait that out.
	pcCloseTimeout = 2 * time.Second

	maxReconnects = 10
)

var (
	// ErrURLRequired is returned when no connector URL was supplied.
	ErrURLRequired = errors.New("salutejazz connector URL required")
	// ErrRoomIDRequired is returned when no room code was supplied.
	ErrRoomIDRequired = errors.New("salutejazz room code required")
	// ErrPasswordRequired is returned when no room password was supplied.
	ErrPasswordRequired = errors.New("salutejazz room password required")
	// ErrWebSocketClosed is returned when a signaling write is attempted
	// while no connector socket is connected.
	ErrWebSocketClosed = errors.New("salutejazz signaling websocket closed")
	// ErrSessionClosed is returned when the session is closed mid-operation.
	ErrSessionClosed = errors.New("salutejazz session closed")
	// ErrJoinTimeout is returned when a join does not complete in time.
	ErrJoinTimeout = errors.New("salutejazz join timeout")
	// ErrPongTimeout is returned when the connector leaves a ping unanswered.
	ErrPongTimeout = errors.New("salutejazz ping was not answered")
	// ErrNoAnswer is returned when the SFU leaves a publisher offer
	// unanswered.
	ErrNoAnswer = errors.New("salutejazz publisher offer was not answered")
	// ErrNoDescription is returned when a description frame carries no SDP.
	ErrNoDescription = errors.New("salutejazz description missing")
	// ErrNoDataChannel is returned when no data channel can carry a payload.
	ErrNoDataChannel = errors.New("salutejazz data channel not ready")
)

// generation is one connection attempt: the peer connections, their
// channels, the ICE state and the signals that drive the handshake. A
// reconnect replaces it wholesale, and every pion callback holds the
// generation it was created for, so a callback from a superseded attempt
// cannot signal the live one.
type generation struct {
	api *webrtc.API

	done     chan struct{}
	subReady chan struct{}
	pubReady chan struct{}
	answer   chan string

	// closeOnce makes teardown idempotent. Close and a Connect that is
	// giving up both hold this generation, and a signal channel closed by
	// two of them at once panics.
	closeOnce sync.Once

	// wsMu owns ws: it serialises the writes (gorilla allows a single
	// concurrent writer) and guards the pointer. The socket belongs to the
	// generation that dialled it, so a teardown can only close its own.
	wsMu sync.Mutex
	ws   *websocket.Conn

	// identity is this participant's id and group the LiveKit room name
	// behind the room code. Both come from this generation's join-response:
	// a rejoin has neither until the connector answers it again.
	identity atomic.Pointer[string]
	group    atomic.Pointer[string]

	subPC, pubPC     atomic.Pointer[webrtc.PeerConnection]
	subRel, subLossy atomic.Pointer[webrtc.DataChannel]
	pubRel, pubLossy atomic.Pointer[webrtc.DataChannel]

	subConnected atomic.Bool
	pubConnected atomic.Bool

	// iceMu owns the ICE state: the advertised servers, and the queue of
	// candidates that arrived before the peer connection they belong to had
	// a remote description.
	iceMu   sync.Mutex
	servers []webrtc.ICEServer
	pending map[string][]webrtc.ICECandidateInit

	// peersMu owns peers, the identity -> sid map rtc:participants:update
	// maintains for everyone else in the room.
	peersMu sync.Mutex
	peers   map[string]string

	// confirmed is the one remote identity the tunnel handshake has
	// authenticated, and it belongs to this connection attempt: the SFU
	// hands every connection fresh participant ids, so a rejoin starts
	// without a binding. See Session.ConfirmPeer.
	confirmed atomic.Pointer[string]

	// pcMu serialises publishing a peer connection against taking both of
	// them away, the same discipline wsMu gives the socket.
	pcMu sync.Mutex
}

func newGeneration(api *webrtc.API) *generation {
	return &generation{
		api:      api,
		done:     make(chan struct{}),
		subReady: make(chan struct{}),
		pubReady: make(chan struct{}),
		answer:   make(chan string, 1),
		pending:  make(map[string][]webrtc.ICECandidateInit),
		peers:    make(map[string]string),
	}
}

// publishPC hands a freshly built peer connection to the generation. A
// generation that has already been torn down owns nothing, so the peer
// connection is closed here instead of being left with an ICE agent and a
// TURN allocation and no owner. It reports whether the generation took it.
func (g *generation) publishPC(target string, pc *webrtc.PeerConnection) bool {
	g.pcMu.Lock()
	gone := g.isDone()
	if !gone {
		if target == targetPublisher {
			g.pubPC.Store(pc)
		} else {
			g.subPC.Store(pc)
		}
	}
	g.pcMu.Unlock()
	if gone {
		_ = pc.Close()
		return false
	}
	return true
}

// takePCs removes both peer connections from the generation, so exactly one
// caller ever closes them.
func (g *generation) takePCs() []*webrtc.PeerConnection {
	g.pcMu.Lock()
	defer g.pcMu.Unlock()
	return []*webrtc.PeerConnection{g.subPC.Swap(nil), g.pubPC.Swap(nil)}
}

// isDone reports whether this generation has been torn down.
func (g *generation) isDone() bool {
	select {
	case <-g.done:
		return true
	default:
		return false
	}
}

func (g *generation) groupID() string { return loadString(&g.group) }

func (g *generation) localIdentity() string { return loadString(&g.identity) }

// remoteIdentities is everyone else the room has reported, sorted so a
// caller sees a stable list.
func (g *generation) remoteIdentities() []string {
	g.peersMu.Lock()
	defer g.peersMu.Unlock()
	out := make([]string, 0, len(g.peers))
	for identity := range g.peers {
		out = append(out, identity)
	}
	slices.Sort(out)
	return out
}

// applyParticipants folds one rtc:participants:update into the identity map:
// a participant that has left is dropped, and this session is never its own
// peer.
func (g *generation) applyParticipants(list []participant) {
	local := g.localIdentity()
	g.peersMu.Lock()
	defer g.peersMu.Unlock()
	for _, peer := range list {
		if peer.Identity == "" || peer.Identity == local {
			continue
		}
		if peer.State == participantDisconnected {
			delete(g.peers, peer.Identity)
			continue
		}
		g.peers[peer.Identity] = peer.SID
	}
}

// Session is the SaluteJazz engine handle.
type Session struct {
	engine.Reconnector

	name     string
	resolver protect.Lookup
	refresh  func(ctx context.Context) (engine.Credentials, error)

	onData         func([]byte)
	onPeerData     func(peerID string, data []byte)
	onDatagram     func([]byte)
	onPeerDatagram func(peerID string, data []byte)

	// credMu guards the credentials a reconnect replaces under a live
	// session.
	credMu       sync.RWMutex
	connectorURL string
	room         string
	pass         string

	cur atomic.Pointer[generation]

	// pongMu owns pongWaiters, one per ping in flight.
	pongMu      sync.Mutex
	pongWaiters map[int64]chan struct{}
	rtt         atomic.Int64

	joinTimeout time.Duration

	// lifecycleMu is held across the two steps that decide who owns a
	// connection attempt: Connect reads the terminated flag and publishes
	// its attempt under it, Close raises the flag and takes the attempt
	// away under it. One of the two always sees the other.
	lifecycleMu sync.Mutex

	closeCh      chan struct{}
	closeOnce    sync.Once
	closed       atomic.Bool
	terminated   atomic.Bool
	reconnecting atomic.Bool

	goMu     sync.Mutex
	goClosed bool
	wg       sync.WaitGroup

	// onJoinPayload observes the frames that drive the join handshake:
	// join-response, rtc:config and rtc:offer, in that order. Tests set it
	// before Connect.
	onJoinPayload func(event string)

	// beforePublish runs in the instant before a connection attempt is
	// published on the session, which is where Close and Connect race for
	// ownership of it. Tests set it before Connect to land a Close in that
	// window every time instead of hoping for it.
	beforePublish func()
}

// New creates a SaluteJazz engine session.
//
// cfg.URL is the connector WebSocket URL, cfg.Token the room password and
// cfg.Extra["roomID"] the room code - the tuple the auth provider issues.
func New(_ context.Context, cfg engine.Config) (engine.Session, error) {
	if cfg.URL == "" {
		return nil, ErrURLRequired
	}
	if cfg.Token == "" {
		return nil, ErrPasswordRequired
	}
	room := cfg.Extra[credentialKeyRoomID]
	if room == "" {
		return nil, ErrRoomIDRequired
	}

	s := &Session{
		name:           cfg.Name,
		resolver:       cfg.Resolver,
		refresh:        cfg.Refresh,
		onData:         cfg.OnData,
		onPeerData:     cfg.OnPeerData,
		onDatagram:     cfg.OnDatagram,
		onPeerDatagram: cfg.OnPeerDatagram,
		connectorURL:   cfg.URL,
		room:           room,
		pass:           cfg.Token,
		pongWaiters:    make(map[int64]chan struct{}),
		joinTimeout:    joinTimeout,
		closeCh:        make(chan struct{}),
	}
	s.Configure(engine.ReconnectorConfig{
		MaxAttempts: maxReconnects,
		Reconnect:   s.reconnect,
		OnError: func(err error) {
			logger.Debugf("salutejazz: reconnect failed: %v", err)
		},
		OnLimit:     s.signalEnded,
		LimitReason: "reconnect limit reached",
	})
	return s, nil
}

// Connect joins the room and negotiates both peer connections. It returns
// once the publisher lane is open, which is when the session can carry
// bytes.
func (s *Session) Connect(ctx context.Context) error {
	api, err := newWebRTCAPI(s.resolver)
	if err != nil {
		return err
	}
	gen := newGeneration(api)
	if s.beforePublish != nil {
		s.beforePublish()
	}
	if !s.publishGeneration(gen) {
		return ErrSessionClosed
	}

	conn, err := s.dialWebSocket(gen)
	if err != nil {
		s.abandon(gen)
		return err
	}
	s.goLaunch(func() { s.readLoop(gen, conn) })
	s.goLaunch(func() { s.pingLoop(gen) })
	s.goLaunch(func() { s.negotiate(gen) })

	if err := s.sendJoin(gen); err != nil {
		s.abandon(gen)
		return err
	}
	if err := s.awaitLanes(ctx, gen); err != nil {
		s.abandon(gen)
		return err
	}
	return s.abortIfTerminated(gen)
}

// publishGeneration makes gen the session's live connection attempt, unless
// the session has been closed. It reports whether the session took it.
//
// Close raises the terminated flag and takes the live attempt away under the
// same lock, so the two cannot pass each other: either Close finds this
// attempt and ends it, or this publish is refused and the attempt is never
// dialled. Reading the flag and publishing as two steps left a Close that
// landed between them with nothing to end, and the attempt went on to dial
// and join behind a closed session - a participant the connector kept in the
// room until the join timed out.
func (s *Session) publishGeneration(gen *generation) bool {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.terminated.Load() {
		return false
	}
	s.closed.Store(false)
	s.cur.Store(gen)
	return true
}

// abandon ends a connection attempt Connect is giving up on and takes it off
// the session. Every accessor reads the live attempt through s.cur, so one
// that has been torn down must not be left answering for a session that has
// none. A later attempt that has already replaced it is left alone.
func (s *Session) abandon(gen *generation) {
	s.teardown(gen)
	s.cur.CompareAndSwap(gen, nil)
}

// awaitLanes waits for the publisher channel that carries the byte stream.
func (s *Session) awaitLanes(ctx context.Context, gen *generation) error {
	timer := time.NewTimer(s.joinTimeout)
	defer timer.Stop()
	select {
	case <-gen.pubReady:
		return nil
	case <-gen.done:
		return ErrSessionClosed
	case <-timer.C:
		return ErrJoinTimeout
	case <-ctx.Done():
		return fmt.Errorf("salutejazz connect cancelled: %w", ctx.Err())
	}
}

// abortIfTerminated tears down the generation Connect just built when Close
// ran while it was being built: Close cannot see resources that were not
// published yet.
func (s *Session) abortIfTerminated(gen *generation) error {
	if !s.terminated.Load() {
		return nil
	}
	s.abandon(gen)
	return ErrSessionClosed
}

// Close terminates the session and releases its resources. The connector
// has no leave frame: the SFU drops a participant on its own ping timeout.
func (s *Session) Close() error {
	gen := s.terminate()
	s.closeOnce.Do(func() { close(s.closeCh) })

	if gen != nil {
		s.teardown(gen)
	}
	s.stopLaunching()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(pcCloseTimeout):
	}
	return nil
}

// terminate marks the session gone for good and takes the live connection
// attempt away, under the lock Connect publishes an attempt with.
func (s *Session) terminate() *generation {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	s.terminated.Store(true)
	s.closed.Store(true)
	return s.cur.Swap(nil)
}

// teardown ends one generation: its goroutines stop, the socket it dialled
// is closed and both peer connections are released. It runs once however
// many callers hold the generation - Close and a Connect that is giving up
// both do.
func (s *Session) teardown(gen *generation) {
	gen.closeOnce.Do(func() {
		engine.CloseSignal(gen.done)
		gen.closeSocket()
		closePeerConnections(gen)
	})
}

// closePeerConnections closes both peer connections without letting a stuck
// TURN deallocation block the caller.
func closePeerConnections(gen *generation) {
	var wg sync.WaitGroup
	for _, pc := range gen.takePCs() {
		if pc == nil {
			continue
		}
		wg.Add(1)
		go func(pc *webrtc.PeerConnection) {
			defer wg.Done()
			_ = pc.Close()
		}(pc)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(pcCloseTimeout):
		logger.Warnf("salutejazz: peer connection close timed out after %s", pcCloseTimeout)
	}
}

// WatchConnection services reconnect requests until ctx ends or the session
// closes.
func (s *Session) WatchConnection(ctx context.Context) {
	s.Watch(ctx, s.closeCh)
}

// Reconnect asks the session to rejoin the room. Upper layers call it when a
// liveness probe declares the path dead before the transport has noticed.
func (s *Session) Reconnect(reason string) {
	if s.closed.Load() {
		return
	}
	logger.Infof("salutejazz: reconnect requested: %s", reason)
	s.queueReconnect()
}

// reconnect tears the current attempt down and joins again.
func (s *Session) reconnect(ctx context.Context) error {
	if s.terminated.Load() {
		return ErrSessionClosed
	}
	s.reconnecting.Store(true)
	defer s.reconnecting.Store(false)

	if gen := s.cur.Swap(nil); gen != nil {
		s.teardown(gen)
	}
	if err := s.Connect(ctx); err != nil {
		return err
	}
	s.NotifyReconnect()
	return nil
}

func (s *Session) queueReconnect() {
	s.Request(s.closed.Load(), s.reconnecting.Load())
}

func (s *Session) signalEnded(reason string) {
	s.closed.Store(true)
	s.SignalEnded(reason)
}

func (s *Session) current() *generation { return s.cur.Load() }

// localIdentity is this participant's id, as this generation's join-response
// named it.
func (s *Session) localIdentity() string {
	gen := s.current()
	if gen == nil {
		return ""
	}
	return gen.localIdentity()
}

// remoteIdentities is everyone else rtc:participants:update has reported in
// the room.
func (s *Session) remoteIdentities() []string {
	gen := s.current()
	if gen == nil {
		return nil
	}
	return gen.remoteIdentities()
}

func (s *Session) subscriberConnected() bool {
	gen := s.current()
	return gen != nil && gen.subConnected.Load()
}

func (s *Session) publisherConnected() bool {
	gen := s.current()
	return gen != nil && gen.pubConnected.Load()
}

// iceServers is what rtc:config advertised, normalised for pion.
func (s *Session) iceServers() []webrtc.ICEServer {
	gen := s.current()
	if gen == nil {
		return nil
	}
	return gen.iceServersSnapshot()
}

func (s *Session) roomID() string {
	s.credMu.RLock()
	defer s.credMu.RUnlock()
	return s.room
}

// password is the room password. It is never logged.
func (s *Session) password() string {
	s.credMu.RLock()
	defer s.credMu.RUnlock()
	return s.pass
}

func (s *Session) signalingURL() string {
	s.credMu.RLock()
	defer s.credMu.RUnlock()
	return s.connectorURL
}

func loadString(p *atomic.Pointer[string]) string {
	if v := p.Load(); v != nil {
		return *v
	}
	return ""
}

func storeString(p *atomic.Pointer[string], value string) {
	p.Store(&value)
}

// participantDisconnected is the state rtc:participants:update reports for
// someone who has left.
const participantDisconnected = "DISCONNECTED"

// goLaunch starts a tracked goroutine unless Close has started waiting.
func (s *Session) goLaunch(fn func()) {
	s.goMu.Lock()
	if s.goClosed {
		s.goMu.Unlock()
		return
	}
	s.wg.Add(1)
	s.goMu.Unlock()

	go func() {
		defer s.wg.Done()
		fn()
	}()
}

func (s *Session) stopLaunching() {
	s.goMu.Lock()
	s.goClosed = true
	s.goMu.Unlock()
}
