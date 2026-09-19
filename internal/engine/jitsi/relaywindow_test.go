package jitsi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtaci/smux"
	"github.com/zarazaex69/j"

	"github.com/openlibrecommunity/olcrtc/internal/engine"
)

// ai-generated: the whole file (the relay window's tests, olcrtc#15).

const (
	// relayModelBuffer is what the JVB holds for one receiver before it drops:
	// dcsctp's default send buffer, which a live receiver that stopped reading
	// for 8 s matched - 224 consecutive frames lost once ~2 MB were queued.
	relayModelBuffer = 2_000_000
	// relayTestFrame is a full datachannel frame, a smux frame of 12 KiB.
	relayTestFrame = 12 * 1024
	// relayTestLoad is what six phone downloads may have in flight: six smux
	// windows of 512 KiB.
	relayTestLoad = 6 * 512 * 1024 / relayTestFrame
)

// relayModel stands in for the JVB between sessions, as it was measured: it
// takes whatever a sender hands it at once (the JVB never closes its receive
// window), holds for each receiver up to relayModelBuffer bytes of
// EndpointMessage JSON, drops what does not fit, and delivers to a receiver,
// in order, only while that receiver's leg is up.
type relayModel struct {
	t       *testing.T
	mu      sync.Mutex
	ends    map[string]*relayEnd
	dropped atomic.Int64
	// dropWindow, when it returns true, loses a window frame on its way:
	// an old peer that never echoes, or an echo lost in transit.
	dropWindow func(from string, frame []byte) bool
}

type relayEnd struct {
	id      string
	sess    *Session
	queue   []relayMsg
	queued  int
	stalled bool
	wake    chan struct{}
}

type relayMsg struct {
	from  string
	frame []byte
	size  int
}

func newRelayModel(t *testing.T) *relayModel {
	t.Helper()
	return &relayModel{t: t, ends: make(map[string]*relayEnd)}
}

// join opens a session on the relay under endpoint id with the given epoch.
// A session with onPeer set runs in peer mode, as a server does; timing, when
// given, shortens its window's timers.
func (m *relayModel) join(
	id string, epoch uint32, onData func([]byte), onPeer func(string, []byte), timing ...relayTiming,
) *Session {
	m.t.Helper()
	cfg := engine.Config{URL: testHost, Extra: map[string]string{credentialKeyRoom: testRoom}, OnPeerData: onPeer}
	if onPeer == nil {
		cfg.OnData = onData
	}
	sess, err := New(context.Background(), cfg)
	if err != nil {
		m.t.Fatalf("New: %v", err)
	}
	s, ok := sess.(*Session)
	if !ok {
		m.t.Fatalf("sess type = %T", sess)
	}
	s.localEpoch.Store(epoch)
	s.bridgeReady.Store(true)
	s.backlogGauge = func() int { return 0 } // the sender's own leg is fast
	s.sendHook = func(to string, frame []byte) error { return m.send(id, to, frame) }
	for _, t := range timing {
		s.relayTiming = t
	}
	end := &relayEnd{id: id, sess: s, wake: make(chan struct{}, 1)}
	m.mu.Lock()
	m.ends[id] = end
	m.mu.Unlock()
	s.goLaunch(s.sendLoop)
	go m.deliver(end)
	m.t.Cleanup(func() { _ = s.Close() })
	return s
}

func (m *relayModel) send(from, to string, frame []byte) error {
	if m.dropWindow != nil && isWindowFrame(frame) && m.dropWindow(from, frame) {
		return nil
	}
	msg, err := json.Marshal(endpointMessage{
		ColibriClass: colibriClassEndpointMessage, To: to,
		MsgPayload: endpointRawPayload{Raw: base64.StdEncoding.EncodeToString(frame)},
	})
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, end := range m.ends {
		if id == from || (to != "" && to != id) {
			continue
		}
		if end.queued+len(msg) > relayModelBuffer {
			m.dropped.Add(1)
			continue
		}
		end.queue = append(end.queue, relayMsg{from: from, frame: append([]byte(nil), frame...), size: len(msg)})
		end.queued += len(msg)
		select {
		case end.wake <- struct{}{}:
		default:
		}
	}
	return nil
}

// deliver is the receiver's leg: one message at a time, in order, through
// the session's receive path, while the leg is up.
func (m *relayModel) deliver(end *relayEnd) {
	for {
		m.mu.Lock()
		var next *relayMsg
		if !end.stalled && len(end.queue) > 0 {
			next = &end.queue[0]
			end.queue = end.queue[1:]
			end.queued -= next.size
		}
		m.mu.Unlock()
		if next == nil {
			select {
			case <-end.sess.done:
				return
			case <-end.wake:
			case <-time.After(time.Millisecond):
			}
			continue
		}
		end.sess.deliverBridgeMessage(j.BridgeMessage{
			Class: colibriClassEndpointMessage,
			From:  next.from,
			Fields: map[string]any{"msgPayload": map[string]any{
				"raw": base64.StdEncoding.EncodeToString(next.frame),
			}},
		}, true)
	}
}

func (m *relayModel) stall(id string, stalled bool) {
	m.mu.Lock()
	m.ends[id].stalled = stalled
	m.mu.Unlock()
}

func isWindowFrame(frame []byte) bool {
	return len(frame) >= len(bridgeWindowMagic) && bytes.Equal(frame[:len(bridgeWindowMagic)], bridgeWindowMagic[:])
}

// seqLog records the sequence numbers a receiver was handed, in order.
type seqLog struct {
	mu  sync.Mutex
	got []uint64
}

func (l *seqLog) onData(data []byte) {
	if len(data) < 8 {
		return
	}
	l.mu.Lock()
	l.got = append(l.got, binary.BigEndian.Uint64(data))
	l.mu.Unlock()
}

func (l *seqLog) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.got)
}

// inOrder reports the first place the log departs from from, from+1, ...
func (l *seqLog) inOrder(from uint64, n int) (int, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.got) != n {
		return len(l.got), false
	}
	for i, seq := range l.got {
		if seq != from+uint64(i) {
			return i, false
		}
	}
	return n, true
}

func relayFrame(seq uint64) []byte {
	frame := make([]byte, relayTestFrame)
	binary.BigEndian.PutUint64(frame, seq)
	return frame
}

// sendFrames sends frames numbered from..to-1 through send, in order.
func sendFrames(t *testing.T, send func([]byte) error, from, to uint64) {
	t.Helper()
	for seq := from; seq < to; seq++ {
		if err := send(relayFrame(seq)); err != nil {
			t.Errorf("send %d: %v", seq, err)
			return
		}
	}
}

func waitUntil(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// A downlink that stalls under six parallel downloads (olcrtc#15): the
// relay keeps taking the sender's frames and has to drop what it cannot
// hold for the receiver, and every stream the lost frames belonged to ends
// short. With the window the sender stops at what the relay can hold and
// nothing is lost.
func TestAStalledReceiverLosesNothingAtTheRelay(t *testing.T) {
	m := newRelayModel(t)
	var got seqLog
	b := m.join("endpoint-b", 0xB0B0B0B0, got.onData, nil)
	a := m.join("endpoint-a", 0xA0A0A0A0, func([]byte) {}, nil)
	a.peerEpoch.Store(b.localEpoch.Load())

	// The handshake a tunnel does before any load, over a leg that is up.
	sendFrames(t, a.Send, 0, 1)
	waitUntil(t, 2*time.Second, "the first frame never arrived", func() bool { return got.count() == 1 })

	m.stall("endpoint-b", true)
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		sendFrames(t, a.Send, 1, relayTestLoad+1)
	}()
	time.Sleep(200 * time.Millisecond)
	m.stall("endpoint-b", false)

	waitUntil(t, 10*time.Second, "the sender never finished", func() bool {
		select {
		case <-sent:
			return true
		default:
			return false
		}
	})
	waitUntil(t, 10*time.Second, "the receiver never caught up", func() bool {
		return got.count()+int(m.dropped.Load()) > relayTestLoad
	})
	if n := m.dropped.Load(); n != 0 {
		t.Fatalf("the relay dropped %d of %d frames: the sender handed it more than it holds for a stalled receiver",
			n, relayTestLoad+1)
	}
	if at, ok := got.inOrder(0, relayTestLoad+1); !ok {
		t.Fatalf("the receiver's frames depart from the order sent at %d", at)
	}
}

// joinPair opens a receiver b and a sender a that has confirmed b, as a
// client has confirmed its server, and passes one frame over the open leg
// first, the handshake a tunnel does before any load.
func joinPair(t *testing.T, m *relayModel, got *seqLog, timing ...relayTiming) (*Session, *Session) {
	t.Helper()
	b := m.join("endpoint-b", 0xB0B0B0B0, got.onData, nil)
	a := m.join("endpoint-a", 0xA0A0A0A0, func([]byte) {}, nil, timing...)
	a.peerEpoch.Store(b.localEpoch.Load())
	sendFrames(t, a.Send, 0, 1)
	waitUntil(t, 2*time.Second, "the first frame never arrived", func() bool { return got.count() == 1 })
	return a, b
}

func closedWithin(t *testing.T, ch <-chan struct{}, within time.Duration, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(within):
		t.Fatal(what)
	}
}

// An older build drops marks unread and never echoes, so the window never
// turns on and the sender is not held for it: a stalled receiver of the old
// kind loses frames at the relay as it always did, but nothing wedges.
func TestAPeerThatNeverEchoesIsSentToAsBefore(t *testing.T) {
	m := newRelayModel(t)
	m.dropWindow = func(string, []byte) bool { return true }
	var got seqLog
	a, _ := joinPair(t, m, &got)
	m.stall("endpoint-b", true)
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		sendFrames(t, a.Send, 1, relayTestLoad+1)
	}()
	closedWithin(t, sent, 5*time.Second, "the sender waited for an echo an older peer never sends")
}

// A lost echo would hold the sender for good; the probe a held sender sends
// once a probe interval brings a fresh echo, and the load goes on.
func TestALostEchoIsRepairedByAProbe(t *testing.T) {
	m := newRelayModel(t)
	var lose atomic.Bool
	m.dropWindow = func(from string, _ []byte) bool { return lose.Load() && from == "endpoint-b" }
	var got seqLog
	a, _ := joinPair(t, m, &got, relayTiming{probe: 20 * time.Millisecond, dead: time.Minute})

	lose.Store(true)
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		sendFrames(t, a.Send, 1, relayTestLoad+1)
	}()
	time.Sleep(150 * time.Millisecond)
	if n := got.count(); n > relayWindow/relayTestFrame+2 {
		t.Fatalf("%d frames arrived with every echo lost; the window lets %d through", n, relayWindow/relayTestFrame)
	}
	lose.Store(false)
	closedWithin(t, sent, 5*time.Second, "the sender stayed held after echoes came back")
	waitUntil(t, 5*time.Second, "the receiver never caught up", func() bool { return got.count() == relayTestLoad+1 })
	if n := m.dropped.Load(); n != 0 {
		t.Fatalf("the relay dropped %d frames", n)
	}
}

// A destination that stops answering for good is gone; after relayDeadAfter
// its window is turned off, so what waits for it drains as it used to rather
// than hold its queue forever.
func TestAPeerThatStopsEchoingIsLetGo(t *testing.T) {
	m := newRelayModel(t)
	var got seqLog
	a, _ := joinPair(t, m, &got, relayTiming{probe: 20 * time.Millisecond, dead: 100 * time.Millisecond})
	m.stall("endpoint-b", true)
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		sendFrames(t, a.Send, 1, relayTestLoad+1)
	}()
	closedWithin(t, sent, 5*time.Second, "the sender stayed held by a peer that never answers again")
}

// A server holds one window per client: a client whose leg stalls holds
// its own frames back and nobody else's.
func TestAStalledPeerDoesNotHoldAnother(t *testing.T) {
	m := newRelayModel(t)
	var got1, got2 seqLog
	srv := m.join("endpoint-srv", 0xA0A0A0A0, nil, func(string, []byte) {})
	b1 := m.join("endpoint-b1", 0xB1B1B1B1, got1.onData, nil)
	b2 := m.join("endpoint-b2", 0xB2B2B2B2, got2.onData, nil)
	for _, b := range []*Session{b1, b2} {
		b.peerEpoch.Store(srv.localEpoch.Load())
		if err := b.Send([]byte("hello")); err != nil {
			t.Fatal(err)
		}
	}
	waitUntil(t, 2*time.Second, "the server never learned its clients", func() bool {
		return srv.echoerEpoch("endpoint-b1") != 0 && srv.echoerEpoch("endpoint-b2") != 0
	})
	to := func(peer string) func([]byte) error {
		return func(frame []byte) error { return srv.SendTo(peer, frame) }
	}
	sendFrames(t, to("endpoint-b1"), 0, 1)
	sendFrames(t, to("endpoint-b2"), 0, 1)
	waitUntil(t, 2*time.Second, "the first frames never arrived", func() bool {
		return got1.count() == 1 && got2.count() == 1
	})

	m.stall("endpoint-b1", true)
	go sendFrames(t, to("endpoint-b1"), 1, relayTestLoad+1)
	go sendFrames(t, to("endpoint-b2"), 1, relayTestLoad+1)
	waitUntil(t, 10*time.Second, "the stalled client held the other one", func() bool {
		return got2.count() == relayTestLoad+1
	})
	m.stall("endpoint-b1", false)
	waitUntil(t, 10*time.Second, "the stalled client never caught up", func() bool {
		return got1.count()+int(m.dropped.Load()) > relayTestLoad
	})
	if n := m.dropped.Load(); n != 0 {
		t.Fatalf("the relay dropped %d frames", n)
	}
	for i, got := range []*seqLog{&got1, &got2} {
		if at, ok := got.inOrder(0, relayTestLoad+1); !ok {
			t.Fatalf("client %d: frames depart from the order sent at %d", i+1, at)
		}
	}
}

// relayFrames records what a session puts on the bridge.
type relayFrames struct {
	mu     sync.Mutex
	frames []windowFrame
	to     []string
}

func (r *relayFrames) hook(to string, frame []byte) error {
	f, ok := parseWindowFrame(frame)
	if !ok {
		return nil
	}
	r.mu.Lock()
	r.frames = append(r.frames, f)
	r.to = append(r.to, to)
	r.mu.Unlock()
	return nil
}

func windowMessage(t *testing.T, from string, f windowFrame) j.BridgeMessage {
	t.Helper()
	return makeBridgeMessageFrom(from, map[string]any{
		"msgPayload": map[string]any{"raw": encodeForTest(t, f.encode())},
	})
}

// Marks and echoes are the window's alone: none reaches onData, a mark is
// echoed only when addressed to this session's epoch, and an echo moves a
// window only from the peer it is kept for and never past what was sent.
func TestWindowFramesStayBetweenTheTwoEnds(t *testing.T) {
	var handed atomic.Int64
	js := newSilentSession(t)
	js.onData = func([]byte) { handed.Add(1) }
	var out relayFrames
	js.sendHook = out.hook
	js.bridgeReady.Store(true)
	js.localEpoch.Store(0xA0A0A0A0)
	js.peerEpoch.Store(0xB0B0B0B0)

	mark := windowFrame{kind: windowMark, senderEpoch: 0xB0B0B0B0, receiverEpoch: 0xA0A0A0A0, counter: 4096}
	js.deliverBridgeMessage(windowMessage(t, "peer", mark), true)
	elsewhere := mark
	elsewhere.receiverEpoch = 0xC0C0C0C0
	js.deliverBridgeMessage(windowMessage(t, "peer", elsewhere), true)
	if handed.Load() != 0 {
		t.Fatal("a window frame was handed up as data")
	}
	out.mu.Lock()
	echoes, to := out.frames, out.to
	out.mu.Unlock()
	want := windowFrame{kind: windowEcho, senderEpoch: 0xA0A0A0A0, receiverEpoch: 0xB0B0B0B0, counter: 4096}
	if len(echoes) != 1 || echoes[0] != want || to[0] != "peer" {
		t.Fatalf("echoes %+v to %q, want one %+v to the mark's sender", echoes, to, want)
	}

	js.relaySent("", 2*relayWindow)
	echo := windowFrame{kind: windowEcho, senderEpoch: 0xC0C0C0C0, receiverEpoch: 0xA0A0A0A0, counter: 1}
	js.deliverBridgeMessage(windowMessage(t, "stranger", echo), true)
	echo.senderEpoch, echo.counter = 0xB0B0B0B0, 2*relayWindow+1
	js.deliverBridgeMessage(windowMessage(t, "peer", echo), true)
	if ok, _, _ := js.relayRoom("", time.Now()); !ok {
		t.Fatal("an echo from a stranger or past what was sent turned the window on")
	}
	echo.counter = 1
	js.deliverBridgeMessage(windowMessage(t, "peer", echo), true)
	if ok, _, _ := js.relayRoom("", time.Now()); ok {
		t.Fatal("the peer's echo did not turn the window on")
	}
}

// Confirming another peer starts the window over: the new one may be an
// older build that never echoes. Confirming the same peer again keeps it.
func TestConfirmingAnotherPeerStartsTheWindowOver(t *testing.T) {
	js := newSilentSession(t)
	var out relayFrames
	js.sendHook = out.hook
	js.localEpoch.Store(0xA0A0A0A0)
	if err := js.ConfirmPeer("b0b0b0b0"); err != nil {
		t.Fatal(err)
	}
	js.relaySent("", 2*relayWindow)
	echo := windowFrame{kind: windowEcho, senderEpoch: 0xB0B0B0B0, receiverEpoch: 0xA0A0A0A0, counter: 1}
	js.deliverBridgeMessage(windowMessage(t, "peer", echo), true)
	if err := js.ConfirmPeer("b0b0b0b0"); err != nil {
		t.Fatal(err)
	}
	if ok, _, _ := js.relayRoom("", time.Now()); ok {
		t.Fatal("confirming the same peer again turned the window off")
	}
	if err := js.ConfirmPeer("c0c0c0c0"); err != nil {
		t.Fatal(err)
	}
	if ok, _, _ := js.relayRoom("", time.Now()); !ok {
		t.Fatal("a window kept for the old peer holds the sender to a new one")
	}
}

// sessionConn carries a byte stream over a session the way muxconn carries
// smux: a Write is one frame, a frame received is one read's worth.
type sessionConn struct {
	send func([]byte) error
	in   chan []byte
	buf  []byte
	done chan struct{}
	once sync.Once
}

func newSessionConn() *sessionConn {
	return &sessionConn{in: make(chan []byte, 128), done: make(chan struct{})}
}

func (c *sessionConn) push(frame []byte) {
	select {
	case c.in <- append([]byte(nil), frame...):
	case <-c.done:
	}
}

func (c *sessionConn) Write(p []byte) (int, error) {
	if err := c.send(append([]byte(nil), p...)); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *sessionConn) Read(p []byte) (int, error) {
	if len(c.buf) == 0 {
		select {
		case c.buf = <-c.in:
		case <-c.done:
			return 0, io.EOF
		}
	}
	n := copy(p, c.buf)
	c.buf = c.buf[n:]
	return n, nil
}

func (c *sessionConn) Close() error {
	c.once.Do(func() { close(c.done) })
	return nil
}

const relayTestPull = 2 << 20 // one pull's body

// pullByte is byte i of pull id's body, so a hole or a swap shows.
func pullByte(id byte, i int) byte { return byte(i*7+int(id)) ^ byte(i>>13) } //nolint:gosec // a test pattern

var errShortPull = errors.New("pull ended short")

// readPull reads one pull and checks every byte of it.
func readPull(st *smux.Stream) error {
	var id [1]byte
	if _, err := io.ReadFull(st, id[:]); err != nil {
		return fmt.Errorf("pull id: %w", err)
	}
	buf := make([]byte, 32<<10)
	got := 0
	for {
		n, err := st.Read(buf)
		for k := range n {
			if buf[k] != pullByte(id[0], got+k) {
				return fmt.Errorf("pull %d: byte %d is not what was sent", id[0], got+k)
			}
		}
		got += n
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("pull %d after %d bytes: %w", id[0], got, err)
		}
	}
	if got != relayTestPull {
		return fmt.Errorf("pull %d: %d of %d bytes: %w", id[0], got, relayTestPull, errShortPull)
	}
	return nil
}

// servePulls answers every stream with a pull: its id, then its body.
func servePulls(server *smux.Session) {
	var next atomic.Int32
	for {
		st, err := server.AcceptStream()
		if err != nil {
			return
		}
		go func() {
			defer func() { _ = st.Close() }()
			id := byte(next.Add(1)) //nolint:gosec // six pulls, an id never wraps
			body := make([]byte, relayTestPull)
			for i := range body {
				body[i] = pullByte(id, i)
			}
			if _, err := st.Write(append([]byte{id}, body...)); err != nil {
				return
			}
		}()
	}
}

// Six pulls at once over smux, as the gate's S2 runs them (olcrtc#15), with
// the client's downlink stalling as they start: a phone's smux windows let
// the server put 3 MiB in flight, twice what the relay holds for the client.
// Without the window every pull lost frames at the relay and ended short or
// corrupt; with it all six arrive whole.
func TestSixPullsThroughAStallingRelayArriveWhole(t *testing.T) {
	m := newRelayModel(t)
	cliConn, srvConn := newSessionConn(), newSessionConn()
	cli := m.join("endpoint-cli", 0xC0C0C0C0, cliConn.push, nil)
	srv := m.join("endpoint-srv", 0x50505050, srvConn.push, nil)
	cli.peerEpoch.Store(srv.localEpoch.Load())
	srv.peerEpoch.Store(cli.localEpoch.Load())
	cliConn.send, srvConn.send = cli.Send, srv.Send

	cfg := smux.DefaultConfig()
	cfg.Version = 2
	cfg.MaxFrameSize = relayTestFrame - 64
	cfg.MaxStreamBuffer = 512 << 10 // a phone's, as the gate's mobile client has
	cfg.MaxReceiveBuffer = 4 << 20
	cfg.KeepAliveDisabled = true
	server, err := smux.Server(srvConn, cfg)
	if err != nil {
		t.Fatal(err)
	}
	client, err := smux.Client(cliConn, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	go servePulls(server)

	// One pull over a quiet link first, as a tunnel's handshake goes first.
	warm, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := readPull(warm); err != nil {
		t.Fatalf("the first pull, before any stall: %v", err)
	}

	m.stall("endpoint-cli", true)
	errs := make(chan error, 6)
	for range 6 {
		go func() {
			st, err := client.OpenStream()
			if err != nil {
				errs <- err
				return
			}
			_ = st.SetReadDeadline(time.Now().Add(15 * time.Second))
			errs <- readPull(st)
		}()
	}
	time.Sleep(200 * time.Millisecond)
	m.stall("endpoint-cli", false)
	for range 6 {
		if err := <-errs; err != nil {
			t.Errorf("%v (the relay dropped %d frames)", err, m.dropped.Load())
		}
	}
}
