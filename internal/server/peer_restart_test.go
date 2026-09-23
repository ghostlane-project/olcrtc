package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtaci/smux"

	"github.com/openlibrecommunity/olcrtc/internal/control"
	cryptopkg "github.com/openlibrecommunity/olcrtc/internal/crypto"
	"github.com/openlibrecommunity/olcrtc/internal/framing"
	"github.com/openlibrecommunity/olcrtc/internal/handshake"
	"github.com/openlibrecommunity/olcrtc/internal/muxconn"
	"github.com/openlibrecommunity/olcrtc/internal/runtime"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
	"github.com/openlibrecommunity/olcrtc/internal/tunnelcore"
)

// ai-generated: the whole file (a client that retries its handshake under the
// same relay identity, olcrtc#49).

// peerTestLiveness pings too seldom to put a ping on the wire while a test
// counts what the server writes.
var peerTestLiveness = control.Config{Interval: time.Hour}

// relayLinkStub is a peer-routing transport without a control plane of its
// own - datachannel over a relay that stamps sender identities - whose sends
// to a peer go to toPeer.
type relayLinkStub struct {
	peerRoutingStub
	features transport.Features
	mu       sync.Mutex
	toPeer   func(peerID string, data []byte)
}

func (l *relayLinkStub) Features() transport.Features { return l.features }

func (l *relayLinkStub) SendTo(peerID string, data []byte) error {
	l.mu.Lock()
	deliver := l.toPeer
	l.mu.Unlock()
	if deliver != nil {
		deliver(peerID, append([]byte(nil), data...))
	}
	return nil
}

// newRelayServer is a server routing peers over link, the way Run sets one up
// for datachannel.
func newRelayServer(t *testing.T, link *relayLinkStub) *Server {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		baseCtx: ctx, ln: link, peerLn: link, ring: cryptopkg.SingleEntry(newServerTestKeys(t), ""),
		authHook: defaultAuthHook, onOpen: func(string, string, map[string]any) {},
		onClose: func(string, string) {}, health: runtime.NewHealthTracker(nil),
		liveness:     peerTestLiveness,
		peerSessions: make(map[string]*peerSession), peerStats: make(map[string]peerStat),
		done: make(chan struct{}), meter: newMeter(),
	}
	t.Cleanup(func() {
		cancel()
		s.shutdown()
		s.wg.Wait()
	})
	return s
}

// relayClient is one client smux session under peerID, over its own conn: a
// client builds a new one for every handshake it tries.
type relayClient struct {
	conn    *muxconn.Conn
	session *smux.Session
}

// clientLinkStub is the client's side: what it sends reaches the server as
// the peer's.
type clientLinkStub struct {
	serverLinkStub
	features transport.Features
	send     func([]byte)
}

func (l *clientLinkStub) Features() transport.Features { return l.features }

func (l *clientLinkStub) Send(data []byte) error {
	l.send(append([]byte(nil), data...))
	return nil
}

func newRelayClient(t *testing.T, s *Server, peerID string) *relayClient {
	t.Helper()
	keys, err := cryptopkg.NewKeySet([]byte("01234567890123456789012345678901"), cryptopkg.Client)
	if err != nil {
		t.Fatal(err)
	}
	link := &clientLinkStub{features: s.ln.Features(), send: func(data []byte) { s.onPeerData(peerID, data) }}
	conn := muxconn.New(link, keys)
	session, err := tunnelcore.NewSession(conn, tunnelcore.ClientRole, runtime.SmuxConfigFor(link))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = session.Close()
		_ = conn.Close()
	})
	return &relayClient{conn: conn, session: session}
}

// handshake opens the client's control stream and runs its hello.
func (c *relayClient) handshake(t *testing.T) *smux.Stream {
	t.Helper()
	stream, err := c.session.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream() error = %v", err)
	}
	_ = stream.SetDeadline(time.Now().Add(5 * time.Second))
	if _, _, err := handshake.Client(stream, "relay-client", nil); err != nil {
		t.Fatalf("handshake.Client() error = %v", err)
	}
	_ = stream.SetDeadline(time.Time{})
	return stream
}

// A client that closes its control stream has left that session: nothing the
// server writes into it now reaches the client, and what it did write would
// land in the session the client runs next under the same relay identity -
// on the stream with the number this one had, which is where a retried hello
// runs. The server ends such a session without a word.
func TestAPeerThatLeftItsSessionIsToldNothing(t *testing.T) {
	link := &relayLinkStub{}
	s := newRelayServer(t, link)
	client := newRelayClient(t, s, "peer")
	var sent atomic.Int64
	link.mu.Lock()
	link.toPeer = func(_ string, data []byte) {
		sent.Add(1)
		client.conn.Push(data)
	}
	link.mu.Unlock()

	stream := client.handshake(t)
	peer := lookupPeer(s, "peer")
	if peer == nil {
		t.Fatal("the server built no session for the peer")
	}
	waitPeerControl(t, peer)
	before := sent.Load()
	_ = stream.Close()

	deadline := time.Now().Add(2 * time.Second)
	for lookupPeer(s, "peer") == peer {
		if time.Now().After(deadline) {
			t.Fatal("the server kept the session its peer left")
		}
		time.Sleep(2 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if extra := sent.Load() - before; extra != 0 {
		t.Fatalf("the server wrote %d records to a peer that had left the session", extra)
	}
}

// A peer's handshake that fails is that peer's failure: the server ends that
// peer's session and nobody else's. Reinstalling peer routing for it - the
// path the server's own session takes - closed every other client's session
// with it, and a client that gives a handshake up under its relay identity
// leaves the server just such a handshake to fail.
func TestAFailedPeerHandshakeLeavesTheOtherPeersAlone(t *testing.T) {
	link := &relayLinkStub{}
	s := newRelayServer(t, link)
	other := newRelayClient(t, s, "other")
	failing := newRelayClient(t, s, "failing")
	clients := map[string]*relayClient{"other": other, "failing": failing}
	link.mu.Lock()
	link.toPeer = func(peerID string, data []byte) {
		if client := clients[peerID]; client != nil {
			client.conn.Push(data)
		}
	}
	link.mu.Unlock()
	other.handshake(t)
	kept := lookupPeer(s, "other")
	if kept == nil {
		t.Fatal("the server built no session for the other peer")
	}

	// A peer whose session goes away before its hello is read.
	if _, err := failing.session.OpenStream(); err != nil {
		t.Fatal(err)
	}
	peer := lookupPeer(s, "failing")
	if peer == nil {
		t.Fatal("the server built no session for the failing peer")
	}
	_ = peer.dataSession().Close()

	deadline := time.Now().Add(2 * time.Second)
	for lookupPeer(s, "failing") == peer {
		if time.Now().After(deadline) {
			t.Fatal("the server kept the session whose handshake failed")
		}
		time.Sleep(2 * time.Millisecond)
	}
	if got := lookupPeer(s, "other"); got != kept {
		t.Fatal("a failed handshake of one peer ended another peer's session")
	}
	if kept.sid() == "" || kept.dataSession().IsClosed() {
		t.Fatal("the other peer's session was closed")
	}
}

// waitPeerControl waits until the server runs peer's control loop.
func waitPeerControl(t *testing.T, peer *peerSession) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		peer.mu.Lock()
		running := peer.controlStrm != nil
		peer.mu.Unlock()
		if running {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the server never started the peer's control loop")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// A peer that starts its session over is answered by a session of its own at
// once, from the record the new session begins with - its first stream and
// the hello on it, one record - in the pin group of the session it left: the
// key it holds is the key it held, and a new session that had to learn it
// from the next record would sit on the welcome until the peer sent one.
func TestAPeerThatStartsItsSessionOverIsAnsweredAtOnce(t *testing.T) {
	link := &relayLinkStub{}
	s := newRelayServer(t, link)
	client := newRelayClient(t, s, "peer")
	welcomed := make(chan []byte, 16)
	link.mu.Lock()
	link.toPeer = func(_ string, data []byte) {
		client.conn.Push(data)
		welcomed <- data
	}
	link.mu.Unlock()
	client.handshake(t)
	left := lookupPeer(s, "peer")
	if left == nil {
		t.Fatal("the server built no session for the peer")
	}
	for len(welcomed) > 0 {
		<-welcomed
	}

	// The retry: a new smux session's first stream, opened again, and the
	// hello on it, in one record.
	keys, err := cryptopkg.NewKeySet([]byte("01234567890123456789012345678901"), cryptopkg.Client)
	if err != nil {
		t.Fatal(err)
	}
	var hello bytes.Buffer
	err = framing.WriteJSON(&hello, handshake.Hello{
		Version: handshake.ProtoVersion, Type: handshake.TypeHello, DeviceID: "relay-client",
		Challenge: strings.Repeat("ab", 16),
	}, handshake.MaxMessageSize)
	if err != nil {
		t.Fatal(err)
	}
	version := byte(runtime.SmuxConfigFor(link).Version) //nolint:gosec // smux versions are 1 and 2
	record := append(smuxFrameV(version, 0, 3, nil), smuxFrameV(version, 2, 3, hello.Bytes())...)
	sealed, err := keys.Seal(record, []byte("olcrtc/muxconn/v2/data"))
	if err != nil {
		t.Fatal(err)
	}
	s.onPeerData("peer", sealed)

	deadline := time.After(2 * time.Second)
	for {
		select {
		case data := <-welcomed:
			opened, err := keys.Open(data, []byte("olcrtc/muxconn/v2/data"))
			if err == nil && bytes.Contains(opened, []byte(handshake.TypeWelcome)) {
				if next := lookupPeer(s, "peer"); next == left || next == nil {
					t.Fatal("the welcome came from the session the peer left")
				}
				return
			}
		case <-deadline:
			t.Fatal("the server did not answer the hello a peer started its session over with")
		}
	}
}

// smuxFrameV lays one smux frame out the way xtaci/smux writes it.
func smuxFrameV(version, cmd byte, sid uint32, data []byte) []byte {
	frame := make([]byte, 8, 8+len(data))
	frame[0], frame[1] = version, cmd
	binary.LittleEndian.PutUint16(frame[2:4], uint16(len(data))) //nolint:gosec // test frames are small
	binary.LittleEndian.PutUint32(frame[4:8], sid)
	return append(frame, data...)
}

// datachannelFeatures are the link features datachannel asks for: records of
// 12 KiB, and small frames batched for 5 ms.
var datachannelFeatures = transport.Features{MaxPayloadSize: 12 * 1024, WriteInterval: 5 * time.Millisecond}

// A client opens streams at once - a tunnel per SOCKS connection, each on its
// own goroutine, and every request parked on a session that has just come up -
// and smux writes their SYNs in no set order. A SYN below one the server has
// seen is still that session's: the peer has not started its session over, and
// the session it is on stays.
func TestStreamsOpenedAtOnceStayOnThePeersSession(t *testing.T) {
	link := &relayLinkStub{features: datachannelFeatures}
	s := newRelayServer(t, link)
	var restarted atomic.Int64
	s.onClose = func(_, reason string) {
		if reason == "restarted" {
			restarted.Add(1)
		}
	}
	client := newRelayClient(t, s, "peer")
	link.mu.Lock()
	link.toPeer = func(_ string, data []byte) { client.conn.Push(data) }
	link.mu.Unlock()
	client.handshake(t)
	peer := lookupPeer(s, "peer")
	if peer == nil {
		t.Fatal("the server built no session for the peer")
	}
	waitPeerControl(t, peer)

	// Uploads keep the client's send path busy while the streams open.
	chunk := bytes.Repeat([]byte{0x5a}, 32*1024)
	for range 3 {
		go func() {
			stream, err := client.session.OpenStream()
			if err != nil {
				return
			}
			for {
				if _, err := stream.Write(chunk); err != nil {
					return
				}
			}
		}()
	}
	const bursts, opens = 4, 32
	for range bursts {
		start := make(chan struct{})
		var wg sync.WaitGroup
		for range opens {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, _ = client.session.OpenStream()
			}()
		}
		close(start)
		wg.Wait()
	}

	deadline := time.Now().Add(2 * time.Second)
	for lookupPeer(s, "peer") == peer && peer.dataSession().NumStreams() < bursts*opens {
		if time.Now().After(deadline) {
			t.Fatalf("the server holds %d of the %d streams opened", peer.dataSession().NumStreams(), bursts*opens)
		}
		time.Sleep(2 * time.Millisecond)
	}
	if lookupPeer(s, "peer") != peer || restarted.Load() != 0 {
		t.Fatal("streams opened at once were taken for the peer starting its session over")
	}
}
