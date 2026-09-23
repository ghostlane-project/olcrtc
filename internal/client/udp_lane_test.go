package client

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtaci/smux"

	"github.com/openlibrecommunity/olcrtc/internal/engine"
	enginebuiltin "github.com/openlibrecommunity/olcrtc/internal/engine/builtin"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
	"github.com/openlibrecommunity/olcrtc/internal/transport/datachannel"
)

// ai-generated: the whole file (a UDP ASSOCIATE that never frees its SOCKS
// slot, olcrtc#49).

// udpLaneTestTarget is a target no rule names and no resolver serves, so a
// datagram for it can only take the lane.
const udpLaneTestTarget = "192.0.2.1"

// heldLane is a datagram lane that exists and does not take anything until
// the test opens it: a SaluteJazz lane over its mark, or one still
// negotiating behind the reliable one.
type heldLane struct {
	transport.Transport
	open  atomic.Bool
	polls atomic.Int32
	sent  atomic.Int32
}

func (*heldLane) Features() transport.Features { return transport.Features{Datagram: true} }

func (l *heldLane) DatagramCanSend() bool {
	l.polls.Add(1)
	return l.open.Load()
}

func (l *heldLane) SendDatagram([]byte) error {
	l.sent.Add(1)
	return nil
}

// jitsiLikeLink is the transport the association used to hang on: a
// datachannel over an engine with no datagram lane. The datachannel has the
// lane's methods whatever its engine, and there DatagramCanSend never turns
// true.
func jitsiLikeLink(t *testing.T) transport.Transport {
	t.Helper()
	room := &listenerTestRoom{sessions: make(map[*listenerTestSession]struct{})}
	provider := "udp-lane-test-" + t.Name()
	enginebuiltin.Register(provider, func(_ context.Context, cfg enginebuiltin.Config) (engine.Session, error) {
		return room.newSession(cfg.OnData), nil
	})
	link, err := datachannel.New(context.Background(), transport.Config{Provider: provider, RoomURL: "room"})
	if err != nil {
		t.Fatalf("datachannel.New() error = %v", err)
	}
	t.Cleanup(func() { _ = link.Close() })
	if err := link.Connect(context.Background()); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	dg, ok := link.(transport.DatagramTransport)
	if !ok || dg.DatagramCanSend() || link.Features().Datagram || !link.CanSend() {
		t.Fatal("the link is not a connected datachannel that says it has no lane")
	}
	return link
}

// serveSocksForTest runs c's accept loop, where the SOCKS slots are counted,
// on a loopback listener until the test ends.
func serveSocksForTest(t *testing.T, c *Client) string {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp4", directTestLoopback+":0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.goTracked(func() { c.acceptLoop(ctx, listener) })
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		c.closeSocksConns()
		waitTracked(t, c)
	})
	return listener.Addr().String()
}

// socksSlots is how many SOCKS connections hold a slot.
func socksSlots(c *Client) int {
	c.socksMu.Lock()
	defer c.socksMu.Unlock()
	return len(c.socksConns)
}

// waitSocksSlots waits for the slots held to come down to want.
func waitSocksSlots(t *testing.T, c *Client, want int, within time.Duration, why string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for socksSlots(c) != want {
		if time.Now().After(deadline) {
			t.Fatalf("%d SOCKS slots held %s after %s, want %d", socksSlots(c), within, why, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// udpAssociate dials addr, asks for a UDP ASSOCIATE from any source and
// returns the control connection with the reply.
func udpAssociate(t *testing.T, addr string) (net.Conn, []byte) {
	t.Helper()
	conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(context.Background(), "tcp4", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	if _, err := conn.Write([]byte{socksVersion, 1, 0}); err != nil {
		t.Fatal(err)
	}
	greeting := make([]byte, 2)
	if _, err := io.ReadFull(conn, greeting); err != nil || greeting[1] != 0 {
		t.Fatalf("greeting = %v, %v", greeting, err)
	}
	if _, err := conn.Write([]byte{socksVersion, socksCmdUDPAssociate, 0, socksAddrIPv4, 0, 0, 0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, 10)
	if _, err := io.ReadFull(conn, reply); err != nil {
		t.Fatalf("no reply to the UDP ASSOCIATE within a second: %v", err)
	}
	_ = conn.SetDeadline(time.Time{})
	return conn, reply
}

// Without a datagram lane an association can never carry a datagram, and
// the datachannel has the lane's methods whatever its engine: over Jitsi
// the association used to be accepted, its first datagram waited for the
// run to end, and its SOCKS slot with it. It is refused at once, before it
// waits for anything, the session included.
func TestUDPAssociateWithoutADatagramLaneIsRefusedAtOnce(t *testing.T) {
	cases := map[string]func(t *testing.T) *Client{
		"with the session up": func(t *testing.T) *Client {
			t.Helper()
			c, _ := newDNSTestClient(t, func(*smux.Stream) error { return nil })
			return c
		},
		"with the session down": func(t *testing.T) *Client {
			t.Helper()
			c := newDirectTestClient(t, nil, staticLookup{})
			// Parked on the session, the reply would come 10 s late, and
			// udpAssociate reads for one.
			c.sessionReadyTimeout = 10 * time.Second
			return c
		},
	}
	for name, newClient := range cases {
		t.Run(name, func(t *testing.T) {
			c := newClient(t)
			c.ln = jitsiLikeLink(t)
			conn, reply := udpAssociate(t, serveSocksForTest(t, c))
			if reply[1] != socksRepHostUnreachable {
				t.Fatalf("UDP ASSOCIATE on a lane-less link: REP = %d, want %d", reply[1], socksRepHostUnreachable)
			}
			_ = conn.SetReadDeadline(time.Now().Add(time.Second))
			if n, err := conn.Read(make([]byte, 1)); err == nil {
				t.Fatalf("the refused control connection stayed open and sent %d bytes", n)
			}
			waitSocksSlots(t, c, 0, time.Second, "the refusal")
		})
	}
}

// An association whose lane never opens used to hold its first datagram
// until the whole run ended: the read loop sat in the wait, the closed
// control connection closed a socket nobody read, and the SOCKS slot stayed
// taken. 512 of them refused every new SOCKS connection, TCP included. The
// wait now ends with the association.
func TestAnAssociationWhoseLaneNeverOpensEndsWithItsControlConnection(t *testing.T) {
	lane := &heldLane{}
	c := newDirectTestClient(t, mustRules(t, "10.0.0.0/8\n"), staticLookup{})
	c.ln = lane
	c.udpFlows = map[uint64]clientUDPFlow{}
	// Only the association's end can let the datagram go within this test.
	c.datagramReadyTimeout = time.Minute

	conn, reply := udpAssociate(t, serveSocksForTest(t, c))
	if reply[1] != socksRepSuccess {
		t.Fatalf("UDP ASSOCIATE: REP = %d, want %d", reply[1], socksRepSuccess)
	}
	relay := &net.UDPAddr{IP: net.IP(reply[4:8]), Port: int(binary.BigEndian.Uint16(reply[8:10]))}
	_, cli := dnsTestSockets(t)
	if _, err := cli.WriteToUDP(socksDatagram(t, udpLaneTestTarget, 4444, []byte("x")), relay); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for lane.polls.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("the datagram never came to wait for the lane")
		}
		time.Sleep(time.Millisecond)
	}
	if got := socksSlots(c); got != 1 {
		t.Fatalf("SOCKS slots held by the association = %d, want 1", got)
	}

	_ = conn.Close()
	waitSocksSlots(t, c, 0, 2*time.Second, "the control connection closed")
	if lane.sent.Load() != 0 {
		t.Fatal("a datagram went down a lane that never opened")
	}
}

// The lane is lossy by contract: a datagram waits for it a short while, so a
// lane over its mark for a moment still takes it, and past that it is
// dropped and the association's read loop moves on to the next one.
func TestADatagramWaitsForTheLaneNoLongerThanItsDeadline(t *testing.T) {
	const wait = 150 * time.Millisecond
	lane := &heldLane{}
	c := newDirectTestClient(t, nil, staticLookup{})
	c.ln = lane
	c.udpFlows = map[uint64]clientUDPFlow{}
	c.datagramReadyTimeout = wait
	assoc, cli := dnsTestSockets(t)
	src := cli.LocalAddr().(*net.UDPAddr)
	packet := socksDatagram(t, udpLaneTestTarget, 4444, []byte("x"))
	// The association outlives the test: only the deadline can end the wait.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := time.Now()
	done := make(chan struct{})
	go func() {
		c.forwardLocalUDP(ctx, lane, assoc, src, packet)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("a datagram still waits for the lane 2 s into a %s deadline", wait)
	}
	if waited := time.Since(start); waited < wait {
		t.Fatalf("the datagram was dropped after %s, before its %s deadline", waited, wait)
	}
	if lane.sent.Load() != 0 {
		t.Fatal("a datagram went down a lane that never opened")
	}

	// A lane that opens inside the deadline takes the datagram.
	c.datagramReadyTimeout = 5 * time.Second
	opened := time.AfterFunc(50*time.Millisecond, func() { lane.open.Store(true) })
	defer opened.Stop()
	c.forwardLocalUDP(ctx, lane, assoc, src, packet)
	if lane.sent.Load() != 1 {
		t.Fatalf("datagrams sent once the lane opened = %d, want 1", lane.sent.Load())
	}
}
