package salutejazz_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/app/session"
	"github.com/openlibrecommunity/olcrtc/internal/client"
	"github.com/openlibrecommunity/olcrtc/internal/control"
	"github.com/openlibrecommunity/olcrtc/internal/engine"
	enginebuiltin "github.com/openlibrecommunity/olcrtc/internal/engine/builtin"
	"github.com/openlibrecommunity/olcrtc/internal/engine/salutejazz"
	"github.com/openlibrecommunity/olcrtc/internal/server"
)

// ai-generated: the whole file (the tunnel over the slow leg, olcrtc#49).

// The whole tunnel over the fake SFU's slow legs: the client and the server,
// the datachannel transport, smux and the control stream as they run in the
// field, with only the SFU faked. The window tests in this package drive the
// engine directly; this one shows what the window is for.

const (
	tunnelKeyHex = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
	// tunnelLeg is what each leg of the SFU carries, in bytes a second.
	tunnelLeg = 128_000
	// tunnelProbeEvery and tunnelPongTimeout are both ends' liveness. The
	// timeout is shorter than the tunnel's 15 s, as the legs are faster than
	// the 34 kB/s one the 15 s is weighed against (window_budget_test.go):
	// still well over what the window lets a pong wait here, and short
	// enough that the queue an engine without it builds on these legs has
	// liveness close the session inside the test.
	tunnelProbeEvery  = 500 * time.Millisecond
	tunnelPongTimeout = 8 * time.Second
	// tunnelWatch is how long the test watches the control stream with both
	// transfers going.
	tunnelWatch = 12 * time.Second
	// tunnelChunk is what a transfer writes at a time.
	tunnelChunk = 32 << 10
)

// TestATunnelOnASlowLegKeepsItsControlStreamUnderBulk is olcrtc#49 end to
// end. A download and an upload run through the tunnel at once, on an SFU
// whose legs carry 128 kB/s each way. Without the window the SFU takes
// whatever smux's windows let a sender write - megabytes, tens of seconds of
// it on these legs - and every control ping waits at the back of it, until
// liveness closes a session whose bytes are still arriving. With it a ping
// waits behind at most a window on its way and its pong behind at most a
// window on the way back, so:
//
//   - no pong is missed, the server closes no session and the client never
//     reconnects;
//   - every pong comes back within two windows' worth of leg and a second;
//   - pongs keep coming at both ends: no gap between two is longer than that
//     and a probe interval;
//   - both transfers move, and what arrives is what was sent.
func TestATunnelOnASlowLegKeepsItsControlStreamUnderBulk(t *testing.T) {
	room := salutejazz.NewFakeRoom(t)
	tunnel := startFakeTunnel(t, room)
	room.SlowLeg(tunnel.serverID, tunnelLeg)
	room.SlowLeg(tunnel.clientID, tunnelLeg)

	down := startDownload(t, tunnel.socksAddr, startSource(t))
	up := startUpload(t, tunnel.socksAddr, startSink(t))
	eventually(t, 10*time.Second, "both transfers under way", func() bool {
		return down.moved() >= tunnelChunk && up.moved() >= tunnelChunk
	})
	watchFrom, downFrom, upFrom := time.Now(), down.moved(), up.moved()
	time.Sleep(tunnelWatch)
	watchTo, downTo, upTo := time.Now(), down.moved(), up.moved()

	tunnel.checkStillUp(t)
	pongBound := time.Duration(2*salutejazz.RelayWindowBound)*time.Second/tunnelLeg + time.Second
	tunnel.serverLive.check(t, "server", watchFrom, watchTo, pongBound)
	tunnel.clientLive.check(t, "client", watchFrom, watchTo, pongBound)

	watched := watchTo.Sub(watchFrom).Seconds()
	downRate, upRate := float64(downTo-downFrom)/watched, float64(upTo-upFrom)/watched
	t.Logf("in %s: download %.0f kB/s, upload %.0f kB/s on %d kB/s legs; the legs peaked at %d KiB to the client "+
		"and %d KiB to the server", tunnelWatch, downRate/1000, upRate/1000, tunnelLeg/1000,
		room.PeakQueuedTo(tunnel.clientID)>>10, room.PeakQueuedTo(tunnel.serverID)>>10)
	for _, moved := range []struct {
		what string
		rate float64
		bad  error
	}{{"download", downRate, down.fault()}, {"upload", upRate, up.fault()}} {
		if moved.bad != nil {
			t.Fatalf("the %s: %v", moved.what, moved.bad)
		}
		if moved.rate < tunnelLeg/4 {
			t.Fatalf("the %s moved %.0f kB/s, under a quarter of its %d kB/s leg",
				moved.what, moved.rate/1000, tunnelLeg/1000)
		}
	}
	for who, id := range map[string]string{"client": tunnel.clientID, "server": tunnel.serverID} {
		if peak := room.PeakQueuedTo(id); peak > salutejazz.RelayWindowBound {
			t.Fatalf("the leg to the %s held %d KiB, over the %d KiB a window allows",
				who, peak>>10, salutejazz.RelayWindowBound>>10)
		}
	}
	if failure := room.LastError(); failure != "" {
		t.Fatalf("fake SFU error: %s", failure)
	}
}

// fakeTunnel is a server and a client in one fake room, paired: the client's
// SOCKS address, both ends' identities in the room, and what their liveness
// and sessions have reported.
type fakeTunnel struct {
	socksAddr          string
	serverID, clientID string
	engines            *madeEngines
	serverLive         liveness
	clientLive         liveness

	closedMu     sync.Mutex
	serverClosed []string
	clientOpened atomic.Int64
	clientDown   atomic.Uint64
}

// startFakeTunnel runs a server and then a client over room, both with the
// test's liveness, until the client has a session. Both stop when the test
// ends.
func startFakeTunnel(t *testing.T, room *salutejazz.FakeRoom) *fakeTunnel {
	t.Helper()
	session.RegisterDefaults()
	provider, engines := registerFakeRoom(t, room)
	x := &fakeTunnel{engines: engines}
	ctx, cancel := context.WithCancel(context.Background())
	var running []chan error
	t.Cleanup(func() {
		cancel()
		for _, done := range running {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Errorf("the tunnel was still running 10 s after the test ended")
				return
			}
		}
	})

	serverDone := make(chan error, 1)
	running = append(running, serverDone)
	go func() {
		serverDone <- server.Run(ctx, server.Config{
			Transport: "datachannel", Provider: provider, RoomURL: "room", KeyHex: tunnelKeyHex,
			DNSServer: "127.0.0.1:53", Liveness: x.serverLive.config(),
			OnSessionClose: func(_, reason string) {
				x.closedMu.Lock()
				defer x.closedMu.Unlock()
				x.serverClosed = append(x.serverClosed, reason)
			},
		})
	}()
	eventually(t, 10*time.Second, "the server in the room", func() bool {
		return engines.count() == 1 && engines.at(0).LocalPeerID() != ""
	})

	clientDone := make(chan error, 1)
	running = append(running, clientDone)
	socks := make(chan string, 1)
	go func() {
		clientDone <- client.RunWithAddress(ctx, client.Config{
			Transport: "datachannel", Provider: provider, RoomURL: "room", KeyHex: tunnelKeyHex,
			DeviceID: "client-1", LocalAddr: "127.0.0.1:0", DNSServer: "127.0.0.1:53",
			Liveness:      x.clientLive.config(),
			OnSessionOpen: func(string) { x.clientOpened.Add(1) },
			OnHealth:      func(st control.Status) { x.clientDown.Store(st.Reconnects + st.UnhealthyEvents) },
		}, func(addr string) { socks <- addr })
	}()
	select {
	case x.socksAddr = <-socks:
	case err := <-clientDone:
		t.Fatalf("the client ended before it listened: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the client did not listen within 10 s")
	}
	eventually(t, 15*time.Second, "the client's session", func() bool { return x.clientOpened.Load() == 1 })
	x.serverID, x.clientID = engines.at(0).LocalPeerID(), engines.at(1).LocalPeerID()
	return x
}

// checkStillUp fails the test if the session the client opened has ended at
// either end: the server closed it, or the client reconnected, opened
// another, or had its engine join again.
func (x *fakeTunnel) checkStillUp(t *testing.T) {
	t.Helper()
	x.closedMu.Lock()
	closed := append([]string(nil), x.serverClosed...)
	x.closedMu.Unlock()
	if len(closed) > 0 {
		t.Fatalf("the server closed the client's session under bulk (reason %q)", closed[0])
	}
	if down := x.clientDown.Load(); down > 0 || x.clientOpened.Load() != 1 || x.engines.count() != 2 {
		t.Fatalf("the client reconnected under bulk: %d reconnects or unhealthy events, %d sessions opened, "+
			"%d engine sessions made", down, x.clientOpened.Load(), x.engines.count())
	}
}

// registerFakeRoom registers a provider whose engine is SaluteJazz on room,
// and returns its name and the engine sessions it makes, in order.
func registerFakeRoom(t *testing.T, room *salutejazz.FakeRoom) (string, *madeEngines) {
	t.Helper()
	name := "salutejazz-fake-" + t.Name()
	made := &madeEngines{}
	enginebuiltin.Register(name, func(ctx context.Context, cfg enginebuiltin.Config) (engine.Session, error) {
		sess, err := salutejazz.New(ctx, room.Join(engine.Config{
			Name: cfg.Name, OnData: cfg.OnData, OnPeerData: cfg.OnPeerData,
			OnDatagram: cfg.OnDatagram, OnPeerDatagram: cfg.OnPeerDatagram,
			RequireTargetedPeer: cfg.RequireTargetedPeer,
		}))
		if err != nil {
			return nil, err
		}
		made.add(sess.(*salutejazz.Session))
		return sess, nil
	})
	return name, made
}

// madeEngines is every engine session a provider has made.
type madeEngines struct {
	mu       sync.Mutex
	sessions []*salutejazz.Session
}

func (m *madeEngines) add(s *salutejazz.Session) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessions = append(m.sessions, s)
}

func (m *madeEngines) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sessions)
}

func (m *madeEngines) at(i int) *salutejazz.Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sessions[i]
}

// liveness is what one end's control loops reported.
type liveness struct {
	mu        sync.Mutex
	pongs     []pong
	missed    int
	unhealthy int
}

// pong is one pong: when it came back, and how long after its ping.
type pong struct {
	at  time.Time
	rtt time.Duration
}

// config is the end's liveness, reporting into l.
func (l *liveness) config() control.Config {
	return control.Config{
		Interval: tunnelProbeEvery,
		Timeout:  tunnelPongTimeout,
		OnPong: func(h control.Health) {
			l.mu.Lock()
			defer l.mu.Unlock()
			l.pongs = append(l.pongs, pong{at: h.LastSeen, rtt: h.RTT})
		},
		OnMissedPong: func(int) {
			l.mu.Lock()
			defer l.mu.Unlock()
			l.missed++
		},
		OnUnhealthy: func(int) {
			l.mu.Lock()
			defer l.mu.Unlock()
			l.unhealthy++
		},
	}
}

// check fails the test unless the end missed no pong and every pong in the
// watch (the span the arguments from and to mark) came back within bound, with
// no gap in the watch longer than bound and a probe interval: between its
// start and the first pong, between two pongs, or between the last pong and
// its end.
func (l *liveness) check(t *testing.T, who string, from, to time.Time, bound time.Duration) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.missed > 0 || l.unhealthy > 0 {
		t.Fatalf("the %s reported missed pongs %d times and went unhealthy %d times", who, l.missed, l.unhealthy)
	}
	var longest, gap time.Duration
	last, count := from, 0
	for _, p := range l.pongs {
		if p.at.Before(from) || p.at.After(to) {
			continue
		}
		count++
		longest = max(longest, p.rtt)
		gap = max(gap, p.at.Sub(last))
		last = p.at
	}
	gap = max(gap, to.Sub(last))
	t.Logf("the %s: %d pongs in %s, the slowest in %s, the longest gap %s",
		who, count, to.Sub(from).Round(time.Millisecond), longest.Round(time.Millisecond), gap.Round(time.Millisecond))
	if longest > bound {
		t.Fatalf("the %s's slowest pong under bulk took %s, over the %s two windows allow", who, longest, bound)
	}
	if gap > bound+tunnelProbeEvery {
		t.Fatalf("the %s went %s without a pong under bulk, over the %s two windows and a probe allow",
			who, gap.Round(time.Millisecond), bound+tunnelProbeEvery)
	}
}

// eventually waits until cond holds, and fails the test after timeout.
func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout after %s waiting for %s", timeout, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// patternAt is the byte a transfer carries at offset: a sequence a shifted,
// dropped or repeated chunk cannot match.
func patternAt(offset int64) byte {
	return byte(offset % 251) //nolint:gosec // an offset is never negative, so the remainder fits
}

// fillPattern fills b with the pattern from offset.
func fillPattern(b []byte, offset int64) {
	for i := range b {
		b[i] = patternAt(offset + int64(i))
	}
}

// transfer counts what one bulk transfer has moved, and the first thing that
// went wrong with it.
type transfer struct {
	bytes atomic.Int64
	mu    sync.Mutex
	err   error
}

func (x *transfer) moved() int64 { return x.bytes.Load() }

func (x *transfer) fail(err error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.err == nil {
		x.err = err
	}
}

func (x *transfer) fault() error {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.err
}

// errCorrupt is a transfer that delivered bytes nobody sent.
var errCorrupt = errors.New("bytes arrived that were not sent")

// readPattern reads r until it fails, checking each byte against the
// pattern.
func readPattern(r io.Reader, x *transfer) {
	buf, want := make([]byte, tunnelChunk), make([]byte, tunnelChunk)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			fillPattern(want[:n], x.bytes.Load())
			if !bytes.Equal(buf[:n], want[:n]) {
				x.fail(errCorrupt)
				return
			}
			x.bytes.Add(int64(n))
		}
		if err != nil {
			return
		}
	}
}

// writePattern writes the pattern to w until a write fails.
func writePattern(w io.Writer, x *transfer) {
	buf := make([]byte, tunnelChunk)
	for {
		fillPattern(buf, x.bytes.Load())
		n, err := w.Write(buf)
		x.bytes.Add(int64(n))
		if err != nil {
			return
		}
	}
}

// startSource listens on loopback for the download's target, which writes
// the pattern to whoever connects for as long as they read.
func startSource(t *testing.T) string {
	t.Helper()
	return serveLoopback(t, func(conn net.Conn) { writePattern(conn, &transfer{}) })
}

// startSink listens on loopback for the upload's target, which reads what
// arrives and checks it.
func startSink(t *testing.T) *sink {
	t.Helper()
	s := &sink{}
	s.addr = serveLoopback(t, func(conn net.Conn) { readPattern(conn, &s.got) })
	return s
}

// sink is the upload's target and what reached it.
type sink struct {
	addr string
	got  transfer
}

// serveLoopback accepts on a loopback port until the test ends and hands
// every connection to serve, closing it when serve returns.
func serveLoopback(t *testing.T, serve func(net.Conn)) string {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				serve(conn)
			}()
		}
	}()
	return ln.Addr().String()
}

// startDownload connects to target through the tunnel and reads from it
// until the test ends, checking what arrives.
func startDownload(t *testing.T, socksAddr, target string) *transfer {
	t.Helper()
	conn := socksConnect(t, socksAddr, target)
	x := &transfer{}
	go readPattern(conn, x)
	return x
}

// startUpload connects to the sink through the tunnel and writes to it until
// the test ends. What it reports is what reached the sink.
func startUpload(t *testing.T, socksAddr string, to *sink) *transfer {
	t.Helper()
	conn := socksConnect(t, socksAddr, to.addr)
	go writePattern(conn, &transfer{})
	return &to.got
}

// socksConnect opens a SOCKS5 CONNECT to target through the client, closed
// when the test ends.
func socksConnect(t *testing.T, socksAddr, target string) net.Conn {
	t.Helper()
	dialer := net.Dialer{Timeout: 5 * time.Second}
	conn, err := dialer.DialContext(context.Background(), "tcp4", socksAddr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	host, portText, err := net.SplitHostPort(target)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	request := append([]byte{5, 1, 0, 5, 1, 0, 1}, net.ParseIP(host).To4()...)
	request = binary.BigEndian.AppendUint16(request, uint16(port)) //nolint:gosec // a port fits
	if _, err := conn.Write(request); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, 2+10)
	if _, err := io.ReadFull(conn, reply); err != nil {
		t.Fatalf("the SOCKS connect: %v", err)
	}
	if !bytes.Equal(reply[:2], []byte{5, 0}) || reply[3] != 0 {
		t.Fatalf("the SOCKS connect was refused: %v", reply)
	}
	return conn
}
