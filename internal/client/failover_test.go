// ai-generated: the whole file (tests for the closed-by-peer fast path and the
// IPv6 latch).
package client

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/xtaci/smux"

	"github.com/openlibrecommunity/olcrtc/internal/control"
	"github.com/openlibrecommunity/olcrtc/internal/runtime"
	"github.com/openlibrecommunity/olcrtc/internal/tunnelcore"
)

// A peer that closes the control stream on purpose - the server retiring the
// room - ends the session at once, so a supervisor can move to the next room.
// The alternative was the reconnect path: a liveness fallback and three
// handshakes against a room that said it was leaving, about a minute.
func TestControlClosedByPeerEndsTheSession(t *testing.T) {
	a, b := net.Pipe()
	defer func() {
		_ = a.Close()
		_ = b.Close()
	}()
	serverSess, err := smux.Server(a, testSmuxCfg())
	if err != nil {
		t.Fatalf("smux.Server() error = %v", err)
	}
	defer func() { _ = serverSess.Close() }()
	clientSess, err := smux.Client(b, testSmuxCfg())
	if err != nil {
		t.Fatalf("smux.Client() error = %v", err)
	}
	defer func() { _ = clientSess.Close() }()

	peerStreamCh := make(chan *smux.Stream, 1)
	go func() {
		stream, acceptErr := serverSess.AcceptStream()
		if acceptErr == nil {
			peerStreamCh <- stream
		}
	}()
	stream, err := clientSess.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream() error = %v", err)
	}
	peerStream := <-peerStreamCh

	liveness := control.Config{Interval: 10 * time.Millisecond, Timeout: 100 * time.Millisecond, Failures: 2}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &Client{sessionID: "sid-retired", health: runtime.NewHealthTracker(nil)}
	c.health.RecordSession("sid-retired")
	c.startControlLoop(ctx, Config{Liveness: liveness}, cancel, stream)

	peerCtx, peerCancel := context.WithCancel(context.Background())
	defer peerCancel()
	go func() { _ = control.Run(peerCtx, peerStream, liveness) }()

	// The server's own goodbye: the close notification it sends before it exits.
	tunnelcore.NotifyControlClose(peerStream)

	select {
	case <-ctx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("session was not ended after the peer closed the control stream")
	}
	c.waitGoroutines()
}

func TestNoteConnectFailureLatchesMissingIPv6(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		target string
		want   bool
	}{
		{name: "ipv6 unreachable latches", err: &connectAckError{code: socksRepHostUnreachable},
			target: "2606:4700:4700::1111", want: true},
		{name: "ipv4 unreachable does not latch", err: &connectAckError{code: socksRepHostUnreachable},
			target: "1.1.1.1", want: false},
		{name: "domain does not latch", err: &connectAckError{code: socksRepHostUnreachable},
			target: "example.com", want: false},
		{name: "other ack code does not latch", err: &connectAckError{code: socksRepNetworkUnreachable},
			target: "2606:4700:4700::1111", want: false},
		{name: "non-ack error does not latch", err: ErrRemoteNotReady,
			target: "2606:4700:4700::1111", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{}
			c.noteConnectFailure(tt.err, tt.target)
			if got := c.peerNoIPv6.Load(); got != tt.want {
				t.Fatalf("peerNoIPv6 = %v, want %v", got, tt.want)
			}
		})
	}
}

// Once the exit is known to have no IPv6 the refusal comes from the client
// itself: the point of the latch is that no tunnel stream is spent on it. With
// no session installed, the tunnel path would instead park the request on the
// session-ready wait, so an immediate reply is what proves the shortcut.
func TestTunnelRefusesIPv6LocallyWhenExitHasNone(t *testing.T) {
	c := &Client{sessionReady: make(chan struct{})}
	c.peerNoIPv6.Store(true)
	server, client := net.Pipe()
	defer func() {
		_ = server.Close()
		_ = client.Close()
	}()
	go c.handleSocks5(context.Background(), server)

	if _, err := client.Write([]byte{socksVersion, 1, 0}); err != nil {
		t.Fatalf("Write() greeting error = %v", err)
	}
	greeting := make([]byte, 2)
	if _, err := io.ReadFull(client, greeting); err != nil {
		t.Fatalf("ReadFull() greeting error = %v", err)
	}
	request := make([]byte, 0, 4+net.IPv6len+2)
	request = append(request, socksVersion, socksCmdConnect, 0, socksAddrIPv6)
	request = append(request, net.ParseIP("2606:4700:4700::1111").To16()...)
	request = append(request, 1, 187)
	if _, err := client.Write(request); err != nil {
		t.Fatalf("Write() request error = %v", err)
	}

	replyCh := make(chan []byte, 1)
	go func() {
		reply := make([]byte, 4+net.IPv6len+2)
		if _, err := io.ReadFull(client, reply); err != nil {
			close(replyCh)
			return
		}
		replyCh <- reply
	}()
	select {
	case reply, ok := <-replyCh:
		if !ok {
			t.Fatal("ReadFull() reply failed")
		}
		if reply[1] != socksRepHostUnreachable {
			t.Fatalf("reply rep = %d, want %d", reply[1], socksRepHostUnreachable)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("IPv6 request was not refused locally")
	}
}
