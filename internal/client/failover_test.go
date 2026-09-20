// ai-generated: the whole file (tests for the graceful close, the empty room
// and the IPv6 latch).
package client

import (
	"context"
	"io"
	"net"
	"testing"
	"time"
)

// A peer that closes the control stream on purpose ends that session at
// once, without the liveness window a silent drop costs - and leaves the room
// alone. The notice says a session is over, not a room: our own server sends
// exactly this one when its provider rebuilt underneath it and when its
// liveness gave up on a client, and in both it is still in the room and
// answers the next handshake. Whether the room is worth keeping is decided by
// that handshake, not by the notice (see TestAnEmptyRoomEndsTheRunWhenAsked).
//
// ai-generated: this test replaces the one that asserted the run was ended
// (the port of olcrtc#39, resolved against olcrtc#19).
func TestGracefulCloseDropsTheSessionAndKeepsTheRoom(t *testing.T) {
	r := newRig(t, func(c *Client) {
		c.livenessFallback = 5 * time.Second // long: the ask must come from the close
		c.handshakeTimeout = time.Second
	})
	first := r.sessionID()

	start := time.Now()
	r.loseSession() // the close, and the ask it causes, with reason peer-close
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("the client took %v to act on a graceful close; it waited for something", took)
	}
	if r.ctx.Err() != nil {
		t.Fatal("a close that ends one session ended the whole run")
	}

	// The server is back on the connection the provider rebuilt, as one whose
	// own provider reconnected is, and the client picks the room up again.
	r.server.answerNew()
	r.link.callback()
	r.waitNewSession(first, 2*time.Second)
}

// A room nobody is in is the one failure the client cannot outwait, so a
// caller that has other rooms gets the run ended and moves on. The round
// stops at the first empty attempt: the other four would each spend a whole
// handshake timeout learning the same thing.
//
// ai-generated: the whole test (the port of olcrtc#39).
func TestAnEmptyRoomEndsTheRunWhenAsked(t *testing.T) {
	r := newRig(t, func(c *Client) {
		c.endOnEmptyRoom = true
		c.livenessFallback = 5 * time.Second
		c.handshakeTimeout = 200 * time.Millisecond
		c.retryDelay = 10 * time.Millisecond
	})
	r.link.peerSeen.Store(false) // the room emptied when the server was retired
	r.server.stopAnswering()
	r.loseSession()
	before := r.server.sessions.Load()

	r.link.callback()
	select {
	case <-r.ctx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("the run did not end on a room nobody is in")
	}
	if got := int(r.server.sessions.Load() - before); got != 1 {
		t.Fatalf("handshake attempts at an empty room = %d, want the first one alone", got)
	}
}

// The same empty room, for a client that is the only one there is: it keeps
// asking the provider rather than ending the run, which is olcrtc#19's
// invariant and the default.
//
// ai-generated: the whole test (the port of olcrtc#39).
func TestAnEmptyRoomIsRetriedWhenThereIsNowhereElse(t *testing.T) {
	r := newRig(t, func(c *Client) {
		c.livenessFallback = 20 * time.Millisecond
		c.handshakeTimeout = 100 * time.Millisecond
		c.retryDelay = 10 * time.Millisecond
	})
	first := r.sessionID()
	r.link.peerSeen.Store(false)
	r.server.stopAnswering()
	r.loseSession()

	r.link.callback()
	r.waitRequest(reconnectHandshake, 3*time.Second)
	if r.ctx.Err() != nil {
		t.Fatal("the run ended at an empty room the client had no alternative to")
	}
	// And it is still the client that comes back when the room refills.
	r.link.peerSeen.Store(true)
	r.server.answerNew()
	r.link.callback()
	r.waitNewSession(first, 2*time.Second)
}

// A peer that is in the room but never answers is not an empty room: it is
// retried in place even for a client that has other rooms, because the next
// hello may well be the one it answers.
//
// ai-generated: the whole test (the port of olcrtc#39).
func TestASilentPeerIsNotAnEmptyRoom(t *testing.T) {
	r := newRig(t, func(c *Client) {
		c.endOnEmptyRoom = true
		c.livenessFallback = 20 * time.Millisecond
		c.handshakeTimeout = 100 * time.Millisecond
		c.retryDelay = 10 * time.Millisecond
	})
	first := r.sessionID()
	r.server.stopAnswering() // present (peerSeen stays true), answering nothing
	r.loseSession()

	r.link.callback()
	r.waitRequest(reconnectHandshake, 3*time.Second)
	if r.ctx.Err() != nil {
		t.Fatal("the run ended on a peer that is in the room and silent")
	}
	r.server.answerNew()
	r.link.callback()
	r.waitNewSession(first, 2*time.Second)
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
