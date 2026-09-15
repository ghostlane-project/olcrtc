package client

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/xtaci/smux"

	"github.com/openlibrecommunity/olcrtc/internal/handshake"
)

// A relay may lose the client's first CLIENT_HELLO: the Jitsi videobridge
// drops a message addressed to an endpoint whose channel is not open yet,
// and the server's channel can open seconds after ours (olcbox#22). These
// tests drive openControlStreamTimeout against a fake server on the other
// end of an smux pair and check that a lost hello is sent again on a fresh
// stream, that an answered one is not, and that a rejection ends it at once.

func newSmuxPair(t *testing.T) (*smux.Session, *smux.Session) {
	t.Helper()
	a, b := net.Pipe()
	serverSess, err := smux.Server(a, testSmuxCfg())
	if err != nil {
		t.Fatalf("smux.Server() error = %v", err)
	}
	clientSess, err := smux.Client(b, testSmuxCfg())
	if err != nil {
		t.Fatalf("smux.Client() error = %v", err)
	}
	t.Cleanup(func() {
		_ = clientSess.Close()
		_ = serverSess.Close()
		_ = a.Close()
		_ = b.Close()
	})
	return serverSess, clientSess
}

func setHelloResend(t *testing.T, interval time.Duration) {
	t.Helper()
	prev := helloResendInterval
	helloResendInterval = interval
	t.Cleanup(func() { helloResendInterval = prev })
}

// acceptStream returns nil when the session is gone; the fake servers run on
// goroutines that may outlive the test, so they report through channels
// rather than t.
func acceptStream(sess *smux.Session) *smux.Stream {
	stream, err := sess.AcceptStream()
	if err != nil {
		return nil
	}
	return stream
}

func okAuth(string, map[string]any) (string, error) { return "sess-1", nil }

func TestOpenControlStreamResendsHelloWhenFirstIsUnanswered(t *testing.T) {
	serverSess, clientSess := newSmuxPair(t)
	setHelloResend(t, 50*time.Millisecond)

	served := make(chan error, 1)
	go func() {
		// The first hello reaches a server whose relay channel was not open:
		// it is read and never answered, as if it had never arrived.
		first := acceptStream(serverSess)
		if first == nil {
			served <- errors.New("no first stream")
			return
		}
		go func() { _, _ = io.Copy(io.Discard, first) }()
		second := acceptStream(serverSess)
		if second == nil {
			served <- errors.New("no second stream")
			return
		}
		_, _, err := handshake.Server(second, okAuth, "peer-1")
		served <- err
	}()

	start := time.Now()
	stream, sessionID, peerID, err := openControlStreamTimeout(
		context.Background(), clientSess, "dev", nil, 2*time.Second,
	)
	if err != nil {
		t.Fatalf("openControlStreamTimeout() error = %v", err)
	}
	defer func() { _ = stream.Close() }()
	if sessionID != "sess-1" || peerID != "peer-1" {
		t.Fatalf("openControlStreamTimeout() = (%q, %q), want (sess-1, peer-1)", sessionID, peerID)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("handshake took %v, the resend did not happen at the interval", elapsed)
	}
	if err := <-served; err != nil {
		t.Fatalf("server handshake error = %v", err)
	}
}

func TestOpenControlStreamKeepsFirstHelloWhileHedging(t *testing.T) {
	serverSess, clientSess := newSmuxPair(t)
	setHelloResend(t, 50*time.Millisecond)

	type accepted struct {
		first, second *smux.Stream
	}
	got := make(chan accepted, 1)
	go func() {
		// A slow server: it answers the first hello, but only after the
		// client has already sent a second one.
		first := acceptStream(serverSess)
		second := acceptStream(serverSess)
		time.Sleep(100 * time.Millisecond)
		if first != nil {
			_, _, _ = handshake.Server(first, okAuth, "peer-1")
		}
		got <- accepted{first, second}
	}()

	stream, _, _, err := openControlStreamTimeout(context.Background(), clientSess, "dev", nil, 2*time.Second)
	if err != nil {
		t.Fatalf("openControlStreamTimeout() error = %v", err)
	}
	defer func() { _ = stream.Close() }()
	streams := <-got
	if streams.first == nil || streams.second == nil {
		t.Fatal("server did not see both hello streams")
	}
	// The stream the client keeps is the one the welcome came back on.
	if _, err := stream.Write([]byte{0x2a}); err != nil {
		t.Fatalf("Write() on the kept stream error = %v", err)
	}
	buf := make([]byte, 1)
	_ = streams.first.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := io.ReadFull(streams.first, buf); err != nil || buf[0] != 0x2a {
		t.Fatalf("kept stream is not the answered one: read = %v, %v", buf, err)
	}
	// The hedge stream, still unanswered, is closed once the first wins: what
	// the server reads there is the second hello and then the end of stream.
	_ = streams.second.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := io.Copy(io.Discard, streams.second); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("the losing hello stream did not close: %v", err)
	}
}

func TestOpenControlStreamRejectEndsItAtOnce(t *testing.T) {
	serverSess, clientSess := newSmuxPair(t)
	setHelloResend(t, time.Second)

	go func() {
		stream := acceptStream(serverSess)
		if stream == nil {
			return
		}
		_, _, _ = handshake.Server(stream, func(string, map[string]any) (string, error) {
			return "", errors.New("no seats")
		}, "peer-1")
	}()

	start := time.Now()
	stream, sessionID, peerID, err := openControlStreamTimeout(context.Background(), clientSess, "dev", nil, 5*time.Second)
	if stream != nil || sessionID != "" || peerID != "" {
		t.Fatalf("a rejected handshake returned (%v, %q, %q)", stream, sessionID, peerID)
	}
	if !errors.Is(err, handshake.ErrRejected) {
		t.Fatalf("openControlStreamTimeout() error = %v, want %v", err, handshake.ErrRejected)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("a rejection took %v to come back; it must not wait for the resend interval", elapsed)
	}
}

func TestOpenControlStreamTimesOutWhenNobodyAnswers(t *testing.T) {
	serverSess, clientSess := newSmuxPair(t)
	setHelloResend(t, 100*time.Millisecond)

	streams := make(chan *smux.Stream, 16)
	go func() {
		for {
			stream, err := serverSess.AcceptStream()
			if err != nil {
				return
			}
			streams <- stream
			go func() { _, _ = io.Copy(io.Discard, stream) }()
		}
	}()

	start := time.Now()
	stream, sessionID, peerID, err := openControlStreamTimeout(context.Background(), clientSess, "dev", nil, 350*time.Millisecond)
	if err == nil || stream != nil || sessionID != "" || peerID != "" {
		t.Fatalf("openControlStreamTimeout() = (%v, %q, %q, %v) with nobody answering", stream, sessionID, peerID, err)
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("openControlStreamTimeout() error = %v, want a timeout", err)
	}
	if elapsed := time.Since(start); elapsed < 300*time.Millisecond || elapsed > 1500*time.Millisecond {
		t.Fatalf("gave up after %v, want about the timeout", elapsed)
	}
	// Several hellos went out, and every stream is closed on the way out.
	if n := len(streams); n < 3 {
		t.Fatalf("server saw %d hello streams, want at least 3 within the timeout", n)
	}
	for len(streams) > 0 {
		stream := <-streams
		_ = stream.SetReadDeadline(time.Now().Add(time.Second))
		if _, err := stream.Read(make([]byte, 1)); err == nil {
			t.Fatal("a hello stream was left open after the timeout")
		}
	}
}
