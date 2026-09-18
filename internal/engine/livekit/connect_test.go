package livekit

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	lksdk "github.com/owenewans/owenlivekit/v2"

	"github.com/openlibrecommunity/olcrtc/internal/engine"
)

// ai-generated: whole file, cover for the join budget and for giving up on a
// join (ghostlane#38).

const (
	// testJoinBudget stands in for connectTimeout. It is longer than the
	// SDK's own 5 s default, so a join that ends before it shows the budget
	// never reached the SDK.
	testJoinBudget = 7 * time.Second
	// testJoinSlack is how much earlier than its budget a join may end.
	testJoinSlack = 500 * time.Millisecond
	// testGrace is how long a test waits past a deadline, a budget or a
	// cancel, before it calls Connect stuck.
	testGrace = 2 * time.Second
)

// joinOnly is the JoinResponse the stub sends: the subscriber is the primary
// peer connection and its offer never comes, so ICE cannot start and the SDK
// can only wait out its budget. The SDK reads a text frame as protojson.
const joinOnly = `{"join":{"room":{"sid":"RM_stub","name":"stub"},` +
	`"participant":{"sid":"PA_stub","identity":"stub"},` +
	`"subscriberPrimary":true,"serverInfo":{"edition":"Standard"}}}`

// signalStub is a LiveKit signalling endpoint that answers the join and says
// nothing after it. joined fires once the JoinResponse is out, left once the
// client has closed the socket.
type signalStub struct {
	url    string
	joined chan struct{}
	left   chan struct{}
}

func newSignalStub(t *testing.T) *signalStub {
	t.Helper()
	stub := &signalStub{joined: make(chan struct{}, 1), left: make(chan struct{}, 1)}
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !websocket.IsWebSocketUpgrade(r) {
			// The SDK checks the endpoint over plain HTTP after a failed join.
			w.WriteHeader(http.StatusOK)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			_ = conn.Close()
			notify(stub.left)
		}()
		if err := conn.WriteMessage(websocket.TextMessage, []byte(joinOnly)); err != nil {
			return
		}
		notify(stub.joined)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	stub.url = "ws" + strings.TrimPrefix(srv.URL, "http")
	return stub
}

func notify(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// newStubSession returns a session that joins url on testJoinBudget.
func newStubSession(t *testing.T, url string) *Session {
	t.Helper()
	sess, err := New(context.Background(), engine.Config{URL: url, Token: "stub-token"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	s, ok := sess.(*Session)
	if !ok {
		t.Fatalf("New() type = %T, want *Session", sess)
	}
	s.joinTimeout = testJoinBudget
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func connectInBackground(ctx context.Context, s *Session) <-chan error {
	done := make(chan error, 1)
	go func() { done <- s.Connect(ctx) }()
	return done
}

func TestConnectWaitsOutTheJoinBudget(t *testing.T) {
	t.Parallel()
	stub := newSignalStub(t)
	s := newStubSession(t, stub.url)

	start := time.Now()
	done := connectInBackground(context.Background(), s)
	var err error
	select {
	case err = <-done:
	case <-time.After(testJoinBudget + testGrace):
		t.Fatalf("Connect() still waiting %s past its %s budget", testGrace, testJoinBudget)
	}
	elapsed := time.Since(start)
	if !errors.Is(err, lksdk.ErrConnectionTimeout) {
		t.Fatalf("Connect() error = %v, want %v", err, lksdk.ErrConnectionTimeout)
	}
	if elapsed < testJoinBudget-testJoinSlack {
		t.Fatalf("Connect() gave up after %s, want no sooner than its %s budget",
			elapsed.Round(time.Millisecond), testJoinBudget)
	}
	t.Logf("Connect() gave up after %s on a %s budget", elapsed.Round(time.Millisecond), testJoinBudget)
}

func TestConnectReturnsWhenContextIsCancelled(t *testing.T) {
	t.Parallel()
	stub := newSignalStub(t)
	s := newStubSession(t, stub.url)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := connectInBackground(ctx, s)
	select {
	case <-stub.joined:
	case <-time.After(testJoinBudget):
		t.Fatal("the join never reached the signalling stub")
	}
	cancel()
	cancelled := time.Now()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Connect() error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(testGrace):
		t.Fatalf("Connect() still waiting %s after its context was cancelled", testGrace)
	}
	t.Logf("Connect() returned %s after the cancel", time.Since(cancelled).Round(time.Microsecond))
	// Nobody waits for that join any more; it still ends on its budget and
	// closes its signalling socket instead of lingering.
	select {
	case <-stub.left:
	case <-time.After(testJoinBudget + testGrace):
		t.Fatal("the abandoned join never closed its signalling socket")
	}
}

// TestConnectLeavesAJoinThatLandsLate covers a join that succeeds after its
// caller gave up: nobody will use that room, so it must not stay in it.
func TestConnectLeavesAJoinThatLandsLate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		giveUp  func(s *Session, cancel context.CancelFunc)
		wantErr error
	}{
		{
			name:    "context cancelled",
			giveUp:  func(_ *Session, cancel context.CancelFunc) { cancel() },
			wantErr: context.Canceled,
		},
		{
			name:    "session closed",
			giveUp:  func(s *Session, _ context.CancelFunc) { _ = s.Close() },
			wantErr: ErrSessionClosed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			room := newFakeRoom()
			release := make(chan struct{})
			var once sync.Once
			releaseJoin := func() { once.Do(func() { close(release) }) }
			t.Cleanup(releaseJoin)
			s := &Session{
				url:   testOldURL,
				token: testOldToken,
				connectRoom: func(string, string, *lksdk.RoomCallback, ...lksdk.ConnectOption) (roomHandle, error) {
					<-release
					return room, nil
				},
				closeCh:   make(chan struct{}),
				sendQueue: make(chan []byte, engine.DefaultSendQueueSize),
				done:      make(chan struct{}),
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			done := connectInBackground(ctx, s)
			tt.giveUp(s, cancel)
			select {
			case err := <-done:
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("Connect() error = %v, want %v", err, tt.wantErr)
				}
			case <-time.After(testGrace):
				t.Fatal("Connect() kept waiting on a join its caller gave up on")
			}
			releaseJoin()
			waitFor(t, func() bool {
				room.mu.Lock()
				defer room.mu.Unlock()
				return room.disconnected == 1
			})
			if s.currentRoom() != nil {
				t.Fatal("a join that landed after its caller left was installed")
			}
		})
	}
}
