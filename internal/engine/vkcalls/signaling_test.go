package vkcalls

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// The server side of the test is one actor: gorilla allows a single
// concurrent reader and writer per connection, so all server-side access is
// serialized through one mutex.
type fakeSFU struct {
	conn     *websocket.Conn
	mu       sync.Mutex
	seenPong bool
	reject   bool
}

func (f *fakeSFU) write(frame any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_ = f.conn.WriteJSON(frame)
}

func (f *fakeSFU) serve() {
	f.mu.Lock()
	_ = f.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	f.mu.Unlock()
	for {
		_, data, err := f.conn.ReadMessage()
		if err != nil {
			return
		}
		if string(data) == "pong" {
			f.mu.Lock()
			f.seenPong = true
			f.mu.Unlock()
			continue
		}
		var frame map[string]any
		if json.Unmarshal(data, &frame) != nil {
			continue
		}
		command, isString := frame["command"].(string)
		if !isString {
			continue
		}
		reply := map[string]any{"sequence": frame["sequence"], "response": command, "type": "response"}
		if f.reject {
			reply = map[string]any{"sequence": frame["sequence"], "type": "error"}
		}
		f.write(reply)
	}
}

// TestSignalingCommandFlow drives the signaling client through its contract:
// the connection notification, command response correlation, the ping/pong
// keepalive, rejection and the closed-session failure.
func TestSignalingCommandFlow(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	fsfu := &fakeSFU{}
	connected := make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		fsfu.conn = conn
		once.Do(func() { close(connected) })
		go fsfu.serve()
		fsfu.write(map[string]any{
			"type": "notification", "notification": "connection",
			"peerId": map[string]any{"id": 111},
			"conversation": map[string]any{"topology": "SERVER", "participants": []any{
				map[string]any{"id": 42, "state": "ACCEPTED"},
				map[string]any{"id": 43, "state": "HUNGUP"},
			}},
		})
	}))
	defer server.Close()

	client, err := dialSignaling(context.Background(), "ws"+server.URL[4:]+"?token=t&userId=42", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.close()
	<-connected

	connection, err := client.waitConnection(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if connection.Conversation == nil || connection.Conversation.Topology != "SERVER" {
		t.Fatalf("connection topology missing: %+v", connection)
	}
	if ids := participantIDs(connection, "42"); len(ids) != 0 {
		t.Fatalf("expected self filtered out, got %v", ids)
	}

	response, err := client.command(context.Background(), "allocate-consumer", map[string]any{"capabilities": referenceCapabilities()})
	if err != nil {
		t.Fatal(err)
	}
	if response.Response != "allocate-consumer" {
		t.Fatalf("response %q", response.Response)
	}

	// The keepalive: a text "ping" from the server is answered "pong"; the
	// serve loop records it.
	fsfu.mu.Lock()
	_ = fsfu.conn.WriteMessage(websocket.TextMessage, []byte("ping"))
	fsfu.mu.Unlock()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		fsfu.mu.Lock()
		seen := fsfu.seenPong
		fsfu.mu.Unlock()
		if seen {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	fsfu.mu.Lock()
	seen := fsfu.seenPong
	fsfu.mu.Unlock()
	if !seen {
		t.Fatal("client did not answer ping with pong")
	}

	fsfu.mu.Lock()
	fsfu.reject = true
	fsfu.mu.Unlock()
	// A command with no payload fields is still one JSON object, so the fake
	// SFU parses it and its error frame maps to ErrSignalingRejected.
	if _, err := client.command(context.Background(), "update-media-modifiers", nil); !errors.Is(err, ErrSignalingRejected) {
		t.Fatalf("got %v, want ErrSignalingRejected", err)
	}

	// Closing the socket fails pending and future commands.
	fsfu.mu.Lock()
	_ = fsfu.conn.Close()
	fsfu.mu.Unlock()
	select {
	case <-client.Closed():
	case <-time.After(2 * time.Second):
		t.Fatal("closed channel not signalled")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := client.command(ctx, "hangup", nil); err == nil {
		t.Fatal("expected closed error")
	}
}

func TestSignalingURL(t *testing.T) {
	got, err := signalingURL("wss://calls.example.okcdn.ru/fb?token=t&userId=1", "peer1")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"platform=WEB", "appVersion=1.1", "version=5", "capabilities=2F7F", "tgt=join", "peerId=peer1"} {
		if !strings.Contains(got, want) {
			t.Fatalf("url %q missing %q", got, want)
		}
	}
	if _, err := signalingURL("https://calls.example/", ""); err == nil {
		t.Fatal("expected endpoint error")
	}
}

// frameHeader is the first header of a client frame as it arrives on the wire.
type frameHeader struct {
	fin    bool
	opcode byte
	length uint64
}

// readFrameHeader parses one raw WebSocket frame header (RFC 6455 5.2):
// gorilla's own reader would join continuation frames and hide a split.
func readFrameHeader(r io.Reader) (frameHeader, error) {
	var b [2]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return frameHeader{}, err
	}
	h := frameHeader{fin: b[0]&0x80 != 0, opcode: b[0] & 0x0f, length: uint64(b[1] & 0x7f)}
	switch h.length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return frameHeader{}, err
		}
		h.length = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return frameHeader{}, err
		}
		h.length = binary.BigEndian.Uint64(ext[:])
	}
	return h, nil
}

// TestSignalingLargeCommandIsOneFrame pins the wire shape of a large command.
// The SFU parses each text frame on its own: an accept-producer description
// (~50 KB) split into 4 KiB continuation frames came back as invalid-request
// "Invalid message format" with no sequence (live engine smoke, 2026-09-28),
// while the probe's single frame with the same bytes was accepted.
func TestSignalingLargeCommandIsOneFrame(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	headers := make(chan frameHeader, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		raw := conn.NetConn()
		_ = raw.SetReadDeadline(time.Now().Add(5 * time.Second))
		header, err := readFrameHeader(raw)
		if err != nil {
			close(headers)
			return
		}
		headers <- header
		_ = conn.WriteMessage(websocket.TextMessage,
			[]byte(`{"stamp":0,"sequence":1,"response":"accept-producer","type":"response"}`))
		_, _ = io.Copy(io.Discard, raw)
	}))
	defer server.Close()

	client, err := dialSignaling(context.Background(), "ws"+server.URL[4:]+"?token=t&userId=42", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.close()

	payload := map[string]any{fieldDescription: strings.Repeat("a=rtcp-fb:96 nack pli\r\n", 3000)}
	want, err := orderedCommandFrame("accept-producer", 1, payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.command(context.Background(), "accept-producer", payload); err != nil {
		t.Fatal(err)
	}
	header, ok := <-headers
	if !ok {
		t.Fatal("server read no frame header")
	}
	if !header.fin || header.opcode != websocket.TextMessage || header.length != uint64(len(want)) {
		t.Fatalf("accept-producer of %d bytes went out as fin=%v opcode=%d length=%d; want one text frame",
			len(want), header.fin, header.opcode, header.length)
	}

	// A command larger than the write buffer is refused before any byte goes
	// out, never split.
	huge := map[string]any{fieldDescription: strings.Repeat("x", signalingWriteBuffer)}
	if _, err := client.command(context.Background(), "accept-producer", huge); !errors.Is(err, ErrSignalingFrameTooLarge) {
		t.Fatalf("oversized command: got %v, want ErrSignalingFrameTooLarge", err)
	}
}

// TestOrderedCommandFrameIsJSON pins the frame to one JSON object whatever
// the payload holds: none, no fields, or fields.
func TestOrderedCommandFrameIsJSON(t *testing.T) {
	for _, payload := range []any{nil, map[string]any{}, map[string]any{"a": 1}} {
		raw, err := orderedCommandFrame("x", 2, payload)
		if err != nil {
			t.Fatal(err)
		}
		if !json.Valid(raw) || !strings.HasPrefix(string(raw), `{"command":"x","sequence":2`) {
			t.Fatalf("payload %v: frame %s", payload, raw)
		}
	}
	if _, err := orderedCommandFrame("x", 2, []int{1}); !errors.Is(err, ErrSignalingPayload) {
		t.Fatalf("array payload: got %v, want ErrSignalingPayload", err)
	}
}
