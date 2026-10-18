// Package max provides the engine for VK MAX calls. The signaling rides
// a JSON WebSocket (not the main messaging protocol's binary MessagePack —
// the calls endpoint speaks plain JSON frames).
package max

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// sigMessage is one JSON frame on the calls WebSocket.
type sigMessage struct {
	Type string          `json:"type,omitempty"`
	Data json.RawMessage `json:"data,omitempty"`
}

// notification is the SFU's initial assignment (peerId, TURN, limits).
type notification struct {
	Stamp  int64 `json:"stamp"`
	PeerID struct {
		ID int64 `json:"id"`
	} `json:"peerId"`
	Endpoint           string `json:"endpoint"`
	ConversationParams struct {
		TURN struct {
			URLs       []string `json:"urls"`
			Username   string   `json:"username"`
			Credential string   `json:"credential"`
		} `json:"turn"`
	} `json:"conversationParams"`
}

// sdpMessage carries an offer or answer.
type sdpMessage struct {
	Type string `json:"type"`
	SDP  string `json:"sdp"`
}

// signaling is the JSON WebSocket connection to the SFU.
type signaling struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

// newSignaling wraps an open WebSocket.
func newSignaling(conn *websocket.Conn) *signaling {
	return &signaling{conn: conn}
}

// send writes one JSON message.
func (s *signaling) send(v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.WriteJSON(v)
}

// sendPing keeps the connection alive (the SFU sends "ping" strings; we
// send them back the same way).
func (s *signaling) sendPing() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.WriteMessage(websocket.TextMessage, []byte("ping"))
}

// recv reads the next JSON message.
func (s *signaling) recv() (*sigMessage, error) {
	var msg sigMessage
	if err := s.conn.ReadJSON(&msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

// setDeadline bounds the next read.
func (s *signaling) setDeadline(d time.Duration) {
	_ = s.conn.SetReadDeadline(time.Now().Add(d))
}

// close shuts the signaling down.
func (s *signaling) close() error {
	return s.conn.Close()
}
