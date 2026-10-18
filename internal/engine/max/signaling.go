// Package max provides the engine for VK MAX calls. The signaling rides
// two protocols on the same WebSocket (wss://videowebrtc.okcdn.ru/ws2):
// JSON text frames for control commands (accept-call, change-media-settings)
// and binary MessagePack frames for the SDP exchange (TRANSMIT_DATA), the
// same binary protocol the ok-calls SDK speaks.
package max

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

const (
	// protoVersion is the binary protocol's version byte.
	protoVersion = 10
	// headerSize is the binary header: version, cmd, seq(2), opcode(2), chunks, len(3).
	headerSize = 10
	// cmdSignaling is the command byte for signaling messages.
	cmdSignaling = 1
	// opTransmitData is the opcode for TRANSMIT_DATA (carries SDP).
	opTransmitData = 6
)

// sigMessage is one JSON control frame.
type sigMessage struct {
	Type     string          `json:"type,omitempty"`
	Command  string          `json:"command,omitempty"`
	Sequence int             `json:"sequence,omitempty"`
	Data     json.RawMessage `json:"data,omitempty"`
}

// transmitData is the binary TRANSMIT_DATA payload: the SDP exchange.
type transmitData struct {
	Name   string `msgpack:"name"`
	Params struct {
		ParticipantID int `msgpack:"participantId"`
		Data          struct {
			SDP  string `msgpack:"sdp"`
			Type string `msgpack:"type"`
		} `msgpack:"data"`
	} `msgpack:"params"`
}

// notification is the SFU's initial assignment.
type notification struct {
	Stamp  int64 `json:"stamp"`
	PeerID struct {
		ID int64 `json:"id"`
	} `json:"peerId"`
}

// sdpExchange carries one SDP offer or answer.
type sdpExchange struct {
	Type string `json:"type" msgpack:"type"`
	SDP  string `json:"sdp" msgpack:"sdp"`
}

// signaling is the dual-protocol WebSocket connection to the SFU.
type signaling struct {
	conn *websocket.Conn
	mu   sync.Mutex
	seq  int
}

// newSignaling wraps an open WebSocket.
func newSignaling(conn *websocket.Conn) *signaling {
	return &signaling{conn: conn}
}

// sendJSON writes one JSON control command.
func (s *signaling) sendJSON(v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.WriteJSON(v)
}

// sendSDP writes one transmit-data command carrying an SDP offer or answer,
// the JSON shape the calls SDK serializes: {command, sequence, ...params}.
func (s *signaling) sendSDP(sequence int, participantID int, sdp, sdpType string) error {
	msg := map[string]any{
		"command":  "transmit-data",
		"sequence": sequence,
	}
	if participantID != 0 {
		msg["participantId"] = participantID
	}
	msg["data"] = map[string]string{
		"sdp":  sdp,
		"type": sdpType,
	}
	return s.sendJSON(msg)
}

// sendPong answers the SFU's raw-text keepalive.
func (s *signaling) sendPong() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.WriteMessage(websocket.TextMessage, []byte("pong"))
}

// recv reads the next message (JSON text or binary).
func (s *signaling) recv() (*sigMessage, *transmitData, error) {
	msgType, data, err := s.conn.ReadMessage()
	if err != nil {
		return nil, nil, err
	}
	switch msgType {
	case websocket.TextMessage:
		var msg sigMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			// Raw "ping" string
			if string(data) == "ping" {
				return &sigMessage{Type: "ping"}, nil, nil
			}
			return nil, nil, nil
		}
		return &msg, nil, nil
	case websocket.BinaryMessage:
		if len(data) < headerSize {
			return nil, nil, fmt.Errorf("short binary frame: %d", len(data))
		}
		opcode := binary.BigEndian.Uint16(data[4:6])
		payload := data[headerSize:]
		var td transmitData
		if err := msgpack.Unmarshal(payload, &td); err != nil {
			return nil, nil, fmt.Errorf("unpack opcode=%d: %w", opcode, err)
		}
		return nil, &td, nil
	}
	return nil, nil, nil
}

// setDeadline bounds the next read.
func (s *signaling) setDeadline(d time.Duration) {
	_ = s.conn.SetReadDeadline(time.Now().Add(d))
}

// close shuts the signaling down.
func (s *signaling) close() error {
	return s.conn.Close()
}
