// Package max provides the engine for VK MAX calls (olcrtc carrier).
//
// The signaling is the ok-calls JSON protocol over the join endpoint's
// WebSocket: {command, sequence, ...params} frames, raw "ping"/"pong"
// keepalive, and the participant-to-participant SDP/ICE relay carried by
// transmit-data frames. The tunnel rides the negotiated datachannel of a
// P2P (DIRECT-topology) call - the megabit path measured against the live
// service (see docs: 2026-10-18-olcrtc-max-carrier-design).
package max

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/gorilla/websocket"
)

// sigMessage is one JSON signaling envelope.
type sigMessage struct {
	Type     string          `json:"type,omitempty"`
	Command  string          `json:"command,omitempty"`
	Sequence int             `json:"sequence,omitempty"`
	Data     json.RawMessage `json:"data,omitempty"`
}

// connectionNotification is the SFU's first assignment frame.
type connectionNotification struct {
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
		STUN struct {
			URLs []string `json:"urls"`
		} `json:"stun"`
	} `json:"conversationParams"`
	Conversation struct {
		ID       string `json:"id"`
		Topology string `json:"topology"`
	} `json:"conversation"`
}

// responseFrame is a command acknowledgement; accept-call's carries the
// other participants.
type responseFrame struct {
	Response       string  `json:"response"`
	Sequence       int     `json:"sequence"`
	ParticipantIDs []int64 `json:"participantIds"`
	Error          string  `json:"error"`
	Message        string  `json:"message"`
	Notification   string  `json:"notification"`
	Description    string  `json:"description"`
}

// transmitPayload is the data field of a transmit-data frame: either an
// SDP exchange or one trickled ICE candidate.
type transmitPayload struct {
	SDP *struct {
		Type string `json:"type"`
		SDP  string `json:"sdp"`
	} `json:"sdp,omitempty"`
	Candidate *struct {
		Candidate     string  `json:"candidate"`
		SDPMid        *string `json:"sdpMid"`
		SDPMLineIndex *uint16 `json:"sdpMLineIndex"`
	} `json:"candidate,omitempty"`
}

// signaling is the JSON WebSocket to the SFU.
type signaling struct {
	conn *websocket.Conn
	mu   sync.Mutex
	seq  int
}

func newSignaling(conn *websocket.Conn) *signaling {
	return &signaling{conn: conn}
}

// sendCommand writes one JSON command with the next sequence number.
func (s *signaling) sendCommand(command string, params map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	msg := map[string]any{"command": command, "sequence": s.seq}
	for k, v := range params {
		msg[k] = v
	}
	return s.conn.WriteJSON(msg)
}

// sendTransmit relays one payload to a participant: the P2P SDP and ICE
// channel. The server caps a relayed message at ~3.5KB - candidates ride
// one per frame, never packed.
func (s *signaling) sendTransmit(participantID int64, data map[string]any) error {
	return s.sendCommand("transmit-data", map[string]any{
		"participantId": participantID,
		"data":          data,
	})
}

// sendSDPExchange relays one offer or answer.
func (s *signaling) sendSDPExchange(participantID int64, sdpType, sdp string) error {
	return s.sendTransmit(participantID, map[string]any{
		"sdp":            map[string]string{"type": sdpType, "sdp": sdp},
		"animojiVersion": 1,
	})
}

// sendCandidate relays one ICE candidate; an empty candidate string marks
// the end of gathering.
func (s *signaling) sendCandidate(participantID int64, init webrtcCandidate) error {
	cand := map[string]any{"candidate": init.Candidate}
	if init.SDPMid != nil {
		cand["sdpMid"] = *init.SDPMid
	}
	if init.SDPMLineIndex != nil {
		cand["sdpMLineIndex"] = *init.SDPMLineIndex
	}
	return s.sendTransmit(participantID, map[string]any{"candidate": cand})
}

// sendPong answers the raw-text keepalive.
func (s *signaling) sendPong() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.WriteMessage(websocket.TextMessage, []byte("pong"))
}

// recv reads the next JSON message; a raw ping comes back as a sigMessage
// with Type "ping".
func (s *signaling) recv() (*sigMessage, *responseFrame, error) {
	msgType, data, err := s.conn.ReadMessage()
	if err != nil {
		return nil, nil, err
	}
	switch msgType {
	case websocket.TextMessage:
		if string(data) == "ping" {
			return &sigMessage{Type: "ping"}, nil, nil
		}
		var m sigMessage
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, nil, fmt.Errorf("parse %dB frame: %w", len(data), err)
		}
		var r responseFrame
		_ = json.Unmarshal(data, &r)
		return &m, &r, nil
	}
	return nil, nil, nil
}

// close shuts the signaling socket.
func (s *signaling) close() error {
	return s.conn.Close()
}
