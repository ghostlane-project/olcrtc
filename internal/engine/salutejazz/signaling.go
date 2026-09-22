package salutejazz

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/internal/protect"
)

// envOut is a client frame. The field order is the official web client's:
// roomId, payload, event, groupId, requestId. groupId is absent on the join
// and echoed on everything after it.
type envOut struct {
	RoomID    string          `json:"roomId"` //nolint:tagliatelle // connector wire is camelCase
	Payload   json.RawMessage `json:"payload,omitempty"`
	Event     string          `json:"event"`
	GroupID   string          `json:"groupId,omitempty"` //nolint:tagliatelle // connector wire is camelCase
	RequestID string          `json:"requestId"`         //nolint:tagliatelle // connector wire is camelCase
}

// envIn is a server frame.
type envIn struct {
	Event         string          `json:"event"`
	RequestID     string          `json:"requestId"`     //nolint:tagliatelle // connector wire is camelCase
	RoomID        string          `json:"roomId"`        //nolint:tagliatelle // connector wire is camelCase
	GroupID       string          `json:"groupId"`       //nolint:tagliatelle // connector wire is camelCase
	ParticipantID string          `json:"participantId"` //nolint:tagliatelle // connector wire is camelCase
	Payload       json.RawMessage `json:"payload"`
}

// mediaIn is a client media payload: the method first, then the one field
// that method carries, in the order the web client writes them.
type mediaIn struct {
	Method      string         `json:"method"`
	Description *sdpOut        `json:"description,omitempty"`
	Candidates  []iceCandidate `json:"rtcIceCandidates,omitempty"` //nolint:tagliatelle // connector wire is camelCase
	PingReq     *pingRequest   `json:"ping_req,omitempty"`
}

// sdpOut is a description this client sends. The server sends the same two
// fields the other way round.
type sdpOut struct {
	SDP  string `json:"sdp"`
	Type string `json:"type"`
}

// pingRequest carries the last measured round trip, as the web client's does.
type pingRequest struct {
	Timestamp int64 `json:"timestamp"`
	RTT       int64 `json:"rtt"`
}

// joinRequest is the join payload, field for field as the web client sends
// it. It carries the room password and is never logged.
type joinRequest struct {
	Password          string            `json:"password"`
	ParticipantName   string            `json:"participantName"`   //nolint:tagliatelle // connector wire is camelCase
	SupportedFeatures supportedFeatures `json:"supportedFeatures"` //nolint:tagliatelle // connector wire is camelCase
	IsSilent          bool              `json:"isSilent"`          //nolint:tagliatelle // connector wire is camelCase
}

// supportedFeatures is what the web client claims it can handle. None of it
// matters to a session that only carries bytes; the shape does.
type supportedFeatures struct {
	AttachedRooms  bool `json:"attachedRooms"` //nolint:tagliatelle // connector wire is camelCase
	SessionGroups  bool `json:"sessionGroups"` //nolint:tagliatelle // connector wire is camelCase
	Transcription  bool `json:"transcription"`
	Interpretation bool `json:"interpretation"`
}

// mediaPayload is a media-out payload: one LiveKit signal method plus the
// field that method carries.
type mediaPayload struct {
	Method           string              `json:"method"`
	Configuration    *rtcConfiguration   `json:"configuration"`
	Description      *sdpDescription     `json:"description"`
	RTCIceCandidates []iceCandidate      `json:"rtcIceCandidates"` //nolint:tagliatelle // connector wire is camelCase
	PongResp         *pongResponse       `json:"pong_resp"`
	Update           *participantsUpdate `json:"update"`
}

// participantsUpdate is what rtc:participants:update carries: the room's
// roster, under an "update" object.
type participantsUpdate struct {
	Participants []participant `json:"participants"`
}

// participant is the part of a roster entry this engine reads. identity is
// what a data packet is addressed to and what the sender is stamped with;
// state says whether the participant is still in the room.
type participant struct {
	SID      string `json:"sid"`
	Identity string `json:"identity"`
	State    string `json:"state"`
}

type rtcConfiguration struct {
	ICEServers []iceServer `json:"iceServers"` //nolint:tagliatelle // connector wire is camelCase
}

// iceServer is one entry of rtc:config. The service issues a TURN credential
// per join, valid for about a day.
type iceServer struct {
	URLs       []string `json:"urls"`
	Username   string   `json:"username"`
	Credential string   `json:"credential"`
}

type sdpDescription struct {
	Type string `json:"type"`
	SDP  string `json:"sdp"`
}

// iceCandidate is one trickled candidate, in both directions. The server
// sends an sdpMLineIndex without an sdpMid; both sides carry the target the
// candidate belongs to. pion keeps the ufrag inside the candidate line and
// leaves the field empty, so it is omitted rather than sent as null.
type iceCandidate struct {
	Candidate        string  `json:"candidate"`
	SDPMid           *string `json:"sdpMid"`                     //nolint:tagliatelle // connector wire is camelCase
	SDPMLineIndex    *uint16 `json:"sdpMLineIndex"`              //nolint:tagliatelle // connector wire is camelCase
	UsernameFragment *string `json:"usernameFragment,omitempty"` //nolint:tagliatelle // connector wire is camelCase
	Target           string  `json:"target"`
}

// pongResponse answers a ping. Both timestamps are milliseconds, quoted the
// way protobuf's JSON mapping quotes an int64.
type pongResponse struct {
	LastPingTimestamp json.RawMessage `json:"lastPingTimestamp"` //nolint:tagliatelle // connector wire is camelCase
	Timestamp         json.RawMessage `json:"timestamp"`
}

// joinResponse is the subset of join-response this engine needs: who we are,
// and the LiveKit room name every later frame has to echo.
type joinResponse struct {
	MeetingID   string `json:"meetingId"` //nolint:tagliatelle // connector wire is camelCase
	Participant struct {
		ParticipantID string `json:"participantId"` //nolint:tagliatelle // connector wire is camelCase
	} `json:"participant"`
	ParticipantGroup struct {
		GroupID string `json:"groupId"` //nolint:tagliatelle // connector wire is camelCase
	} `json:"participantGroup"` //nolint:tagliatelle // connector wire is camelCase
}

// serverError is the payload of an error frame.
type serverError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// dialWebSocket opens the connector socket and publishes it as the session's
// single writer target. The headers are the browser's: the service is
// fronted by a bot filter that sees them.
func (s *Session) dialWebSocket(gen *generation) (*websocket.Conn, error) {
	dialer := protect.NewWebSocketDialer(wsHandshakeTimeout, s.resolver)
	header := http.Header{}
	header.Set("Origin", webOrigin)
	header.Set("User-Agent", webUserAgent)

	conn, resp, err := dialer.Dial(s.signalingURL(), header)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		return nil, fmt.Errorf("salutejazz dial connector: %w", err)
	}
	conn.SetReadLimit(wsReadLimit)

	gen.wsMu.Lock()
	gen.ws = conn
	gen.wsMu.Unlock()
	return conn, nil
}

// closeSocket closes this generation's signaling socket. Clearing the
// pointer under wsMu makes every later write fail with ErrWebSocketClosed
// instead of writing to a dead socket.
func (g *generation) closeSocket() {
	g.wsMu.Lock()
	conn := g.ws
	g.ws = nil
	g.wsMu.Unlock()
	if conn == nil {
		return
	}
	_ = conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(time.Second))
	_ = conn.Close()
}

// writeJSON is the single writer path: it owns wsMu (gorilla permits one
// concurrent writer) and bounds the write, so a black-holed socket cannot
// hold the lock forever and take every other writer with it. It writes to
// the socket this generation dialled and to no other.
func (g *generation) writeJSON(v any) error {
	g.wsMu.Lock()
	defer g.wsMu.Unlock()
	if g.ws == nil {
		return ErrWebSocketClosed
	}
	_ = g.ws.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
	if err := g.ws.WriteJSON(v); err != nil {
		return fmt.Errorf("salutejazz ws write: %w", err)
	}
	return nil
}

// envelope wraps one payload in the client envelope: the room, the group id
// the connector expects echoed once it has named one, and a fresh request id.
func (s *Session) envelope(gen *generation, event string, payload any) (envOut, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return envOut{}, fmt.Errorf("salutejazz marshal %s payload: %w", event, err)
	}
	return envOut{
		RoomID:    s.roomID(),
		Payload:   raw,
		Event:     event,
		GroupID:   gen.groupID(),
		RequestID: uuid.NewString(),
	}, nil
}

// sendEnvelope wraps one payload in the client envelope and writes it.
func (s *Session) sendEnvelope(gen *generation, event string, payload any) error {
	frame, err := s.envelope(gen, event, payload)
	if err != nil {
		return err
	}
	return gen.writeJSON(frame)
}

// sendMedia writes one media-in frame carrying a LiveKit signal method.
func (s *Session) sendMedia(gen *generation, payload mediaIn) error {
	return s.sendEnvelope(gen, eventMediaIn, payload)
}

// sendJoin is the first frame on the socket. The payload mimics the web
// client's, and is the one frame that carries the room password: nothing
// here or below logs it.
func (s *Session) sendJoin(gen *generation) error {
	return s.sendEnvelope(gen, eventJoin, joinRequest{
		Password:        s.password(),
		ParticipantName: s.name,
		SupportedFeatures: supportedFeatures{
			AttachedRooms:  true,
			SessionGroups:  true,
			Transcription:  true,
			Interpretation: true,
		},
		IsSilent: false,
	})
}

// readLoop is the single reader. The connection is captured once: gorilla
// allows one concurrent reader, and a reconnect starts a fresh loop over the
// new socket while this one unwinds on its own read error.
func (s *Session) readLoop(gen *generation, conn *websocket.Conn) {
	for {
		_ = conn.SetReadDeadline(time.Now().Add(wsReadTimeout))
		var frame envIn
		if err := conn.ReadJSON(&frame); err != nil {
			// A generation that has been torn down closed this socket on
			// purpose - Close, or a Connect giving up. Only a link that
			// died under a live generation is worth a reconnect.
			if !s.closed.Load() && !gen.isDone() {
				logger.Debugf("salutejazz: signaling read: %v", err)
				s.queueReconnect()
			}
			return
		}
		s.handleEnvelope(gen, frame)
	}
}

func (s *Session) handleEnvelope(gen *generation, frame envIn) {
	if frame.GroupID != "" && gen.groupID() == "" {
		storeString(&gen.group, frame.GroupID)
	}
	switch frame.Event {
	case eventJoinResponse:
		s.handleJoinResponse(gen, frame.Payload)
	case eventMediaOut:
		s.handleMediaOut(gen, frame.Payload)
	case eventError:
		s.handleServerError(frame.Payload)
	default:
		logger.Debugf("salutejazz: event %s", frame.Event)
	}
}

// handleJoinResponse records who we are and the group id every later frame
// echoes.
func (s *Session) handleJoinResponse(gen *generation, payload json.RawMessage) {
	var resp joinResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		logger.Debugf("salutejazz: join-response: %v", err)
		return
	}
	if resp.Participant.ParticipantID != "" {
		storeString(&gen.identity, resp.Participant.ParticipantID)
	}
	if resp.ParticipantGroup.GroupID != "" {
		storeString(&gen.group, resp.ParticipantGroup.GroupID)
	}
	logger.Debugf("salutejazz: joined as %s in meeting %s", resp.Participant.ParticipantID, resp.MeetingID)
	s.notifyJoin(eventJoinResponse)
}

// handleMediaOut dispatches one LiveKit signal method.
func (s *Session) handleMediaOut(gen *generation, payload json.RawMessage) {
	var media mediaPayload
	if err := json.Unmarshal(payload, &media); err != nil {
		logger.Debugf("salutejazz: media-out: %v", err)
		return
	}
	switch media.Method {
	case methodConfig:
		s.notifyJoin(methodConfig)
		gen.applyICEConfig(media.Configuration)
	case methodOffer:
		s.notifyJoin(methodOffer)
		if err := s.handleOffer(gen, media.Description); err != nil {
			logger.Warnf("salutejazz: subscriber offer: %v", err)
			s.queueReconnect()
		}
	case methodAnswer:
		s.deliverAnswer(gen, media.Description)
	case methodICE:
		s.addRemoteICE(gen, media.RTCIceCandidates)
	case methodParticipants:
		if media.Update != nil {
			gen.applyParticipants(media.Update.Participants)
		}
	case methodPong:
		s.resolvePong(media.PongResp)
	default:
		logger.Debugf("salutejazz: media-out %s", media.Method)
	}
}

// deliverAnswer hands the SFU's answer to the publisher negotiation, which
// is the only thing waiting for one.
func (s *Session) deliverAnswer(gen *generation, desc *sdpDescription) {
	if desc == nil {
		return
	}
	select {
	case gen.answer <- desc.SDP:
	default:
		logger.Debugf("salutejazz: unexpected rtc:answer")
	}
}

// handleServerError reports what the connector refused. Which codes end a
// session for good is decided on the reconnect path.
func (s *Session) handleServerError(payload json.RawMessage) {
	var failure serverError
	if err := json.Unmarshal(payload, &failure); err != nil {
		logger.Warnf("salutejazz: server error frame: %v", err)
		return
	}
	logger.Warnf("salutejazz: server error %s: %s", failure.Code, failure.Message)
}

// notifyJoin reports one handshake frame to the observer a test installed.
func (s *Session) notifyJoin(event string) {
	if s.onJoinPayload != nil {
		s.onJoinPayload(event)
	}
}

// pingLoop keeps the session alive on the connector's own cadence.
func (s *Session) pingLoop(gen *generation) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if gen.groupID() == "" {
				// The join has not been answered yet, and every frame
				// after it echoes the group the answer names.
				continue
			}
			if err := s.ping(gen); err != nil {
				logger.Debugf("salutejazz: ping: %v", err)
				s.queueReconnect()
				return
			}
		case <-gen.done:
			return
		case <-s.closeCh:
			return
		}
	}
}

// pingOnce pings on the live generation.
func (s *Session) pingOnce() error {
	gen := s.current()
	if gen == nil {
		return ErrSessionClosed
	}
	return s.ping(gen)
}

// ping sends one rtc:ping and waits for the pong that answers it. The frame
// carries the last measured round trip, as the web client's does.
func (s *Session) ping(gen *generation) error {
	sent, waiter := s.awaitPong(time.Now().UnixMilli())
	defer s.forgetPong(sent)

	err := s.sendMedia(gen, mediaIn{
		Method:  methodPing,
		PingReq: &pingRequest{Timestamp: sent, RTT: s.rtt.Load()},
	})
	if err != nil {
		return err
	}
	timer := time.NewTimer(pongTimeout)
	defer timer.Stop()
	select {
	case <-waiter:
		s.rtt.Store(time.Now().UnixMilli() - sent)
		return nil
	case <-timer.C:
		return ErrPongTimeout
	case <-gen.done:
		return ErrSessionClosed
	case <-s.closeCh:
		return ErrSessionClosed
	}
}

// awaitPong registers a waiter for the ping about to go out and returns the
// timestamp to send it under: two pings in one millisecond would otherwise
// share a key, and the first waiter would never be released.
func (s *Session) awaitPong(sent int64) (int64, <-chan struct{}) {
	waiter := make(chan struct{})
	s.pongMu.Lock()
	defer s.pongMu.Unlock()
	for {
		if _, taken := s.pongWaiters[sent]; !taken {
			break
		}
		sent++
	}
	s.pongWaiters[sent] = waiter
	return sent, waiter
}

func (s *Session) forgetPong(sent int64) {
	s.pongMu.Lock()
	delete(s.pongWaiters, sent)
	s.pongMu.Unlock()
}

// resolvePong releases the ping this pong answers. A pong that names no ping
// we know of still proves the link, so it releases everything waiting.
func (s *Session) resolvePong(resp *pongResponse) {
	var sent int64
	if resp != nil {
		sent = parseMillis(resp.LastPingTimestamp)
	}
	s.pongMu.Lock()
	defer s.pongMu.Unlock()
	if waiter, ok := s.pongWaiters[sent]; ok {
		delete(s.pongWaiters, sent)
		close(waiter)
		return
	}
	for key, waiter := range s.pongWaiters {
		delete(s.pongWaiters, key)
		close(waiter)
	}
}

// parseMillis reads a millisecond timestamp the connector sends as a quoted
// int64, the way protobuf's JSON mapping renders one.
func parseMillis(raw json.RawMessage) int64 {
	text := strings.Trim(string(raw), `"`)
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0
	}
	return value
}
