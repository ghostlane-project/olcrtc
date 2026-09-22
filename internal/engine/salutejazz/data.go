package salutejazz

import (
	"context"
	"fmt"
	"time"

	"github.com/livekit/protocol/livekit"
	"github.com/pion/webrtc/v4"
	"google.golang.org/protobuf/proto"

	"github.com/openlibrecommunity/olcrtc/internal/engine"
	"github.com/openlibrecommunity/olcrtc/internal/logger"
)

// The data plane. Both lanes carry LiveKit data packets: a payload is
// wrapped in a UserPacket, the packet is marshalled and written to a
// publisher channel, and the SFU relays it to the room stamped with this
// participant's identity.
//
// Which channel a lane uses is the finding the engine is built around: the
// SFU drops what is written to the channels it created on the subscriber
// peer connection, so this engine sends on the publisher's channels and
// receives on the subscriber's.

const (
	// datagramPublishTopic marks the lossy lane. It is the topic the other
	// engines here publish datagrams under, so a room can hold a LiveKit
	// peer and a SaluteJazz peer of the same tunnel.
	datagramPublishTopic = "olcrtc.udp"

	// peerPollInterval is how often WaitForPeer looks at the room. The
	// roster and the first frame both arrive on other goroutines.
	peerPollInterval = 50 * time.Millisecond
)

// The capability interfaces the transport layer type-asserts on. A session
// that carries bytes for the tunnel has to satisfy every one of them: the
// datachannel transport reaches SendTo, SendDatagram and the peer binding
// only through these.
var (
	_ engine.Session             = (*Session)(nil)
	_ engine.PeerSession         = (*Session)(nil)
	_ engine.DatagramSession     = (*Session)(nil)
	_ engine.PeerDatagramSession = (*Session)(nil)
	_ engine.PeerReadySession    = (*Session)(nil)
	_ engine.PeerIdentity        = (*Session)(nil)
	_ engine.PeerResetter        = (*Session)(nil)
)

// Send publishes one payload on the reliable lane, to the whole room.
func (s *Session) Send(data []byte) error {
	return s.publish(data, "", nil, true)
}

// SendTo publishes one payload on the reliable lane, to a single
// participant. An empty peerID addresses the room, as every engine here
// treats it.
func (s *Session) SendTo(peerID string, data []byte) error {
	return s.publish(data, "", destinations(peerID), true)
}

// SendDatagram publishes one payload on the lossy lane, to the whole room.
// The lane is unordered and not retransmitted: a packet that cannot go now
// is worth nothing later, so a lane that is not up refuses it.
func (s *Session) SendDatagram(data []byte) error {
	return s.publish(data, datagramPublishTopic, nil, false)
}

// SendDatagramTo publishes one payload on the lossy lane, to a single
// participant.
func (s *Session) SendDatagramTo(peerID string, data []byte) error {
	return s.publish(data, datagramPublishTopic, destinations(peerID), false)
}

// destinations is the address list one peer id makes. LiveKit reads an empty
// list as the whole room.
func destinations(peerID string) []string {
	if peerID == "" {
		return nil
	}
	return []string{peerID}
}

// publish writes one data packet to the publisher channel of a lane.
//
// A closed session is over and says so; a generation that is gone, or has
// not negotiated this lane, is a lane that is not there right now, which a
// reconnect may still bring back. Either way nothing is written to a
// generation that has been torn down. The window between that check and the
// write closes itself: teardown closes the peer connections, and pion then
// refuses the write with an error of its own.
func (s *Session) publish(payload []byte, topic string, dest []string, reliable bool) error {
	if s.closed.Load() {
		return ErrSessionClosed
	}
	gen := s.current()
	if gen == nil || gen.isDone() {
		return ErrNoDataChannel
	}
	dc := publisherChannelOf(gen, reliable)
	if dc == nil || dc.ReadyState() != webrtc.DataChannelStateOpen {
		return ErrNoDataChannel
	}
	frame, err := proto.Marshal(dataPacket(payload, topic, dest, reliable))
	if err != nil {
		return fmt.Errorf("salutejazz marshal data packet: %w", err)
	}
	if err := dc.Send(frame); err != nil {
		return fmt.Errorf("salutejazz data channel send: %w", err)
	}
	return nil
}

// dataPacket builds the packet the SFU relays. The service runs LiveKit
// 1.5.3, which reads the kind and the destinations off the fields that
// later versions deprecated; the current fields are filled in too, so the
// same frame reaches a newer server unchanged.
func dataPacket(payload []byte, topic string, dest []string, reliable bool) *livekit.DataPacket {
	user := &livekit.UserPacket{
		Payload:               payload,
		DestinationIdentities: dest,
	}
	if topic != "" {
		user.Topic = proto.String(topic)
	}
	kind := livekit.DataPacket_LOSSY
	if reliable {
		kind = livekit.DataPacket_RELIABLE
	}
	return &livekit.DataPacket{
		Kind:                  kind,
		DestinationIdentities: dest,
		Value:                 &livekit.DataPacket_User{User: user},
	}
}

// receiveOn attaches the receive path to one channel of the subscriber peer
// connection. Only those carry inbound traffic: what this session publishes
// goes out on the publisher's channels and never comes back on them.
func (s *Session) receiveOn(gen *generation, dc *webrtc.DataChannel) {
	dc.OnMessage(func(msg webrtc.DataChannelMessage) {
		s.handleDataPacket(gen, msg.Data)
	})
}

// handleDataPacket routes one relayed packet by topic: the datagram topic
// feeds the lossy lane, everything else the byte stream. A packet that
// arrives on a generation that has been torn down belongs to a connection
// this session has already replaced, and is dropped.
func (s *Session) handleDataPacket(gen *generation, frame []byte) {
	if s.closed.Load() || gen.isDone() {
		return
	}
	var packet livekit.DataPacket
	if err := proto.Unmarshal(frame, &packet); err != nil {
		logger.Debugf("salutejazz: data packet: %v", err)
		return
	}
	user := packet.GetUser()
	if user == nil {
		// Speaker updates, transcriptions, chat and the rest of what the
		// room puts on these channels. None of it is ours.
		return
	}
	sender, byIdentity := senderIdentity(&packet)
	if sender != "" && sender == gen.localIdentity() {
		// Our own payload, relayed back to us. Nothing in the room should
		// send it, but a tunnel that reads its own bytes answers its own
		// handshake, so a packet stamped with this session's identity is
		// dropped wherever it came from.
		return
	}
	if byIdentity {
		// Only an identity belongs in the roster. The sid fallback below
		// names the same participant under an id no packet can be addressed
		// to, and a roster entry under it would hand WaitForPeer and every
		// caller of the peer list a participant they cannot reach.
		gen.notePeer(sender)
	}
	if user.GetTopic() == datagramPublishTopic {
		s.deliver(sender, user.GetPayload(), s.onPeerDatagram, s.onDatagram)
		return
	}
	s.deliver(sender, user.GetPayload(), s.onPeerData, s.onData)
}

// deliver hands one payload to the lane's callbacks: the per-peer one when
// the room named a sender and the upper layer routes by peer, the plain one
// otherwise. Payload bytes are never logged.
func (s *Session) deliver(sender string, payload []byte, toPeer func(string, []byte), plain func([]byte)) {
	switch {
	case toPeer != nil && sender != "":
		toPeer(sender, payload)
	case plain != nil:
		plain(payload)
	}
}

// senderIdentity is who sent one data packet, and whether the room named
// them by identity. LiveKit 1.5.3, which the service runs, stamps the user
// packet and falls back to the sid for a participant it has no identity for;
// later versions stamp the packet around it instead. The sid names the same
// participant, so it is reported as the sender, but it is not an identity
// and the roster does not take it.
func senderIdentity(packet *livekit.DataPacket) (string, bool) {
	user := packet.GetUser()
	if identity := user.GetParticipantIdentity(); identity != "" { //nolint:staticcheck // 1.5.3 wire
		return identity, true
	}
	if sid := user.GetParticipantSid(); sid != "" { //nolint:staticcheck // 1.5.3 wire
		return sid, false
	}
	identity := packet.GetParticipantIdentity()
	return identity, identity != ""
}

// CanSend reports whether the reliable lane is open.
func (s *Session) CanSend() bool {
	return !s.closed.Load() && laneOpen(s.publisherChannel(true))
}

// DatagramCanSend reports whether the lossy lane is open. It negotiates
// alongside the reliable one and can lag it by a moment.
func (s *Session) DatagramCanSend() bool {
	return !s.closed.Load() && laneOpen(s.publisherChannel(false))
}

// SubscriberCanSend reports whether the subscriber PC is connected. Unlike
// CanSend it does not wait for the publisher PC, which negotiates second.
func (s *Session) SubscriberCanSend() bool {
	return !s.closed.Load() && s.subscriberConnected()
}

// GetBufferedAmount is what both publisher channels still hold.
func (s *Session) GetBufferedAmount() uint64 {
	var buffered uint64
	for _, dc := range []*webrtc.DataChannel{s.publisherChannel(true), s.publisherChannel(false)} {
		if dc != nil {
			buffered += dc.BufferedAmount()
		}
	}
	return buffered
}

// publisherChannel returns the live publisher channel of one lane. A
// generation that is gone has no lane, whatever state its channels are still
// in: teardown ends the generation before pion closes them, and in that
// window publish already refuses. Everything that reports on a lane reads it
// through here, so the predicates and the write agree.
func (s *Session) publisherChannel(reliable bool) *webrtc.DataChannel {
	gen := s.current()
	if gen == nil || gen.isDone() {
		return nil
	}
	return publisherChannelOf(gen, reliable)
}

func publisherChannelOf(gen *generation, reliable bool) *webrtc.DataChannel {
	if reliable {
		return gen.pubRel.Load()
	}
	return gen.pubLossy.Load()
}

func laneOpen(dc *webrtc.DataChannel) bool {
	return dc != nil && dc.ReadyState() == webrtc.DataChannelStateOpen
}

// LocalPeerID is the identity this session is addressed by, which is the
// participantId its join-response named. The SFU issues a fresh one per
// connection, so it changes under a rejoin and is empty until the connector
// has answered a join.
func (s *Session) LocalPeerID() string { return s.localIdentity() }

// ConfirmPeer binds this session to the one remote identity the tunnel
// handshake authenticated.
//
// The binding belongs to the connection attempt that is live when it is
// made, and a rejoin does not carry it over: the SFU hands every connection
// fresh participant ids, so an identity confirmed under the previous attempt
// names nobody under the next one. The upper layer runs its handshake again
// after a reconnect and confirms again.
func (s *Session) ConfirmPeer(peerID string) error {
	if peerID == "" {
		return fmt.Errorf("%w: empty identity", engine.ErrInvalidPeerID)
	}
	gen := s.current()
	if gen == nil || gen.isDone() {
		return ErrSessionClosed
	}
	storeString(&gen.confirmed, peerID)
	return nil
}

// ResetPeer drops the confirmed binding, after an upper-layer handshake
// failure. The room roster is left alone: who is in the room is the
// connector's statement, not this session's.
func (s *Session) ResetPeer() {
	if gen := s.current(); gen != nil {
		gen.confirmed.Store(nil)
	}
}

// WaitForPeer blocks until someone else is in the room: the roster has named
// a participant, a data packet has arrived from one, or the handshake has
// confirmed one.
func (s *Session) WaitForPeer(ctx context.Context) error {
	ticker := time.NewTicker(peerPollInterval)
	defer ticker.Stop()
	for {
		if s.hasPeer() {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("salutejazz wait for peer: %w", ctx.Err())
		case <-s.closeCh:
			return ErrSessionClosed
		case <-ticker.C:
		}
	}
}

// hasPeer reports whether the live connection attempt knows of anyone else.
func (s *Session) hasPeer() bool {
	gen := s.current()
	if gen == nil {
		return false
	}
	return loadString(&gen.confirmed) != "" || gen.hasRemote()
}

// hasRemote reports whether anyone else is in the room, without building the
// list: WaitForPeer asks on a poll.
func (g *generation) hasRemote() bool {
	g.peersMu.Lock()
	defer g.peersMu.Unlock()
	return len(g.peers) > 0
}

// notePeer records a sender the roster has not named yet, so the first
// packet from a participant counts as that participant appearing. The
// roster update that follows fills in its sid, and the one that reports it
// gone removes it again.
func (g *generation) notePeer(identity string) {
	if identity == "" || identity == g.localIdentity() {
		return
	}
	g.peersMu.Lock()
	defer g.peersMu.Unlock()
	if _, known := g.peers[identity]; !known {
		g.peers[identity] = ""
	}
}
