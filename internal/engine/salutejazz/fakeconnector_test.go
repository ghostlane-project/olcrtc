package salutejazz

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/livekit/protocol/livekit"
	"github.com/pion/webrtc/v4"
	"google.golang.org/protobuf/proto"
)

// The in-process connector fake. It speaks the frames the capture of the
// official web client recorded, so the engine can be exercised without the
// live service: a join answered with join-response, media-out rtc:config,
// the subscriber offer the SFU itself makes, the answer to the client's
// publisher offer, trickled ICE for both, and rtc:pong.
//
// Test surface, all safe to call from a test goroutine:
//
//	newFakeConnector(t) -> (url, *fakeSFU)  the ws:// URL to join
//	(*fakeSFU).sawAnswer()                  the client's subscriber answer arrived
//	(*fakeSFU).sawPublisherOffer()          the client's publisher offer arrived
//	(*fakeSFU).joins()                      every join seen: its payload and group
//	(*fakeSFU).holdJoins() -> release       stall the join answer, for a race
//	(*fakeSFU).dropAll()                    close every fake-side socket
//	(*fakeSFU).sendError(code, message)     a server error frame to every peer
//	(*fakeSFU).lastError()                  what the fake itself tripped over,
//	                                        including a client frame whose
//	                                        group id does not match the capture
//
// What it deliberately does not do: no preconnect (that is the auth
// provider's HTTP call), no rtc:join, no participant bookkeeping beyond the
// identities a room's peers need to address each other.

var (
	errGroupOnJoin = errors.New("join carried a group id")
	errWrongGroup  = errors.New("frame carried the wrong group id")
)

const (
	// fakeConnectorPath is the connector endpoint, as on the real service.
	fakeConnectorPath = "/connector"
	// fakeSTUNURL is the single ICE server the fake advertises in
	// rtc:config. It needs no credentials and the fake's own candidates are
	// host candidates, so nothing in a test depends on reaching it.
	fakeSTUNURL = "stun:stun.l.google.com:19302"
)

// seenJoin is one join frame as it arrived: the payload the client built and
// the envelope group id it carried (the capture has none on a join).
type seenJoin struct {
	payload string
	group   string
}

type fakeSFU struct {
	mu       sync.Mutex
	peers    []*fakePeer
	rooms    map[string][]*fakePeer
	seq      int
	answered bool
	pubOffer bool
	failure  string
	seen     []seenJoin
	// joinGate, while non-nil, stalls every join answer.
	joinGate chan struct{}
}

func newFakeConnector(t *testing.T) (string, *fakeSFU) {
	t.Helper()
	fake := &fakeSFU{rooms: make(map[string][]*fakePeer)}
	// The engine sends the browser's Origin, which gorilla's default check
	// refuses against a loopback host.
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != fakeConnectorPath {
			http.NotFound(w, r)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		fake.serve(conn)
	}))
	t.Cleanup(func() {
		fake.shutdown()
		srv.Close()
	})
	return "ws" + strings.TrimPrefix(srv.URL, "http") + fakeConnectorPath, fake
}

// sawAnswer reports whether a client answered the subscriber offer.
func (f *fakeSFU) sawAnswer() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.answered
}

// sawPublisherOffer reports whether a client offered a publisher PC.
func (f *fakeSFU) sawPublisherOffer() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pubOffer
}

// joins is every join frame the fake has read, in arrival order.
func (f *fakeSFU) joins() []seenJoin {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]seenJoin(nil), f.seen...)
}

// holdJoins stalls every join answer until the returned release is called,
// so a test can hold a Connect in flight. Releasing twice is safe.
func (f *fakeSFU) holdJoins() func() {
	gate := make(chan struct{})
	f.mu.Lock()
	f.joinGate = gate
	f.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { close(gate) }) }
}

// awaitJoinGate blocks while holdJoins is in force.
func (f *fakeSFU) awaitJoinGate() {
	f.mu.Lock()
	gate := f.joinGate
	f.mu.Unlock()
	if gate == nil {
		return
	}
	<-gate
}

// lastError is what the fake itself failed on, for a test's failure message.
// The fake never calls t from its own goroutines: a websocket handler
// outlives the test that started it.
func (f *fakeSFU) lastError() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.failure
}

// dropAll closes every fake-side socket without a close frame, the way a
// dead link ends a session.
func (f *fakeSFU) dropAll() {
	for _, peer := range f.snapshot() {
		peer.closeSocket()
	}
}

// sendError delivers a server error frame to every connected peer. It is
// part of the fake's documented surface, driven by the reconnect and
// fatal-error cover.
//
//nolint:unused // driven by the reconnect and fatal-error tests
func (f *fakeSFU) sendError(code, message string) {
	for _, peer := range f.snapshot() {
		_ = peer.write(eventError, map[string]any{"code": code, "message": message})
	}
}

// shutdown ends every peer: sockets first, then their peer connections.
func (f *fakeSFU) shutdown() {
	f.dropAll()
	for _, peer := range f.snapshot() {
		peer.closePeerConnections()
	}
}

func (f *fakeSFU) snapshot() []*fakePeer {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*fakePeer(nil), f.peers...)
}

func (f *fakeSFU) fail(what string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failure == "" {
		f.failure = what + ": " + err.Error()
	}
}

// join registers a peer in its room and hands it a distinct identity, the
// way the SFU hands every participant its own participantId.
func (f *fakeSFU) join(peer *fakePeer, room string, frame fakeIn) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seen = append(f.seen, seenJoin{payload: string(frame.Payload), group: frame.GroupID})
	f.seq++
	peer.id = "fakepart-" + strconv.Itoa(f.seq)
	peer.room = room
	peer.group = "fakegroup-" + room
	f.peers = append(f.peers, peer)
	f.rooms[room] = append(f.rooms[room], peer)
}

func (f *fakeSFU) roomPeers(room string) []*fakePeer {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*fakePeer(nil), f.rooms[room]...)
}

func (f *fakeSFU) markAnswer() {
	f.mu.Lock()
	f.answered = true
	f.mu.Unlock()
}

func (f *fakeSFU) markPublisherOffer() {
	f.mu.Lock()
	f.pubOffer = true
	f.mu.Unlock()
}

// forward relays one data packet from its sender to the room, stamped with
// the sender's identity and honouring DestinationIdentities, the way the SFU
// relays what a publisher writes. Receivers get it on the channels the fake
// created on their subscriber PC.
func (f *fakeSFU) forward(from *fakePeer, frame []byte) {
	var packet livekit.DataPacket
	if err := proto.Unmarshal(frame, &packet); err != nil {
		f.fail("forward unmarshal", err)
		return
	}
	user := packet.GetUser()
	if user == nil {
		return
	}
	// LiveKit 1.5.3, which the service runs, carries the sender and the
	// destinations on the user packet, not on the data packet around it.
	user.ParticipantIdentity = from.id      //nolint:staticcheck // 1.5.3 wire
	user.ParticipantSid = from.id           //nolint:staticcheck // 1.5.3 wire
	dest := user.GetDestinationIdentities() //nolint:staticcheck // 1.5.3 wire
	out, err := proto.Marshal(&packet)
	if err != nil {
		f.fail("forward marshal", err)
		return
	}
	lossy := packet.GetKind() == livekit.DataPacket_LOSSY //nolint:staticcheck // 1.5.3 wire
	for _, peer := range f.roomPeers(from.room) {
		if peer == from || !addressed(dest, peer.id) {
			continue
		}
		peer.deliver(out, lossy)
	}
}

func addressed(dest []string, id string) bool {
	if len(dest) == 0 {
		return true
	}
	for _, want := range dest {
		if want == id {
			return true
		}
	}
	return false
}

// fakePeer is one connected client: its socket, the SFU side of its two peer
// connections and the channels the fake owns on each.
type fakePeer struct {
	fake *fakeSFU
	conn *websocket.Conn

	wsMu sync.Mutex

	id    string
	room  string
	group string

	mu               sync.Mutex
	subPC            *webrtc.PeerConnection
	pubPC            *webrtc.PeerConnection
	subRel, subLossy *webrtc.DataChannel
	pending          map[string][]webrtc.ICECandidateInit
}

func (f *fakeSFU) serve(conn *websocket.Conn) {
	peer := &fakePeer{fake: f, conn: conn, pending: make(map[string][]webrtc.ICECandidateInit)}
	defer func() {
		peer.close()
		f.leave(peer)
	}()
	for {
		var frame fakeIn
		if err := conn.ReadJSON(&frame); err != nil {
			return
		}
		if err := peer.handle(frame); err != nil {
			f.fail("handle "+frame.Event, err)
			return
		}
	}
}

// fakeIn is a client frame as it arrives on the wire.
type fakeIn struct {
	RoomID    string          `json:"roomId"` //nolint:tagliatelle // connector wire is camelCase
	Payload   json.RawMessage `json:"payload"`
	Event     string          `json:"event"`
	GroupID   string          `json:"groupId"`   //nolint:tagliatelle // connector wire is camelCase
	RequestID string          `json:"requestId"` //nolint:tagliatelle // connector wire is camelCase
}

// fakeMedia is the media-in payload the fake reads.
type fakeMedia struct {
	Method           string          `json:"method"`
	Description      *fakeDesc       `json:"description"`
	RTCIceCandidates []fakeCandidate `json:"rtcIceCandidates"` //nolint:tagliatelle // connector wire is camelCase
	PingReq          *fakePing       `json:"ping_req"`
}

type fakeDesc struct {
	SDP  string `json:"sdp"`
	Type string `json:"type"`
}

type fakeCandidate struct {
	Candidate     string  `json:"candidate"`
	SDPMid        *string `json:"sdpMid"`        //nolint:tagliatelle // connector wire is camelCase
	SDPMLineIndex *uint16 `json:"sdpMLineIndex"` //nolint:tagliatelle // connector wire is camelCase
	Target        string  `json:"target"`
}

type fakePing struct {
	Timestamp int64 `json:"timestamp"`
	RTT       int64 `json:"rtt"`
}

func (p *fakePeer) handle(frame fakeIn) error {
	switch frame.Event {
	case eventJoin:
		// The capture's join carries no group id: the connector names the
		// group in its answer to this very frame.
		if frame.GroupID != "" {
			p.fake.fail("join", errGroupOnJoin)
		}
		p.fake.join(p, frame.RoomID, frame)
		p.fake.awaitJoinGate()
		if err := p.write(eventJoinResponse, p.joinResponse()); err != nil {
			return err
		}
		if err := p.sendMedia(methodConfig, map[string]any{
			"configuration": map[string]any{"iceServers": []any{map[string]any{"urls": []string{fakeSTUNURL}}}},
		}); err != nil {
			return err
		}
		if err := p.offerSubscriber(); err != nil {
			return err
		}
		p.fake.broadcastParticipants(p.room, nil)
		return nil
	case eventMediaIn:
		// Everything after the join echoes the group the join-response
		// named, as the capture does.
		if frame.GroupID != p.group {
			p.fake.fail("media-in", errWrongGroup)
		}
		var media fakeMedia
		if err := json.Unmarshal(frame.Payload, &media); err != nil {
			return err
		}
		return p.handleMedia(media)
	default:
		return nil
	}
}

// broadcastParticipants sends the room roster to everyone in it, the way the
// SFU does when the membership changes. gone, when set, is listed as
// DISCONNECTED: that is how a participant leaves a LiveKit roster. A write
// that fails is dropped - one dead socket does not concern the others.
func (f *fakeSFU) broadcastParticipants(room string, gone *fakePeer) {
	peers := f.roomPeers(room)
	roster := make([]any, 0, len(peers)+1)
	for _, peer := range peers {
		roster = append(roster, rosterEntry(peer, "ACTIVE"))
	}
	if gone != nil {
		roster = append(roster, rosterEntry(gone, participantDisconnected))
	}
	payload := map[string]any{"update": map[string]any{"participants": roster}}
	for _, peer := range peers {
		_ = peer.sendMedia(methodParticipants, payload)
	}
}

func rosterEntry(peer *fakePeer, state string) map[string]any {
	return map[string]any{"sid": "PA_" + peer.id, "identity": peer.id, "state": state, "name": peer.id}
}

// leave removes a peer whose socket is gone and tells the room, as the SFU
// does when a participant drops.
func (f *fakeSFU) leave(peer *fakePeer) {
	f.mu.Lock()
	f.peers = slices.DeleteFunc(f.peers, func(other *fakePeer) bool { return other == peer })
	if peer.room != "" {
		f.rooms[peer.room] = slices.DeleteFunc(f.rooms[peer.room],
			func(other *fakePeer) bool { return other == peer })
	}
	room := peer.room
	f.mu.Unlock()
	if room != "" {
		f.broadcastParticipants(room, peer)
	}
}

func (p *fakePeer) joinResponse() map[string]any {
	return map[string]any{
		"roomId":    p.room,
		"meetingId": "fakemeeting",
		"participant": map[string]any{
			"sessionId":     "fakesession-" + p.id,
			"participantId": p.id,
		},
		"participantGroup": map[string]any{"groupId": p.group},
		"settings":         map[string]any{"maxParticipantCount": 100},
	}
}

func (p *fakePeer) handleMedia(media fakeMedia) error {
	switch media.Method {
	case methodAnswer:
		if media.Description == nil {
			return nil
		}
		p.fake.markAnswer()
		return p.acceptSubscriberAnswer(media.Description.SDP)
	case methodOffer:
		if media.Description == nil {
			return nil
		}
		p.fake.markPublisherOffer()
		return p.answerPublisher(media.Description.SDP)
	case methodICE:
		for _, cand := range media.RTCIceCandidates {
			p.addICE(cand)
		}
		return nil
	case methodPing:
		return p.pong(media.PingReq)
	default:
		return nil
	}
}

func (p *fakePeer) pong(ping *fakePing) error {
	var last int64
	if ping != nil {
		last = ping.Timestamp
	}
	return p.sendMedia(methodPong, map[string]any{
		"pong_resp": map[string]any{
			"lastPingTimestamp": strconv.FormatInt(last, 10),
			"timestamp":         strconv.FormatInt(time.Now().UnixMilli(), 10),
		},
	})
}

// offerSubscriber plays the SFU's half of the subscriber PC: it creates both
// channels, offers, and trickles its candidates.
func (p *fakePeer) offerSubscriber() error {
	pc, err := newFakePC()
	if err != nil {
		return err
	}
	pc.OnICECandidate(func(cand *webrtc.ICECandidate) { p.trickle(targetSubscriber, cand) })
	ordered := true
	rel, err := pc.CreateDataChannel(labelReliable, &webrtc.DataChannelInit{Ordered: &ordered})
	if err != nil {
		return err
	}
	var noRetransmits uint16
	lossy, err := pc.CreateDataChannel(labelLossy,
		&webrtc.DataChannelInit{Ordered: &ordered, MaxRetransmits: &noRetransmits})
	if err != nil {
		return err
	}
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return err
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		return err
	}
	p.mu.Lock()
	p.subPC, p.subRel, p.subLossy = pc, rel, lossy
	p.mu.Unlock()
	return p.sendMedia(methodOffer, map[string]any{
		"description": map[string]any{"type": sdpTypeOffer, "sdp": offer.SDP},
	})
}

func (p *fakePeer) acceptSubscriberAnswer(sdp string) error {
	pc := p.peerConnection(targetSubscriber)
	if pc == nil {
		return nil
	}
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: sdp}); err != nil {
		return err
	}
	p.drainICE(targetSubscriber)
	return nil
}

// answerPublisher accepts the PC the client offers. The channels it carries
// arrive through OnDataChannel; what the client writes on them is what the
// SFU relays to the room.
func (p *fakePeer) answerPublisher(sdp string) error {
	pc, err := newFakePC()
	if err != nil {
		return err
	}
	pc.OnICECandidate(func(cand *webrtc.ICECandidate) { p.trickle(targetPublisher, cand) })
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		dc.OnMessage(func(msg webrtc.DataChannelMessage) { p.fake.forward(p, msg.Data) })
	})
	p.mu.Lock()
	p.pubPC = pc
	p.mu.Unlock()
	offer := webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: sdp}
	if setErr := pc.SetRemoteDescription(offer); setErr != nil {
		return setErr
	}
	p.drainICE(targetPublisher)
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return err
	}
	if err := pc.SetLocalDescription(answer); err != nil {
		return err
	}
	return p.sendMedia(methodAnswer, map[string]any{
		"description": map[string]any{"type": sdpTypeAnswer, "sdp": answer.SDP},
	})
}

// deliver writes one relayed packet to this peer on the channel its kind
// belongs to.
func (p *fakePeer) deliver(frame []byte, lossy bool) {
	p.mu.Lock()
	dc := p.subRel
	if lossy {
		dc = p.subLossy
	}
	p.mu.Unlock()
	if dc == nil || dc.ReadyState() != webrtc.DataChannelStateOpen {
		return
	}
	if err := dc.Send(frame); err != nil {
		p.fake.fail("deliver", err)
	}
}

func (p *fakePeer) trickle(target string, cand *webrtc.ICECandidate) {
	if cand == nil {
		return
	}
	init := cand.ToJSON()
	mid := "0"
	if init.SDPMid != nil && *init.SDPMid != "" {
		mid = *init.SDPMid
	}
	var index uint16
	if init.SDPMLineIndex != nil {
		index = *init.SDPMLineIndex
	}
	_ = p.sendMedia(methodICE, map[string]any{
		"rtcIceCandidates": []any{map[string]any{
			"candidate":     init.Candidate,
			"sdpMid":        mid,
			"sdpMLineIndex": index,
			"target":        target,
		}},
	})
}

func (p *fakePeer) addICE(cand fakeCandidate) {
	target := cand.Target
	if target == "" {
		target = targetSubscriber
	}
	mid := "0"
	if cand.SDPMid != nil && *cand.SDPMid != "" {
		mid = *cand.SDPMid
	}
	var index uint16
	if cand.SDPMLineIndex != nil {
		index = *cand.SDPMLineIndex
	}
	init := webrtc.ICECandidateInit{Candidate: cand.Candidate, SDPMid: &mid, SDPMLineIndex: &index}
	p.mu.Lock()
	defer p.mu.Unlock()
	pc := p.pcLocked(target)
	if pc == nil || pc.RemoteDescription() == nil {
		p.pending[target] = append(p.pending[target], init)
		return
	}
	if err := pc.AddICECandidate(init); err != nil {
		p.fake.fail("add ice", err)
	}
}

func (p *fakePeer) drainICE(target string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pc := p.pcLocked(target)
	queued := p.pending[target]
	p.pending[target] = nil
	if pc == nil {
		return
	}
	for _, init := range queued {
		if err := pc.AddICECandidate(init); err != nil {
			p.fake.fail("drain ice", err)
		}
	}
}

func (p *fakePeer) peerConnection(target string) *webrtc.PeerConnection {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pcLocked(target)
}

func (p *fakePeer) pcLocked(target string) *webrtc.PeerConnection {
	if target == targetPublisher {
		return p.pubPC
	}
	return p.subPC
}

// write sends one server frame with the keys the capture shows for that
// event: a join-response carries none of the session ids, a media-out
// carries both, an error frame carries the group only.
func (p *fakePeer) write(event string, payload any) error {
	frame := map[string]any{
		"event":     event,
		"requestId": uuid.NewString(),
		"roomId":    p.room,
	}
	switch event {
	case eventMediaOut:
		frame["groupId"] = p.group
		frame["participantId"] = p.id
	case eventError:
		frame["groupId"] = p.group
	}
	if payload != nil {
		frame["payload"] = payload
	}
	p.wsMu.Lock()
	defer p.wsMu.Unlock()
	return p.conn.WriteJSON(frame)
}

func (p *fakePeer) sendMedia(method string, extra map[string]any) error {
	payload := map[string]any{"method": method}
	for key, value := range extra {
		payload[key] = value
	}
	return p.write(eventMediaOut, payload)
}

func (p *fakePeer) closeSocket() {
	p.wsMu.Lock()
	defer p.wsMu.Unlock()
	_ = p.conn.Close()
}

func (p *fakePeer) close() {
	p.closeSocket()
	p.closePeerConnections()
}

func (p *fakePeer) closePeerConnections() {
	p.mu.Lock()
	sub, pub := p.subPC, p.pubPC
	p.subPC, p.pubPC = nil, nil
	p.mu.Unlock()
	for _, pc := range []*webrtc.PeerConnection{sub, pub} {
		if pc != nil {
			_ = pc.Close()
		}
	}
}

// newFakePC builds the SFU side of a peer connection: no ICE servers, IPv4
// host candidates only, so a test never leaves the machine.
func newFakePC() (*webrtc.PeerConnection, error) {
	settings := webrtc.SettingEngine{}
	settings.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	return webrtc.NewAPI(webrtc.WithSettingEngine(settings)).NewPeerConnection(webrtc.Configuration{})
}
