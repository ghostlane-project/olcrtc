package salutejazz

import (
	"encoding/json"
	"testing"
)

// TestRTCJoinSeedsTheRoster pins where a joiner learns who is already in the
// room: rtc:join carries otherParticipants, and that is the only frame that
// names them. The roster updates the connector sends afterwards describe this
// participant to itself, so a session that read only those would sit in a
// full room believing it was alone.
func TestRTCJoinSeedsTheRoster(t *testing.T) {
	sess := newIdleSession(t)
	gen := newGeneration(nil)
	storeString(&gen.identity, "self")

	sess.handleMediaOut(gen, mediaOutJSON(t, map[string]any{
		"method": methodJoin,
		"join": map[string]any{"otherParticipants": []any{
			map[string]any{"sid": "PA_first", "identity": "first", "state": "ACTIVE"},
			map[string]any{"sid": "PA_second", "identity": "second", "state": "ACTIVE"},
		}},
	}))
	if peers := gen.remoteIdentities(); len(peers) != 2 || peers[0] != "first" || peers[1] != "second" {
		t.Fatalf("roster after rtc:join = %v, want both participants it listed", peers)
	}

	// The update frames describe this participant, and this session is never
	// its own peer.
	sess.handleMediaOut(gen, mediaOutJSON(t, map[string]any{
		"method": methodParticipants,
		"update": map[string]any{"participants": []any{
			map[string]any{"sid": "PA_self", "identity": "self", "state": "ACTIVE"},
		}},
	}))
	if peers := gen.remoteIdentities(); len(peers) != 2 {
		t.Fatalf("roster after an update about this participant = %v", peers)
	}

	// An empty room is the other half: a joiner that is first in is told so,
	// and takes nobody from it.
	fresh := newGeneration(nil)
	sess.handleMediaOut(fresh, mediaOutJSON(t, map[string]any{
		"method": methodJoin,
		"join":   map[string]any{"otherParticipants": []any{}},
	}))
	if peers := fresh.remoteIdentities(); len(peers) != 0 {
		t.Fatalf("roster after joining an empty room = %v", peers)
	}
}

// TestParticipantLeftClearsTheGhost covers the event the connector sends the
// rest of the room when a participant's socket has been gone for its ping
// timeout. Nothing else is known to report it, and a participant left in the
// roster keeps this session believing there is someone there to talk to.
func TestParticipantLeftClearsTheGhost(t *testing.T) {
	for _, tc := range []struct {
		name  string
		frame envIn
	}{
		{"named by the envelope", envIn{
			Event: eventParticipantLeft, ParticipantID: "ghost",
		}},
		{"named by the payload", envIn{
			Event: eventParticipantLeft, Payload: json.RawMessage(`{"participantId":"ghost"}`),
		}},
		{"named by the payload participant", envIn{
			Event: eventParticipantLeft, Payload: json.RawMessage(`{"participant":{"participantId":"ghost"}}`),
		}},
	} {
		sess := newIdleSession(t)
		gen := newGeneration(nil)
		if !sess.publishGeneration(gen) {
			t.Fatal("the session refused the attempt")
		}
		gen.applyParticipants([]participant{
			{SID: "PA_ghost", Identity: "ghost", State: "ACTIVE"},
			{SID: "PA_other", Identity: "other", State: "ACTIVE"},
		})
		if !sess.hasPeer() {
			t.Fatalf("%s: the roster named nobody to lose", tc.name)
		}

		sess.handleEnvelope(gen, tc.frame)

		if peers := gen.remoteIdentities(); len(peers) != 1 || peers[0] != "other" {
			t.Fatalf("%s: roster = %v, want the participant that is still here", tc.name, peers)
		}
	}

	// The last one out leaves a session with no peer at all: that is the
	// state WaitForPeer is about.
	sess := newIdleSession(t)
	gen := newGeneration(nil)
	if !sess.publishGeneration(gen) {
		t.Fatal("the session refused the attempt")
	}
	gen.applyParticipants([]participant{{SID: "PA_ghost", Identity: "ghost", State: "ACTIVE"}})
	sess.handleEnvelope(gen, envIn{Event: eventParticipantLeft, ParticipantID: "ghost"})
	if sess.hasPeer() {
		t.Fatal("a participant the connector reported gone still counts as a peer")
	}
}

// TestAParticipantLeftThatNamesNobodyLeavesTheRosterAlone is the other side
// of a shape the capture does not carry: a frame this engine cannot read an
// identity out of takes nobody out of the room.
func TestAParticipantLeftThatNamesNobodyLeavesTheRosterAlone(t *testing.T) {
	sess := newIdleSession(t)
	gen := newGeneration(nil)
	gen.applyParticipants([]participant{{SID: "PA_other", Identity: "other", State: "ACTIVE"}})

	for _, payload := range []string{`{}`, `{"participantId":""}`, `not json`, ``} {
		sess.handleEnvelope(gen, envIn{Event: eventParticipantLeft, Payload: json.RawMessage(payload)})
		if peers := gen.remoteIdentities(); len(peers) != 1 || peers[0] != "other" {
			t.Fatalf("payload %q left the roster as %v", payload, peers)
		}
	}
}

// mediaOutJSON renders one media-out payload the way it arrives on the wire.
func mediaOutJSON(t *testing.T, payload map[string]any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
