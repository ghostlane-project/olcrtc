package salutejazz

import "encoding/binary"

// The relay window's wire (olcrtc#49). The window itself - what a sender may
// have handed the SFU for one destination that the destination has not yet
// taken off it - is internal/relaywin's; this engine carries its marks and
// echoes to the other end of the tunnel and back.
//
// A mark or an echo is a UserPacket under windowTopic on the reliable lane,
// addressed to the one participant it is for:
//
//	[version=1][kind: 1 mark, 2 echo][counter, u64 big-endian][advertised window, u32 big-endian]
//
// The counter is the sender's count of bytes handed to the relay for that
// destination when the mark went out, and an echo carries back the counter of
// the mark it answers. The advertised window is reserved for budgeting a
// receiver that several senders fill at once: version 1 writes the window it
// keeps, and a version 1 reader takes the frame whatever it says.
//
// ai-generated: the whole file (olcrtc#49).

const (
	// windowTopic marks a window frame. It is never the tunnel's: the
	// receive path takes it off before anything is delivered.
	windowTopic = "olcrtc.win"

	// windowVersion is the version this engine writes and the only one it
	// reads. A later version that needs another layout says so here.
	windowVersion byte = 1
	windowMark    byte = 1
	windowEcho    byte = 2
	// windowFrameLen is a version 1 frame, and only a frame of exactly this
	// length is read as one.
	windowFrameLen = 1 + 1 + 8 + 4

	// relayWindow is what may be in flight to one destination: 192 KiB. A
	// control ping waits behind up to a window one way and its pong behind
	// up to a window the other, and on the slowest SFU-to-receiver leg the
	// gate has measured, 34 kB/s, two windows and a round trip have to fit
	// in the tunnel's 15 s liveness timeout with a margin: 256 KiB does not.
	// At a 0.35 s round trip it still carries about 3.9 Mbit/s (7/8 of a
	// window a round trip), over the gate's 1.4 and 1.6 Mbit/s floors.
	relayWindow = 192 << 10
)

// windowFrame is a mark or an echo, as the wire carries it.
type windowFrame struct {
	kind    byte
	counter uint64
}

// encode writes the frame in version 1, advertising the window this engine
// keeps.
func (f windowFrame) encode() []byte {
	frame := make([]byte, windowFrameLen)
	frame[0] = windowVersion
	frame[1] = f.kind
	binary.BigEndian.PutUint64(frame[2:10], f.counter)
	binary.BigEndian.PutUint32(frame[10:], relayWindow)
	return frame
}

// parseWindowFrame reads a version 1 frame. Another version, a kind it does
// not know or any other length is not one, and reports false.
func parseWindowFrame(frame []byte) (windowFrame, bool) {
	if len(frame) != windowFrameLen || frame[0] != windowVersion {
		return windowFrame{}, false
	}
	kind := frame[1]
	if kind != windowMark && kind != windowEcho {
		return windowFrame{}, false
	}
	return windowFrame{kind: kind, counter: binary.BigEndian.Uint64(frame[2:10])}, true
}
