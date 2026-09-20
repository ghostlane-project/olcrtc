package vp8channel

// ai-generated: the whole file (issue #12: the frame size a data lane keeps
// on a path that loses packets).

import (
	"cmp"
	"encoding/binary"
	"math"
	"sync"

	kcp "github.com/xtaci/kcp-go/v5"
)

const (
	// deliveryBucketMs is how many milliseconds of KCP timestamps one bucket
	// of the delivery count spans.
	deliveryBucketMs = 250
	// minJudgedPushes is the fewest pushed packets a judgement rests on,
	// and minJudgedFrames the fewest frames they came in: every packet of a
	// frame shares its fate, so it is the frames that make the evidence.
	// Sixty-four packets is a frame and a half at full size, and a lane on
	// a path losing one packet in a hundred read shares of 0.01 and 0.14
	// off such samples and halved frames it had no reason to.
	minJudgedPushes = 64
	minJudgedFrames = 24
	// minFramePackets is the fewest KCP packets a capped lane puts in a
	// frame.
	minFramePackets = 4
	// What a frame of k packets carries through a path that loses one in n
	// goes as k*q^(1.17k), whose peak sits at a delivered share of 1/e:
	// aiming higher than that trades away more in frame size than it wins
	// back in deliveries, and on the 1-3% paths aiming at 65% measured
	// 30-45% slower than no cap at all. So a lane shrinks its frames only
	// once two judgements in a row read well under the peak, and aims a
	// little over it. Under darkShare next to nothing got through: a relay
	// that stopped forwarding, which the lane's dark spells deal with, not
	// loss. What a lane pushed before it came back from a dark spell is not
	// weighed at all (forget).
	darkShare  = 0.02
	lossyShare = 0.25
	aimShare   = 0.40
	// frameGrowBuckets is how long a capped lane goes between raising the
	// cap a quarter while its readings are not lossy, in buckets. A cap
	// that only grew on a share the aim never reaches would be permanent,
	// and a cap set on a burst of bad luck would cost the rest of the
	// session; growing until the readings turn lossy and shrinking back
	// keeps a lane around the peak instead, and the curve is flat enough
	// there that the cycle costs a few per cent.
	frameGrowBuckets = 8

	// kcpTSOff is where the timestamp sits in a KCP segment header.
	kcpTSOff = 8
)

// stampClock turns kcp-go's timestamps, uint32 milliseconds that wrap every
// 49.7 days, into a count that keeps rising: each stamp is read as the signed
// difference from the newest one seen. Without it the bucket ids of a session
// alive at the wrap fall behind the newest one acknowledged, every bucket
// after it is judged before its acknowledgements are in, and the lane caps
// its frames on a path that loses nothing.
type stampClock struct {
	newest int64
	seen   bool
}

// unwrap is ts on the rising count.
func (c *stampClock) unwrap(ts uint32) int64 {
	if !c.seen {
		c.newest, c.seen = int64(ts), true
		return c.newest
	}
	at := c.newest + int64(int32(ts-uint32(c.newest))) //nolint:gosec // the signed difference is the point
	c.newest = max(c.newest, at)
	return at
}

// deliveryTrack counts the packets with a push a conn sends and the packets
// of acknowledgements that come back for them, both under the timestamp of
// the packet's last push: an acknowledgement echoes it. A conn acknowledges
// every packet it receives on its own (SetACKNoDelay), always including the
// packet's last push, so the second count over the first is the share of
// packets that got through and back. Pushes go out, and acknowledgements
// come back, in timestamp order, so once one for a later bucket is back,
// an earlier bucket has all it is going to get, whatever the round trip.
type deliveryTrack struct {
	mu        sync.Mutex
	clock     stampClock
	buckets   [32]deliveryBucket
	newestAck int64 // the bucket of the newest push acknowledged, zero for none
}

// deliveryBucket is one bucket of the count; id is the unwrapped timestamp
// over deliveryBucketMs plus one, zero for an unused bucket.
type deliveryBucket struct {
	id                   int64
	pushes, acks, frames int
}

// count adds pushes, acknowledgements and frames under the push timestamp ts
// and returns its bucket. An acknowledgement for a bucket already judged or
// reused is dropped.
func (t *deliveryTrack) count(ts uint32, pushes, acks, frames int) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	id := t.clock.unwrap(ts)/deliveryBucketMs + 1
	if acks > 0 && id > t.newestAck {
		t.newestAck = id
	}
	b := &t.buckets[id%int64(len(t.buckets))]
	if b.id != id {
		if pushes == 0 || id < b.id {
			return id
		}
		*b = deliveryBucket{id: id}
	}
	b.pushes += pushes
	b.acks += acks
	b.frames += frames
	return id
}

// take clears the buckets from before the newest acknowledged one, which
// have all they are going to get, and returns the pushes and
// acknowledgements of those from the bucket id from on.
func (t *deliveryTrack) take(from int64) (int, int, int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	pushes, acks, frames, before := 0, 0, 0, t.newestAck
	if before == 0 {
		return 0, 0, 0
	}
	for i := range t.buckets {
		b := &t.buckets[i]
		if b.id == 0 || b.id >= before {
			continue
		}
		if b.id >= from {
			pushes += b.pushes
			acks += b.acks
			frames += b.frames
		}
		*b = deliveryBucket{}
	}
	return pushes, acks, frames
}

// frameCap is the most KCP packets a data lane puts in one frame. A frame
// reaches the peer only when every RTP packet of it does, so on a path that
// loses one packet in twenty-five a full frame of fifty is lost seven times
// in eight, and every segment in it with it: KCP resends the same segments
// in the same full frames, backs their timeouts off each time, and the
// transfer crawls while the small control frames still get through.
type frameCap struct {
	// limit is the cap, zero for none. newest is the newest bucket a push
	// went out in, and since the first bucket whose pushes went out under
	// the current limit. pushes and acks are the evidence gathered since.
	limit                int
	newest, since        int64
	grownAt              int64
	pushes, acks, frames int
	// lossy is whether the last judgement read under lossyShare. One such
	// reading is a run of bad luck as often as it is a lossy path, and
	// halving the frames for it costs seconds of growing back.
	lossy bool
}

// pushed counts a packet with a push that went out under timestamp ts.
func (f *frameCap) pushed(track *deliveryTrack, ts uint32) {
	f.newest = track.count(ts, 1, 0, 0)
}

// wrote counts the frame those pushes went out in, under the timestamp of
// its last one.
func (f *frameCap) wrote(track *deliveryTrack, ts uint32) {
	track.count(ts, 0, 0, 1)
}

// forget drops the evidence gathered so far, and what was pushed before now:
// the lane came back from a dark spell, which says nothing of how lossy the
// path is.
func (f *frameCap) forget() {
	f.pushes, f.acks, f.frames, f.lossy, f.since = 0, 0, 0, false, f.newest+1
}

// halve halves the cap, down to minFramePackets, as the lane goes dark, and
// reports whether it changed. On a path that loses one packet in twelve
// next to no full frame gets through, the lane goes dark before a share
// could be weighed, and halving is what lets one be: frames half the size
// come back several times as often. A relay that stopped forwarding for
// being over its budget takes smaller frames just as well, and the cap
// grows back once they come back clean.
func (f *frameCap) halve(full int) bool {
	limit := max(cmp.Or(f.limit, full)/2, minFramePackets)
	changed := limit != f.limit
	f.limit, f.grownAt = limit, f.newest
	f.forget()
	return changed
}

// judge weighs what came back of the packets pushed before the newest one
// acknowledged, and changes the cap when there is enough of it: while two
// judgements in a row read under lossyShare, down to what should bring back
// aimShare, at most half of it; otherwise a quarter up every
// frameGrowBuckets, to full, the packets a sample holds, which lifts it. It
// returns the share judged and whether the cap changed.
func (f *frameCap) judge(track *deliveryTrack, full int) (float64, bool) {
	pushes, acks, frames := track.take(f.since)
	f.pushes, f.acks, f.frames = f.pushes+pushes, f.acks+acks, f.frames+frames
	if f.pushes < minJudgedPushes || f.frames < minJudgedFrames {
		return 0, false
	}
	share := float64(f.acks) / float64(f.pushes)
	f.pushes, f.acks, f.frames = 0, 0, 0
	lossyBefore := f.lossy
	f.lossy = share >= darkShare && share < lossyShare
	limit := f.limit
	switch {
	case share < darkShare:
		return share, false
	case f.lossy && lossyBefore:
		limit = shrunk(cmp.Or(limit, full), share)
	case limit > 0 && !f.lossy && f.newest-f.grownAt >= frameGrowBuckets:
		limit += max(limit/4, 1)
		if limit >= full {
			limit = 0
		}
	}
	if limit == f.limit {
		return share, false
	}
	f.limit, f.since, f.grownAt = limit, f.newest+1, f.newest
	return share, true
}

// shrunk is the frame size that should bring back aimShare of the pushed
// packets where frames of limit packets brought back share: every packet of
// a frame has to get through, so the share falls as a power of the size.
// It is at most half of limit and at least minFramePackets.
func shrunk(limit int, share float64) int {
	aim := float64(limit) * math.Log(aimShare) / math.Log(share)
	return max(min(int(aim), limit/2), minFramePackets)
}

// packetStamp is what stampsOf reads off a KCP packet: the timestamps of its
// last push and of its last acknowledgement, and whether it has either.
type packetStamp struct {
	push, ack     uint32
	pushed, acked bool
}

// stampsOf walks a KCP packet for its packetStamp. kcp-go packs
// acknowledgements first and a packet of them alone is walked to its end; a
// length longer than what is left ends the walk.
func stampsOf(segs []byte) packetStamp {
	var st packetStamp
	for len(segs) >= kcp.IKCP_OVERHEAD {
		ts := binary.LittleEndian.Uint32(segs[kcpTSOff:])
		switch segs[kcpCmdOff] {
		case kcp.IKCP_CMD_PUSH:
			st.push, st.pushed = ts, true
		case kcp.IKCP_CMD_ACK:
			st.ack, st.acked = ts, true
		}
		size := binary.LittleEndian.Uint32(segs[kcpLenOff:])
		rest := segs[kcp.IKCP_OVERHEAD:]
		if uint64(size) > uint64(len(rest)) {
			break
		}
		segs = rest[size:]
	}
	return st
}
