// Package relaywin bounds what a sender has handed a relay for one
// destination and the destination has not yet taken off it: an end-to-end
// window per destination, over a relay that never pushes back.
//
// An SFU that relays data between participants reads a sender's channel as
// fast as frames arrive and queues them toward each receiver. LiveKit 1.5.3,
// which SaluteJazz runs, acknowledges a megabyte at once and queues toward a
// subscriber without a limit; the JVB queues about 2 MB and drops the rest
// (olcrtc#15). Either way the sender's own channel says nothing about the
// receiver's leg, and on a slow one the queue grows to whatever smux's
// windows allow: megabytes, tens of seconds of it, with the tunnel's control
// pings waiting at the back (olcrtc#49).
//
// So a sender counts the bytes it hands the relay for each destination and
// follows them, every MarkEvery bytes, with a mark that carries the count.
// The destination echoes a mark when its receive path reaches it. The relay
// delivers in order, so an echo means every byte before the mark has left
// the relay; what is in flight is what was sent less the last count echoed,
// and a sender holds while that is a whole Window. The count is the sender's
// own, so a frame lost anyway cannot skew it: the next echo covers it.
//
// The carrier owns the wire - how a mark and an echo are framed, and whose
// echo counts for which destination. This package counts and decides:
//
//   - A window holds a sender only once it is on: an echo came from the
//     destination (ApplyEcho), or the destination said some other way that it
//     speaks the window (Arm). An older build never echoes and is sent to as
//     before. Until it echoes, a destination is marked at least once a
//     ProbeAfter, so one that does speak the window turns it on within an
//     interval of traffic.
//   - An echo moves a window only past its last echo and within what was
//     sent. Counts never start over: a window opened after a Reset starts at
//     the count of every byte counted here, past every mark sent before it,
//     so a late echo of one of those is out of its range.
//   - A sender held for ProbeAfter since the last mark marks again (Room asks
//     for it), which repairs a lost mark or echo. One held for DeadAfter
//     without an echo lets the destination go: its window is off, and what
//     waits for it goes as it would without one. Liveness, not the window,
//     decides that a peer is gone.
//   - Each window has an epoch, which Reset, ResetAll and Bump replace. A
//     sender that parked under one epoch and wakes under another holds a
//     frame for a session that is over, and must not send it.
//   - A frame the carrier would rather drop than hold, a datagram, asks Over
//     instead of Room: it goes until the window is on and more than a Window
//     and the carrier's slack is in flight, or a sender has been held on the
//     window for ProbeAfter. The slack lets datagrams through beside a bulk
//     sender that keeps the window full, which is let go at every echo; the
//     yield keeps a datagram flow as fast as the destination's leg from
//     taking every byte an echo frees before a held sender looks again, which
//     would hold it until liveness closed the session. Over only looks, so
//     asking it on every datagram moves none of the clocks a held sender runs
//     on.
//
// A sender that has to wait takes its window's Wake before it asks Room, then
// waits on that channel and a Retry timer. An echo that moves the window, a
// Reset or a Bump of it, and ResetAll close the channel, so a wake between
// the question and the wait is not missed. One wake releases every sender
// parked on that window and none parked on another: a server with a sender
// parked per client wakes the one an echo made room for, not all of them on
// every echo from any of them.
//
// Jitsi keeps its own copy of the mechanism, where it was proved
// (internal/engine/jitsi/relaywindow.go); this is that mechanism without a
// wire, for the carriers that came after it.
//
// ai-generated: the whole package (olcrtc#49).
package relaywin

import (
	"sync"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
)

const (
	// DefaultWindow is 192 KiB a destination. A control ping waits behind
	// up to a window one way and its pong behind up to a window the other,
	// so on the slowest SFU-to-receiver leg the gate has measured, 34 kB/s,
	// two windows and a round trip have to fit in the tunnel's 15 s
	// liveness timeout with a margin: 256 KiB does not. At a 0.35 s round
	// trip it still carries about 3.9 Mbit/s (7/8 of a window a round
	// trip), over the gate's 1.4 and 1.6 Mbit/s floors.
	DefaultWindow = 192 << 10
	// DefaultProbeAfter: a sender held this long since its last mark marks
	// again, and a destination that has not echoed yet is marked at least
	// this often.
	DefaultProbeAfter = time.Second
	// DefaultDeadAfter: a destination that has held a sender this long
	// without an echo is let go. A relay that never drops reliable data
	// (LiveKit) makes a silent destination a slow leg or a hole being
	// repaired far more often than a dead one, and liveness closes a dead
	// one long before this: it is a backstop, not a verdict.
	DefaultDeadAfter = 120 * time.Second
	// DefaultRetry is how often a held sender looks again, for the probe and
	// the dead check; an echo, a Reset or a Bump wakes it at once anyway.
	DefaultRetry = DefaultProbeAfter / 4

	// markDivisor: a MarkEvery left zero is this part of the window, which
	// keeps a sender's view within an eighth of the truth for one small frame
	// a mark.
	markDivisor = 8
)

// Timing sizes a Windows. A field left zero takes its default: the Default
// constants, and for MarkEvery an eighth of Window.
type Timing struct {
	// Window is how many bytes may be in flight to one destination.
	Window uint64
	// MarkEvery is how many bytes to a destination go between two marks.
	MarkEvery uint64
	// ProbeAfter, DeadAfter and Retry: see DefaultProbeAfter,
	// DefaultDeadAfter and DefaultRetry.
	ProbeAfter time.Duration
	DeadAfter  time.Duration
	Retry      time.Duration
}

func (t Timing) withDefaults() Timing {
	if t.Window == 0 {
		t.Window = DefaultWindow
	}
	if t.MarkEvery == 0 {
		t.MarkEvery = max(t.Window/markDivisor, 1)
	}
	if t.ProbeAfter <= 0 {
		t.ProbeAfter = DefaultProbeAfter
	}
	if t.DeadAfter <= 0 {
		t.DeadAfter = DefaultDeadAfter
	}
	if t.Retry <= 0 {
		t.Retry = DefaultRetry
	}
	return t
}

// state is the window toward one destination.
type state struct {
	sent      uint64    // bytes handed to the relay for the destination
	marked    uint64    // sent, as of the last mark
	echoed    uint64    // the highest count the destination has echoed
	active    bool      // the window holds: an echo came, or Arm
	markedAt  time.Time // when the last mark went out
	heldSince time.Time // when the window filled, cleared by every echo
	// waitingSince is when a sender was first held since Room last let one
	// go. Unlike heldSince an echo leaves it: an echo whose room a datagram
	// takes lets nobody go. Room letting a sender go clears it, and so does
	// Bump, which sends every parked sender away.
	waitingSince time.Time
	epoch        uint64 // replaced by Reset, ResetAll and Bump; never zero
	// wake is closed when a sender held on the window may go on: replaced
	// by an echo that moves it and by Bump, left closed by Reset and
	// ResetAll, which drop the window with it.
	wake chan struct{}
}

// Windows is the window toward every destination of one sender, keyed by
// whatever the carrier addresses a destination by. It is safe for concurrent
// use. Make one with New.
type Windows struct {
	timing Timing

	mu      sync.Mutex
	windows map[string]*state
	// count is every byte counted against a window since New, where a new
	// window's count starts. epochs is the last epoch handed out.
	count  uint64
	epochs uint64
}

// New returns a Windows with no window open, sized by t.
func New(t Timing) *Windows {
	return &Windows{
		timing:  t.withDefaults(),
		windows: make(map[string]*state),
	}
}

// Timing is the timing in force, its zero fields filled in: a held sender
// waits Retry between looks.
func (w *Windows) Timing() Timing { return w.timing }

// Room reports whether a send to key may go now. It is false only while the
// window is on and a whole Window is in flight.
//
// A window that holds the sender and has not marked for ProbeAfter asks for
// a probe: probe is true and counter is the count the mark carries, which the
// caller sends whether or not ok. A window that has held the sender for
// DeadAfter without an echo is turned off, and the send goes.
func (w *Windows) Room(key string, now time.Time) (bool, bool, uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	st := w.windows[key]
	if st == nil || !st.active || st.sent-st.echoed < w.timing.Window {
		if st != nil {
			st.heldSince, st.waitingSince = time.Time{}, time.Time{}
		}
		return true, false, 0
	}
	if st.heldSince.IsZero() {
		st.heldSince = now
	}
	if st.waitingSince.IsZero() {
		st.waitingSince = now
	}
	if held := now.Sub(st.heldSince); held >= w.timing.DeadAfter {
		// The destination is not named: the key is the carrier's address
		// for it, which does not go into a log.
		logger.Infof("relaywin: no echo for %s with %d bytes in flight - window off",
			held.Round(time.Second), st.sent-st.echoed)
		st.active = false
		st.heldSince, st.waitingSince = time.Time{}, time.Time{}
		return true, false, 0
	}
	if now.Sub(st.markedAt) >= w.timing.ProbeAfter {
		st.marked, st.markedAt = st.sent, now
		return false, true, st.sent
	}
	return false, false, 0
}

// Sent counts n bytes handed to the relay for key, opening its window if
// there is none, and reports whether a mark is due and the count it carries:
// every MarkEvery bytes, and while the destination has not echoed, once a
// ProbeAfter as well, starting with the first bytes. The caller sends the
// mark after the bytes it counts, on the same ordered lane. counter is zero
// when no mark is due.
func (w *Windows) Sent(key string, n int, now time.Time) (bool, uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	st := w.open(key)
	add := uint64(max(n, 0))
	st.sent += add
	w.count += add
	due := st.sent-st.marked >= w.timing.MarkEvery ||
		(!st.active && now.Sub(st.markedAt) >= w.timing.ProbeAfter)
	if !due {
		return false, 0
	}
	st.marked, st.markedAt = st.sent, now
	return true, st.sent
}

// ApplyEcho takes the destination's echo of counter. It moves key's window
// only past the last echo and within what was sent, and reports whether it
// moved and whether that turned the window on. A window that moved wakes
// every sender parked on it. The caller has checked that the echo came from
// the destination key names: anyone who can see a count can send it back.
func (w *Windows) ApplyEcho(key string, counter uint64) (bool, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	st := w.windows[key]
	if st == nil || counter <= st.echoed || counter > st.sent {
		return false, false
	}
	turnedOn := !st.active
	st.active = true
	st.echoed = counter
	st.heldSince = time.Time{}
	st.wakeSenders()
	return true, turnedOn
}

// Arm turns key's window on without an echo, opening it if there is none:
// the destination has said some other way that it speaks the window, so a
// sender may be held before its first echo comes. What is in flight stays.
func (w *Windows) Arm(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	st := w.open(key)
	if !st.active {
		st.active = true
		st.heldSince = time.Time{}
	}
}

// Reset forgets key's window: the destination starts over, and what was
// counted toward it means nothing to what comes after. A window opened for
// key later starts off, at the count of every byte counted so far, under a
// new epoch. A sender parked on the window is woken.
func (w *Windows) Reset(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if st := w.windows[key]; st != nil {
		close(st.wake)
		delete(w.windows, key)
	}
}

// ResetAll forgets every window, as Reset does one, and wakes every parked
// sender. The count of bytes and the epochs go on.
func (w *Windows) ResetAll() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, st := range w.windows {
		close(st.wake)
	}
	clear(w.windows)
}

// Bump gives key's window a new epoch and keeps its counts: the session a
// parked sender's frame belongs to is over, but the bytes it handed the relay
// before are still queued there and their echoes still count. A sender
// parked on the window is woken, to find its epoch gone, so nobody is waiting
// on the window any more.
func (w *Windows) Bump(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if st := w.windows[key]; st != nil {
		st.epoch = w.nextEpoch()
		st.waitingSince = time.Time{}
		st.wakeSenders()
	}
}

// Epoch is the epoch of key's window, opening the window if there is none.
// It is never zero, and it is never one another window has had: a sender
// takes it before it waits and compares it with the one it wakes to.
func (w *Windows) Epoch(key string) uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.open(key).epoch
}

// Over reports whether key's window is on with more than a Window and slack
// in flight, or has held a sender for ProbeAfter by now. It is for a frame the
// carrier drops rather than holds, a datagram: one that finds the window over
// is dropped, and one that does not goes and is counted with Sent like any
// other. A window that is off is never over - an older build never echoes, so
// what is in flight to it only grows, and its datagrams go as they did before
// the window - and neither is a destination with no window.
//
// The slack is for a bulk sender that keeps the window full: it is let go at
// every echo, so its waits are short, and datagrams still go beside it. A
// sender held for ProbeAfter is one they are starving: a flow as fast as the
// leg takes the room each echo frees before the sender looks again. From then
// until Room lets a sender go, every datagram to key is dropped, even one
// that finds room an echo has just made: that room is the held sender's.
//
// Over only looks. It opens no window, starts no hold, takes no probe, lets
// nothing go and wakes nobody: those are Room's, for the sender that waits.
func (w *Windows) Over(key string, slack uint64, now time.Time) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	st := w.windows[key]
	if st == nil || !st.active {
		return false
	}
	if !st.waitingSince.IsZero() && now.Sub(st.waitingSince) >= w.timing.ProbeAfter {
		return true
	}
	// Compared past the Window, so no slack can wrap the sum.
	inFlight := st.sent - st.echoed
	return inFlight > w.timing.Window && inFlight-w.timing.Window > slack
}

// Wake is the channel closed the next time a sender held on key's window may
// go on: an echo moved the window, a Reset or a Bump changed it, or ResetAll
// changed every window. A sender takes it before it asks Room, so a wake
// between the two is not missed. It opens the window if there is none, as
// Epoch does, so the channel is the one of the window Room will look at.
func (w *Windows) Wake(key string) <-chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.open(key).wake
}

// Len is how many destinations have a window open. It only looks. A window
// stays open until Reset or ResetAll: a carrier that sends to a destination
// gone for good opens one it keeps, and Len is how that shows.
func (w *Windows) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.windows)
}

// open returns key's window, opening one at the session's count if there is
// none: no mark sent before it is past that count, so no echo of one can move
// the new window. Called with mu held.
func (w *Windows) open(key string) *state {
	st := w.windows[key]
	if st == nil {
		st = &state{
			sent: w.count, marked: w.count, echoed: w.count,
			epoch: w.nextEpoch(),
			wake:  make(chan struct{}),
		}
		w.windows[key] = st
	}
	return st
}

// nextEpoch hands out an epoch no window has had. Called with mu held.
func (w *Windows) nextEpoch() uint64 {
	w.epochs++
	return w.epochs
}

// wakeSenders releases every sender parked on the window and arms the next
// wait. Called with mu held.
func (st *state) wakeSenders() {
	close(st.wake)
	st.wake = make(chan struct{})
}
