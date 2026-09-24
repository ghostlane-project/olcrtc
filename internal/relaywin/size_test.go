package relaywin

import (
	"testing"
	"time"
)

// ai-generated: the whole file (a window sized for its leg, olcrtc#49).

// sized is the Timing SaluteJazz sizes its windows with.
var sized = Timing{TargetQueue: 1500 * time.Millisecond}

// leg is a relay and the destination behind it, on the fake clock: what a
// sender hands the relay queues, the leg drains it at rate bytes a second, and
// the destination's echo of a mark comes back rtt after the leg delivered the
// mark's last byte. A sender keeps the window full, or sends only appLimit
// bytes a second when that is set.
type leg struct {
	t        *testing.T
	w        *Windows
	key      string
	rate     float64
	rtt      time.Duration
	appLimit float64
	// timed says whether echoes carry the clock (ApplyEchoAt) or not.
	timed bool

	now       time.Time
	drained   float64
	pending   []uint64 // marks sent, not yet delivered
	echoes    []echoAt // marks delivered, echo on its way
	appBudget float64
	// worstQueue is the longest a byte waited in the relay, in time: the
	// queue ahead of a pong on this leg.
	worstQueue time.Duration
}

type echoAt struct {
	counter uint64
	at      time.Time
}

func newLeg(t *testing.T, timing Timing, rate float64, rtt time.Duration) *leg {
	t.Helper()
	l := &leg{t: t, w: New(timing), key: "peer", rate: rate, rtt: rtt, timed: true, now: t0}
	on(t, l.w, l.key, 1)
	l.drained = float64(snapshot(t, l.w, l.key).sent)
	return l
}

// run steps the clock by tick for d, sending, draining and echoing.
func (l *leg) run(d, tick time.Duration) {
	l.t.Helper()
	for end := l.now.Add(d); l.now.Before(end); l.now = l.now.Add(tick) {
		l.send(tick)
		l.drain(tick)
		l.echo()
	}
}

func (l *leg) send(tick time.Duration) {
	if l.appLimit > 0 {
		l.appBudget += l.appLimit * tick.Seconds()
	}
	for {
		if l.appLimit > 0 && l.appBudget < frame {
			return
		}
		ok, probe, counter := l.w.Room(l.key, l.now)
		if probe {
			l.pending = append(l.pending, counter)
		}
		if !ok {
			return
		}
		if due, counter := l.w.Sent(l.key, frame, l.now); due {
			l.pending = append(l.pending, counter)
		}
		if l.appLimit > 0 {
			l.appBudget -= frame
		}
	}
}

func (l *leg) drain(tick time.Duration) {
	sent := float64(snapshot(l.t, l.w, l.key).sent)
	if queued := sent - l.drained; queued > 0 {
		if wait := time.Duration(queued / l.rate * float64(time.Second)); wait > l.worstQueue {
			l.worstQueue = wait
		}
	}
	l.drained = min(sent, l.drained+l.rate*tick.Seconds())
	for len(l.pending) > 0 && float64(l.pending[0]) <= l.drained {
		l.echoes = append(l.echoes, echoAt{counter: l.pending[0], at: l.now.Add(l.rtt)})
		l.pending = l.pending[1:]
	}
}

func (l *leg) echo() {
	for len(l.echoes) > 0 && !l.echoes[0].at.After(l.now) {
		if l.timed {
			l.w.ApplyEchoAt(l.key, l.echoes[0].counter, l.now)
		} else {
			l.w.ApplyEcho(l.key, l.echoes[0].counter)
		}
		l.echoes = l.echoes[1:]
	}
}

// settle runs the leg long enough for a window to find its size, then starts
// the queue record over.
func (l *leg) settle() {
	l.run(30*time.Second, 10*time.Millisecond)
	l.worstQueue = 0
}

// The leg the gate measured through Sber's relay (olcrtc#49): 26 kB/s from the
// SFU to the receiver, 0.3 s of round trip under the queue.
const (
	slowLeg = 26_000.0
	slowRTT = 300 * time.Millisecond
)

func TestASizedWindowShrinksToWhatASlowLegCarries(t *testing.T) {
	l := newLeg(t, sized, slowLeg, slowRTT)
	l.settle()
	l.run(30*time.Second, 10*time.Millisecond)

	size := l.w.Size(l.key)
	if size >= DefaultWindow/2 {
		t.Fatalf("on a %.0f kB/s leg the window stayed at %d KiB", slowLeg/1000, size>>10)
	}
	if size < DefaultMinWindow {
		t.Fatalf("the window shrank to %d bytes, under its %d KiB floor", size, DefaultMinWindow>>10)
	}
	// What a pong waits behind is the queue the window lets build: about
	// TargetQueue, plus the round trip the window cannot tell from queue
	// (the first mark's own interval rides in the shortest echo it sees).
	if limit := 3 * time.Second; l.worstQueue > limit {
		t.Fatalf("a byte waited %s in the relay on a settled %.0f kB/s leg, over %s",
			l.worstQueue.Round(time.Millisecond), slowLeg/1000, limit)
	}
	t.Logf("%.0f kB/s leg: window %d KiB, worst queue %s", slowLeg/1000, size>>10, l.worstQueue.Round(time.Millisecond))
}

func TestAFixedWindowQueuesSecondsOnTheSameLeg(t *testing.T) {
	// The case the sizing exists for, kept as a control: the same leg with
	// the fixed window queues a whole window, several seconds of it.
	l := newLeg(t, Timing{}, slowLeg, slowRTT)
	l.settle()
	l.run(30*time.Second, 10*time.Millisecond)
	if size := l.w.Size(l.key); size != DefaultWindow {
		t.Fatalf("a fixed window changed size to %d", size)
	}
	if l.worstQueue < 6*time.Second {
		t.Fatalf("the fixed window queued only %s on a %.0f kB/s leg: the control proves nothing",
			l.worstQueue.Round(time.Millisecond), slowLeg/1000)
	}
}

func TestASizedWindowGrowsBackWhenTheLegSpeedsUp(t *testing.T) {
	l := newLeg(t, sized, slowLeg, slowRTT)
	l.settle()
	if l.w.Size(l.key) >= DefaultWindow {
		t.Fatal("the window did not shrink on the slow leg to begin with")
	}
	l.rate = 1_000_000
	l.run(20*time.Second, 10*time.Millisecond)
	if size := l.w.Size(l.key); size != DefaultWindow {
		t.Fatalf("on a 1 MB/s leg the window is %d KiB, not its %d KiB cap", size>>10, DefaultWindow>>10)
	}
}

func TestASenderThatIsNeverHeldLeavesTheSizeAlone(t *testing.T) {
	// A sender under what the leg carries says nothing about the leg.
	l := newLeg(t, sized, slowLeg, slowRTT)
	l.appLimit = slowLeg / 4
	l.run(60*time.Second, 10*time.Millisecond)
	if size := l.w.Size(l.key); size != DefaultWindow {
		t.Fatalf("a sender that never filled the window moved its size to %d KiB", size>>10)
	}
}

func TestMarksFollowTheSize(t *testing.T) {
	l := newLeg(t, sized, slowLeg, slowRTT)
	l.settle()
	st := snapshot(t, l.w, l.key)
	want := max(st.size*l.w.Timing().MarkEvery/DefaultWindow, minMarkEvery)
	if st.markEvery != want {
		t.Fatalf("a %d KiB window marks every %d bytes, want %d", st.size>>10, st.markEvery, want)
	}
	if st.markEvery >= l.w.Timing().MarkEvery {
		t.Fatalf("a %d KiB window still marks every %d bytes, as the cap does", st.size>>10, st.markEvery)
	}
}

func TestASizedWindowHoldsAndOverflowsAtItsOwnSize(t *testing.T) {
	l := newLeg(t, sized, slowLeg, slowRTT)
	l.settle()
	st := snapshot(t, l.w, l.key)
	inFlight := st.sent - st.echoed
	if inFlight < st.size {
		// The settled sender keeps the window full; a tick without an echo
		// leaves it held.
		t.Fatalf("a settled sender has %d bytes in flight under its %d byte window", inFlight, st.size)
	}
	if ok, _, _ := l.w.Room(l.key, l.now); ok {
		t.Fatalf("Room lets a send go with %d bytes in flight on a %d byte window", inFlight, st.size)
	}
	if !l.w.Over(l.key, 0, l.now) && inFlight > st.size {
		t.Fatalf("Over does not see %d bytes in flight past a %d byte window", inFlight, st.size)
	}
}

func TestResetForgetsTheSize(t *testing.T) {
	l := newLeg(t, sized, slowLeg, slowRTT)
	l.settle()
	l.w.Reset(l.key)
	l.w.Epoch(l.key) // the next send opens it again
	if size := l.w.Size(l.key); size != DefaultWindow {
		t.Fatalf("a window reset starts at %d KiB, not the cap", size>>10)
	}
	st := snapshot(t, l.w, l.key)
	if st.leg == nil {
		t.Fatal("a sized window opened again has nothing to measure its leg with")
	}
	if st.leg.minRTT != 0 || !st.leg.rateSince.IsZero() || len(st.leg.stamps) != 0 {
		t.Fatalf("a reset window kept its measurements: min rtt %s, rate from %s, %d marks timed",
			st.leg.minRTT, st.leg.rateSince, len(st.leg.stamps))
	}
}

func TestAnEchoWithoutAClockNeverResizes(t *testing.T) {
	l := newLeg(t, sized, slowLeg, slowRTT)
	l.timed = false
	l.run(60*time.Second, 10*time.Millisecond)
	if size := l.w.Size(l.key); size != DefaultWindow {
		t.Fatalf("echoes with no clock resized the window to %d KiB", size>>10)
	}
}

func TestWithoutATargetQueueTheWindowIsFixed(t *testing.T) {
	l := newLeg(t, Timing{}, slowLeg, slowRTT)
	l.run(60*time.Second, 10*time.Millisecond)
	if size := l.w.Size(l.key); size != DefaultWindow {
		t.Fatalf("a window with no TargetQueue changed size to %d KiB", size>>10)
	}
	if got := l.w.Timing().MinWindow; got != 0 {
		t.Fatalf("a Timing with no TargetQueue took a MinWindow of %d", got)
	}
}

func TestSizeOfAWindowNotOpenIsTheCap(t *testing.T) {
	w := New(sized)
	if size := w.Size("nobody"); size != DefaultWindow {
		t.Fatalf("Size of a window never opened is %d, want the %d cap", size, DefaultWindow)
	}
	if w.Len() != 0 {
		t.Fatal("Size opened a window")
	}
}
