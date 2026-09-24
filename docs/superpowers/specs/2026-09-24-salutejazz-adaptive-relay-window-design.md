# A relay window sized for the leg (olcrtc#49)

**Status:** approved design, 2026-09-24
**Repository:** engine `ghostlane-project/olcrtc`, branch `proofkit`
**Issue:** olcrtc#49 (open since 2026-09-22; the first half shipped as 7b78fd4a)

## Problem

The SaluteJazz relay window (`internal/relaywin`, used by
`internal/engine/salutejazz/window.go`) lets a sender hand Sber's SFU at most
192 KiB for one destination that the destination has not echoed. The size is
fixed. It was chosen so that a ping behind one window and its pong behind
another fit the tunnel's 15 s liveness timeout on the slowest leg the gate had
then measured, 34 kB/s.

Legs are slower than that. The engine gate on 7b78fd4a (run 35862192631,
GitHub runner) measured 26-62 kB/s from the SFU to the receiver, with a 3-7 s
echo round trip. At 26 kB/s one window is 7.5 s of queue each way: the client
missed three pongs under a download at saturation (S2), closed the session on
liveness, and all six pulls ended at about 830 kB. A connect on top of the
transfer waited 46 s at p95.

A smaller fixed window is no answer: at the 0.35 s round trip of a normal leg a
window carries at most a window a round trip, and 192 KiB is already what the
gate's 1.4 and 1.6 Mbit/s floors need. Excusing more late pongs is no answer
either: a client would sit on a dead session for minutes again (olcbox#25).

## Goal

On any leg, a pong and the first bytes of a new stream wait behind at most
about `TargetQueue` of data in each direction, and a fast leg keeps the
throughput it has today. Carriers that do not ask for it (Jitsi keeps its own
copy of the mechanism) behave exactly as before.

## Design

### The window follows the leg

Each destination's window gets its own size, recomputed from its echoes:

    window = clamp(rate × (minRTT + TargetQueue), MinWindow, Window)

- **rate** is the rate the destination takes bytes off the relay: echoed bytes
  over the time they took, measured only over intervals in which a sender was
  held on the window. A sender that is not held is sending less than the leg
  takes, and its rate says nothing about the leg; the window stays as it is.
- **minRTT** is the shortest echo round trip this window has seen: the leg's
  own delay, without the queue. It only goes down, until the window is Reset.
  A route that grows longer makes the window smaller than it needs to be,
  which costs rate and never costs a pong.
- **TargetQueue** is how much queue the window keeps on purpose: 1.5 s. On a
  leg the window saturates, what is in flight settles at the leg's
  bandwidth-delay product plus that much queue. On a leg the window does not
  saturate the queue is near zero, the measured rate is the window's own, and
  the formula grows the window by `(minRTT + TargetQueue) / RTT` per step until
  either the leg saturates or the window reaches its cap.
- **MinWindow**, 16 KiB, keeps a very slow leg moving; **Window**, 192 KiB,
  stays the cap, so a fast leg is sized exactly as today.

A sized window starts at 64 KiB, not at the cap: what a new window hands the
relay before it has measured anything is queued however slow the leg, and a
whole 192 KiB is 7.6 s at 26 kB/s (on the six-pull tunnel test that start alone
closed the session). A fast leg grows it to the cap within one measured
interval, by `(minRTT + TargetQueue) / RTT`. Marks follow the size: at the cap
every `MarkEvery` as today, a smaller window proportionally, never under 2 KiB.

### Where it lives

`relaywin` is the mechanism for every carrier after Jitsi, so the sizing goes
there, off unless a carrier asks for it:

- `Timing` gains `TargetQueue time.Duration` (zero keeps the fixed window) and
  `MinWindow uint64`.
- `state` keeps the window's current size, the send time of each mark in
  flight (it already decides when a mark is due, in `Sent` and `Room`), the
  shortest echo round trip, and the start of the current rate interval.
- `ApplyEchoAt(key, counter, now)`, a timed `ApplyEcho`, times the echo
  against its mark and recomputes the size at the end of each rate interval;
  `ApplyEcho` keeps its signature and measures nothing.
- `Room`, `Sent` and `Over` read the destination's own size and mark interval
  instead of the Timing's.
- `Size(key)` reports a destination's current window, for the debug log.

`salutejazz` sets `TargetQueue` and `MinWindow` in `defaultRelayTiming`, and
its debug report adds the window size to the rate and round trip it already
logs.

#### The other half of a pong's wait: smux

The window bounds the relay's queue, not all of a pong's wait. On datachannel
the control stream shares the data session, and smux writes one record per
stream in turn (class first, then the order the writes came in), each stream
with one record at a time. So a pong waits for a record from every busy
stream - with six pulls on a 26 kB/s leg, 2.8 s - and may first wait for its
own stream's ping to go the same way. The measurements on the tunnel test
(26 kB/s, six pulls, the tunnel's own liveness) are a pong in 5-7 s, against
10.4 s with the fixed window. A probe much faster than the tunnel's own cannot
keep up on such a leg: the control stream gets about one record out every
2.8 s there. This is also the limit of the change: with many more busy streams
on a leg this slow, even the tunnel's own probes queue up. A control stream
that goes ahead of data (a control plane of its own, as vp8channel has, or a
priority in smux) is a separate change.

## Compatibility

Nothing on the wire changes: the size is the sender's own. A peer on any build
sends and echoes marks as before; only how much this sender lets itself put in
flight differs. So each end can move on its own:

- **Servers first.** In a download, the case that fails, the sender is the
  server. A server with this engine fixes downloads for every client, old ones
  and iOS included, with no app release.
- **Clients later**, with the next engine pin in the app: that half covers
  uploads, which pass the gate today (S3).

## Tests

- `relaywin`: with `TargetQueue` zero every existing test passes unchanged.
  With it set: a leg that echoes at 26 kB/s shrinks the window to about
  `26 kB/s × (minRTT + TargetQueue)` and no lower than `MinWindow`; a leg that
  speeds up grows it back to the cap; a sender that is never held does not move
  it; marks follow the size; `Reset` forgets the round trip and the rate.
- `salutejazz`, the whole tunnel over the fake SFU: the existing slow-leg test
  keeps passing, and a new one on 26 kB/s legs with six parallel pulls misses
  no pong at the tunnel's real liveness (10 s probes, 15 s timeout); every
  settled pong comes back within `2 × TargetQueue`, two smux turns and a
  second; the leg to the client holds under three quarters of the cap (61 KiB
  measured, the fixed window holds all of it); the pulls keep three quarters
  of the leg (98 % measured).
- The window budget test pins the new guarantee: a pong's worst wait is set by
  `TargetQueue`, not by the slowest leg ever measured.
- The engine gate on live rooms: salutejazz S2 and S3. S2's `on_top_p95_ms`
  miss on a normal leg (5.7 s against 5 s on 1.0.440, where the queue is well
  under a second) is not explained by the window; it stays on `known.go`
  under #49 until the gate says otherwise.

## Rollout

1. The engine change on its own branch; a PR the user merges into `proofkit`.
2. The engine gate on live rooms, never while an app release gate runs (the
   two share the rooms).
3. A server build (the next metering image) and an agent version that pins
   it; a canary server, then the fleet by Force Update, which the user
   confirms.
4. The app picks the engine up with its next pin.
