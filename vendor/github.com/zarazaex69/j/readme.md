<div align="center">

<img src="asset/logo.png" width="150">

![License](https://img.shields.io/badge/license-ZARAZAEX%20ANY%20DO-0D1117?style=flat-square&logo=open-source-initiative&logoColor=green&labelColor=0D1117)
![Golang](https://img.shields.io/badge/-Golang-0D1117?style=flat-square&logo=go&logoColor=00A7D0)

</div>

## j

Low-level Go library for programmatic connection to **Jitsi Meet** calls.
Handles XMPP signaling, joins MUC, catches Jingle session-initiate, parses
SDP/ICE/SSRC, opens bridge channel (colibri-ws) and integrates with
[pion/webrtc](https://github.com/pion/webrtc) for media.

Built for the [olcRTC](https://github.com/openlibrecommunity/olcrtc) project.

No hardcoded addresses/rooms — everything is user-supplied.

## Features

- Anonymous SASL → XMPP MUC join → focus → Jingle session-initiate
- Auto-discovery of MUC domain from server `config.js` (works with both `conference.host` and `muc.host`)
- Full Jingle XML ↔ SDP converter (BUNDLE, RTP payload-types, RTCP-FB, SSRC, fingerprint, ICE candidates, rtcp-mux filtering)
- Binds `*webrtc.PeerConnection` (pion) to Jingle session: automatic session-accept, **trickle ICE**, **reconnect** on `session-terminate moving`
- **Sending sendonly video track** (included in session-accept SDP, Jicofo distributes to other participants)
- **Receiving video**: `RequestVideo(ctx, maxHeight)` — sends `ReceiverVideoConstraints` via bridge channel (without this JVB will NOT forward video!)
- **Plan B detection**: `Negotiator.Accept` detects Plan B offers, `peer.IsPlanBError(err)` for handling
- `source-add` / `source-remove` / `transport-info` / `session-terminate` helpers
- **Bridge channel with two transports** (selected automatically based on what server offers):
  - **colibri-ws** (modern, default for new deployments) — WebSocket to JVB
  - **SCTP datachannel** (legacy, used by older Jitsi deployments) — pion DataChannel to JVB
  - Both share the same `colibri.Bridge` interface — broadcast/unicast `EndpointMessage`, **raw bytes** via base64
- Groupchat, raise/lower hand, leave
- Low-level XMPP API (`Send(rawXML)`, `SendJingle`, `NextID`, `LastJingleStanza`)
- `-insecure` flag to skip TLS verification (handy for self-signed certs in testing)
- CLI with 6 modes for testing and benchmarking

## colibri-ws Throughput (via JVB)

On `meet.cryptopro.ru` (single bridge):

| payload | rx steady | tx side | notes |
|---|---|---|---|
| 8 KB | **~135 Mbit/s** | ~220 Mbit/s | stable, no drops |
| 16 KB | 80–160 Mbit/s | ~195 Mbit/s | fluctuates |
| 64 KB | — | — | bridge closes connection (max-message-size) |

Up to ~1 Gbit/s achievable with close geolocation to JVB and multiple parallel endpoints.

## Structure

```
j/
├── j.go                     # public API: Join, JoinMUC, Session, Negotiator()
├── internal/
│   ├── xmpp/                # WS + ANONYMOUS SASL + bind + MUC + focus + Stream Mgmt + raw Jingle/IQ helpers
│   ├── jingle/              # Jingle XML ↔ SDP (BUNDLE, RTP, RTCP-FB, SSRC, fingerprint, candidates)
│   ├── colibri/             # bridge channel WebSocket — JVB protocol (EndpointMessage, LastN, …)
│   └── peer/                # pion PeerConnection ↔ Jingle bridge (Accept, trickle ICE, source-add)
├── cmd/cli/                 # CLI: jingle | chat | dc | dc-raw | media (+send-video) | bench
└── readme.md
```

## Protocol

```
WebSocket wss://host/xmpp-websocket?room=ROOM   (subprotocol: xmpp)
   │
   ├─ ANONYMOUS SASL → bind → session → Stream Management (XEP-0198)
   ├─ extdisco:2 → TURN/STUN credentials
   ├─ focus.host conference allocation
   ├─ MUC join (presence + codecList + SourceInfo + nick + caps)
   ├─ ← Jingle session-initiate (SDP-as-XML, ICE candidates, colibri-ws URL or SCTP map)
   ├─ → Jingle session-accept (with pion-generated SDP→Jingle)
   ├─ → Jingle transport-info (trickle late candidates)
   ├─ → Jingle source-add / source-remove (for late tracks)
   ├─ ← Jingle session-terminate (reason="moving" → reconnect)
   │
   └─ Bridge channel (one of):
      │
      ├─ colibri-ws (wss URL from session-initiate) ──→ JVB
      │      ClientHello / ServerHello / EndpointMessage / EndpointStats / …
      │
      └─ SCTP datachannel (pion DataChannel "JVB data channel") ──→ JVB
             Same JSON protocol, but transport is dc.SendText() (NOT binary!)
             — JVB only processes DataChannelStringMessage, ignores binary frames.
```

## Usage

### Config

```go
type Config struct {
    Host     string  // Jitsi host, e.g. "meet.example.com"
    Room     string  // room name (no @domain)
    Nick     string  // display name
    Debug    bool    // verbose XMPP/WS logging
    Insecure bool    // skip TLS verification (self-signed / expired certs)
}
```

`Host` is enough — j fetches `https://<host>/config.js` to discover the actual MUC
domain (`muc.host` or `conference.host`). Override with `-debug` to see the choice.

### Chat / MUC

`j.JoinMUC` — XMPP only, no Jingle (doesn't wait for session-initiate).

```go
sess, _ := j.JoinMUC(ctx, j.Config{Host: "meet.example.com", Room: "myroom", Nick: "thejproject"})
defer sess.Close()

sess.Chat("hello")
sess.RaiseHand()
sess.LowerHand()

for m := range sess.Messages() {
    fmt.Printf("<%s> %s\n", m.From, m.Body)
}
```

### Bridge channel (JVB data channel)

The bridge channel carries control/data messages between participants and JVB.
`j` supports both transports automatically — pick the one your server uses.

#### colibri-ws (modern, default)

```go
sess, _ := j.Join(ctx, j.Config{...})           // waits for Jingle (needs ≥1 other participant)
defer sess.Close()

if sess.ColibriWS != "" {
    sess.OpenBridge(ctx)  // dials wss://<jvb>/colibri-ws/...
}
```

#### SCTP datachannel (legacy)

If `sess.ColibriWS` is empty, the server uses SCTP. The DataChannel must be
created **before** `Negotiator.Accept` so it ends up in the SDP answer:

```go
pc, _ := webrtc.NewPeerConnection(sess.IceConfig())

// 1. Create DC before Accept (so it's in the SDP)
sess.PrepareBridgeSCTP(pc)

// 2. Negotiate SDP / send session-accept
neg := sess.Negotiator()
neg.PC = pc
neg.Accept(ctx)

// 3. Wait for DC to open + ServerHello from JVB
sess.WaitBridgeSCTP(ctx)
```

#### Sending and receiving (same API for both)

```go
// raw bytes — broadcast to all
sess.BridgeSendRaw("", []byte{0xDE, 0xAD, 0xBE, 0xEF})
// unicast to specific endpoint
sess.BridgeSendRaw("2968719f", payload)

// JSON EndpointMessage with arbitrary fields (bridge doesn't parse, relays as-is)
sess.BridgeSendMessage("", map[string]any{"type": "chat", "text": "hi"})

for m := range sess.BridgeMessages() {
    if raw := colibri.DecodeRaw(m); raw != nil {
        // received raw bytes from peer
    }
}
```

Low-level bridge (works with both transports via `colibri.Bridge` interface):
```go
br := sess.Bridge()
br.SendLastN(8)
br.SendVideoType("camera")        // "camera" | "desktop" | "none"
br.SendReceiverVideoConstraints(map[string]any{ /* … */ })
br.SendJSON(anyJSONserialisable)
```

### pion integration: receiving + sending media

```go
import "github.com/pion/webrtc/v4"

pc, _ := webrtc.NewPeerConnection(sess.IceConfig())

// receive audio
pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio,
    webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})

// send video — pion puts ssrc/cname/msid in SDP, our Negotiator
// automatically converts it to <source> inside session-accept,
// Jicofo distributes to other participants
videoTrack, _ := webrtc.NewTrackLocalStaticSample(
    webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
    "myvideo", "mystream")
pc.AddTrack(videoTrack)

pc.OnTrack(func(t *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
    buf := make([]byte, 1500)
    for {
        _, _, err := t.Read(buf)
        if err != nil { return }
        // RTP packet from remote participant — process as needed
    }
})

neg := sess.Negotiator()
neg.PC = pc
if err := neg.Accept(ctx); err != nil { panic(err) }
defer neg.Terminate("success")

// IMPORTANT: without this JVB will NOT forward video!
sess.RequestVideo(ctx, 720)

// ICE candidates discovered AFTER session-accept are automatically
// sent via transport-info (trickle ICE)

// send VP8 frames
go func() {
    for { videoTrack.WriteSample(media.Sample{Data: vp8Frame, Duration: 33*time.Millisecond}) }
}()
```

### Receiving video (ReceiverVideoConstraints)

**Critical**: JVB does not forward video until the receiver sends
`ReceiverVideoConstraints` via bridge channel. Without this, `OnTrack` will never fire.

```go
// Simple — request all video at 720p:
sess.RequestVideo(ctx, 720)

// Or manually via bridge for fine-grained control:
sess.OpenBridge(ctx)
sess.Bridge().SendJSON(map[string]any{
    "colibriClass":       "ReceiverVideoConstraints",
    "lastN":              3,                              // max 3 video streams
    "onStageSources":     []string{"alice-v0"},           // prioritized source
    "defaultConstraints": map[string]any{"maxHeight": 180},
    "constraints": map[string]any{
        "alice-v0": map[string]any{"maxHeight": 720},     // alice in HD
    },
})
```

### Plan B (multiple participants with video)

When there are already participants with video in the room, Jicofo sends the offer in
**Plan B** format (multiple SSRCs in a single `m=video`). pion defaults to Unified Plan and will fail.

```go
neg := sess.Negotiator()
neg.PC = pc
err := neg.Accept(ctx)
if peer.IsPlanBError(err) {
    // Recreate PC with Plan B semantics
    pc.Close()
    cfg := sess.IceConfig()
    cfg.SDPSemantics = webrtc.SDPSemanticsPlanB
    pc, _ = webrtc.NewPeerConnection(cfg)
    // ... add transceivers/tracks again ...
    neg = sess.Negotiator()
    neg.PC = pc
    neg.Accept(ctx)
}
```

CLI `-media` handles this automatically.

### Reconnect loop (session-terminate moving)

Jicofo sometimes switches to another bridge (`session-terminate reason="moving"`).
`Session.WaitJingleReinitiate(ctx)` blocks until the next `session-initiate`:

```go
for {
    pc, _ := webrtc.NewPeerConnection(sess.IceConfig())
    // … add tracks/transceivers …
    neg := sess.Negotiator()
    neg.PC = pc
    neg.Accept(ctx)

    // Wait until pc.OnConnectionStateChange hits Failed/Closed
    waitForFailed(pc)
    pc.Close()
    neg.Terminate("success")

    if _, err := sess.WaitJingleReinitiate(ctx); err != nil { return }
    // loop — next session-initiate
}
```

CLI `-media` does this automatically.

### Late tracks: source-add

If you add a track **after** session-accept:

```go
pc.AddTrack(newTrack)
sdp := pc.LocalDescription().SDP   // pion regenerates with new SSRC
neg.SendSourceAddFromSDP(sdp)      // → <jingle action="source-add"> to Jicofo
```

If the track is added **before** Accept — it's included in session-accept SDP automatically, source-add is not needed (otherwise Jicofo returns `SSRC is already used`).

### Low-level XMPP

```go
xc := sess.LowLevel()                // *xmpp.Conn
xc.Send(`<message …>…</message>`)    // any stanza with xmlns="jabber:client"
xc.SendJingle(to, "transport-info", sid, initiator, innerXML)
id := xc.NextID()                    // monotonic id for IQ
stanza := xc.LastJingleStanza()      // raw <iq><jingle action="session-initiate"…/></iq>
```

## CLI

```sh
go build -o jcli ./cmd/cli
```

| Mode | Description |
|---|---|
| (no flag) | Wait for Jingle and output JSON with SDP/ICE/SSRC/colibriWS |
| `-chat` | MUC chat: stdin → groupchat. Commands: `/raise`, `/lower`, `/quit` |
| `-dc` | Bridge channel: stdin (text) → broadcast `EndpointMessage{text:line}` |
| `-dc-raw` | Bridge channel raw: pipe raw bytes between two CLIs via JVB |
| `-media` | pion + session-accept + reconnect loop. Receives RTP from other tracks |
| `-media -send-video` | same + sendonly VP8 track (dummy keyframe loop) with auto SSRC announcement |
| `-bench` | colibri-ws throughput benchmark (`-bench-size`, `-bench-secs`) |

```sh
# chat
./jcli -host meet.example.com -room myroom -nick alice -chat

# pipe raw bytes between two CLIs via JVB
./jcli -host meet.example.com -room myroom -nick alice -dc-raw <input.bin
./jcli -host meet.example.com -room myroom -nick bob   -dc-raw >output.bin

# receive + send video to room (needs ≥1 other participant in room)
./jcli -host meet.example.com -room myroom -nick mediabot -media -send-video

# bridge channel throughput benchmark
./jcli -host meet.example.com -room myroom -nick recv -bench -bench-secs 30  &
./jcli -host meet.example.com -room myroom -nick send -bench -bench-size 8192 -bench-secs 20

# common flags
-host           Jitsi server
-room           room name
-nick           display name
-debug          verbose XMPP/WS logging
-insecure       skip TLS certificate verification (self-signed / expired certs)
-timeout 5m     how long to wait for Jingle session-initiate
```

## Dependencies

- Go 1.21+
- `github.com/coder/websocket`
- `github.com/pion/webrtc/v4` (for media)

## Build

```sh
git clone https://github.com/zarazaex69/j
cd j
go build ./...
```

## Out of scope

- `provider.Provider` adapter for olcRTC (that's part of olcRTC, not `j`)
- `vp8channel` / `seichannel` / `videochannel` transports (also olcRTC, we only provide a sendable VideoTrack)
- TLS fingerprint Chrome / XHR telemetry for TSPU evasion — higher-level concern (utls, connection wrappers)

<div align="center">

---

### Contact

Telegram: [zarazaex](https://t.me/zarazaexe)
<br>
Email: [zarazaex@tuta.io](mailto:zarazaex@tuta.io)

</div>
