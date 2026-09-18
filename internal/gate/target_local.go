package gate

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ai-generated: the whole file (the local target: cmd/olcrtc built once with
// the test hooks and run as a mode: srv child per pair, from a private
// directory, with only a scrubbed copy of its log handed out).

// EnvWBStreamToken names the variable the WB account token comes from. The
// entry reads it and passes it in LocalOptions: never a flag, so the token
// shows in no process list and no test log (amendment A2).
const EnvWBStreamToken = "OLCRTC_GATE_WBSTREAM_TOKEN"

var (
	// ErrLocalOptions is a local target asked for a plan it cannot run: a
	// name it does not know, a provider left with no transport, a transport
	// no provider carries. Each would be a cell that could never pass.
	ErrLocalOptions = errors.New("local target options")
	// ErrPairNotCarried is a pair a target does not offer.
	ErrPairNotCarried = errors.New("pair not carried by the target")
	// ErrNoWBStreamToken fails every wbstream pair of a run without the
	// token: a guest cannot be the first participant of an idle WB room.
	ErrNoWBStreamToken = errors.New("wbstream: " + EnvWBStreamToken +
		" is not set (WB refuses a guest as the first participant of an idle room)")
	// ErrServerExited is a server that ended before it joined its room.
	ErrServerExited = errors.New("server exited before the link connected")
	// ErrLineNotSeen is a log that did not show the line waited for in time.
	ErrLineNotSeen = errors.New("line not seen in the log")
)

// Target and platform names, the first two parts of a cell's identity.
const (
	targetLocal         = "local"
	targetLink          = "link"
	platformEngineLinux = "engine-linux"
)

// Provider and transport names, as the engine's YAML spells them.
const (
	providerJitsi    = "jitsi"
	providerTelemost = "telemost"
	providerWBStream = "wbstream"

	transportData  = "datachannel"
	transportVideo = "videochannel"
	transportSEI   = "seichannel"
	transportVP8   = "vp8channel"
)

const (
	// defaultDNS is the resolver the servers use when none is given.
	defaultDNS = "8.8.8.8:53"
	// linkedLine is what the server logs once it has joined its room.
	linkedLine = "Link connected"
	// linkBudget is how long a server may take to join its room.
	linkBudget = 60 * time.Second
	// stopGrace is how long a server has to leave its room after SIGTERM
	// before it is killed, so no ghost of it stays in a pool room for the
	// next pair.
	stopGrace = 10 * time.Second
	// linePoll is how often a log is read again for the line waited for.
	linePoll = 200 * time.Millisecond
	// tailBytes bounds the server line an error quotes.
	tailBytes = 400
	// serverLogName is the server's log, raw in the private directory and
	// scrubbed in the pair's.
	serverLogName = "srv.log"
	// bridgeDelayEnv is what internal/testhooks reads in the child.
	bridgeDelayEnv = "OLCRTC_TEST_BRIDGE_DELAY"
)

// transportsOf is which transports a provider carries (amendment A1, the
// engine's real E2E expectations, confirmed live on 2026-09-18). Telemost
// drops SCTP, so no datachannel, and seichannel fails there by design; WB
// guests cannot publish data, so no datachannel there either.
func transportsOf(provider string) []string {
	switch provider {
	case providerJitsi:
		return []string{transportData, transportVideo, transportSEI, transportVP8}
	case providerTelemost:
		return []string{transportVP8, transportVideo}
	case providerWBStream:
		return []string{transportVP8, transportVideo, transportSEI}
	default:
		return nil
	}
}

// LocalOptions configures a target whose servers are child processes.
type LocalOptions struct {
	ModuleRoot string // the engine repository root, where go build runs
	// WorkDir is private: the server binary, each server's YAML (room, key,
	// WB token) and its raw log. Never upload it (amendment A9).
	WorkDir       string
	Instances     string   // docs/jitsi.instances.yaml
	JitsiHosts    []string // used instead of Instances when it names a host
	TelemostRooms []string // pool: bare ids or room URLs
	WBStreamRooms []string // pool: bare ids or room URLs
	// WBStreamToken is the WB account token the server joins with; the
	// client stays a guest, as the app is. From EnvWBStreamToken.
	WBStreamToken string
	RunNumber     int
	Providers     []string
	Transports    []string
	DNS           string // default 8.8.8.8:53
	Origin        *Origin
}

// LocalTarget builds cmd/olcrtc once and runs it as mode: srv per pair.
type LocalTarget struct {
	opts       LocalOptions
	pairs      []Pair
	jitsiHosts []string
	probe      func(ctx context.Context, host string) bool
	linkWait   time.Duration

	buildMu sync.Mutex
	binary  string
}

// NewLocalTarget checks the plan and prepares the target; the server is
// built on the first Open (or by Build). Pairs are the providers crossed
// with the transports in the order given, less the combinations a provider
// does not carry.
func NewLocalTarget(opts LocalOptions) (*LocalTarget, error) {
	if opts.WorkDir == "" {
		return nil, fmt.Errorf("%w: no work directory", ErrLocalOptions)
	}
	work, err := filepath.Abs(opts.WorkDir)
	if err != nil {
		return nil, fmt.Errorf("work directory: %w", err)
	}
	opts.WorkDir = work
	if opts.DNS == "" {
		opts.DNS = defaultDNS
	}
	pairs, err := planPairs(opts.Providers, opts.Transports)
	if err != nil {
		return nil, err
	}
	t := &LocalTarget{opts: opts, pairs: pairs, probe: ProbeHTTPS, linkWait: linkBudget}
	if slices.Contains(opts.Providers, providerJitsi) {
		if t.jitsiHosts, err = JitsiHosts(opts.Instances, opts.JitsiHosts); err != nil {
			return nil, err
		}
	}
	return t, nil
}

// planPairs crosses providers with transports in the order given and drops
// what a provider does not carry. An unknown or repeated name, a provider
// left with no transport and a transport no provider carries are errors.
func planPairs(providers, transports []string) ([]Pair, error) {
	if err := checkNames("provider", providers, func(p string) bool { return transportsOf(p) != nil }); err != nil {
		return nil, err
	}
	known := transportsOf(providerJitsi)
	if err := checkNames("transport", transports, func(tr string) bool { return slices.Contains(known, tr) }); err != nil {
		return nil, err
	}
	var pairs []Pair
	carried := make(map[string]bool, len(transports))
	for _, p := range providers {
		n := len(pairs)
		for _, tr := range transports {
			if slices.Contains(transportsOf(p), tr) {
				pairs = append(pairs, Pair{Provider: p, Transport: tr})
				carried[tr] = true
			}
		}
		if len(pairs) == n {
			return nil, fmt.Errorf("%w: %s carries none of %s", ErrLocalOptions, p, strings.Join(transports, ","))
		}
	}
	for _, tr := range transports {
		if !carried[tr] {
			return nil, fmt.Errorf("%w: none of %s carries %s", ErrLocalOptions, strings.Join(providers, ","), tr)
		}
	}
	return pairs, nil
}

// checkNames refuses an empty list, a name known does not accept and a name
// given twice.
func checkNames(kind string, names []string, known func(string) bool) error {
	if len(names) == 0 {
		return fmt.Errorf("%w: no %s", ErrLocalOptions, kind)
	}
	for i, name := range names {
		if !known(name) {
			return fmt.Errorf("%w: unknown %s %q", ErrLocalOptions, kind, name)
		}
		if slices.Contains(names[:i], name) {
			return fmt.Errorf("%w: %s %s listed twice", ErrLocalOptions, kind, name)
		}
	}
	return nil
}

// Name implements Target.
func (t *LocalTarget) Name() string { return targetLocal }

// Platform implements Target.
func (t *LocalTarget) Platform() string { return platformEngineLinux }

// Pairs implements Target.
func (t *LocalTarget) Pairs() []Pair { return slices.Clone(t.pairs) }

// Load is the loopback origin's URLs; none without an origin.
func (t *LocalTarget) Load() LoadURLs {
	if t.opts.Origin == nil {
		return LoadURLs{}
	}
	return t.opts.Origin.URLs
}

// Build compiles the server with the test hooks into WorkDir, once.
func (t *LocalTarget) Build(ctx context.Context) error {
	_, err := t.build(ctx)
	return err
}

func (t *LocalTarget) build(ctx context.Context) (string, error) {
	t.buildMu.Lock()
	defer t.buildMu.Unlock()
	if t.binary != "" {
		return t.binary, nil
	}
	out := filepath.Join(t.opts.WorkDir, "olcrtc")
	cmd := exec.CommandContext(ctx, "go", "build", "-tags", "olcrtc_testhooks", "-o", out, "./cmd/olcrtc")
	cmd.Dir = t.opts.ModuleRoot
	if b, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("build server: %w\n%s", err, b)
	}
	t.binary = out
	return out, nil
}

// Open picks a room for the pair and starts its server from a private
// directory, returning once the server has joined the room. The stop
// function ends the server and leaves a scrubbed copy of its log in dir as
// srv.log; a server that never joins leaves the same, and the error quotes
// its last line (the provider's answer) scrubbed too.
func (t *LocalTarget) Open(ctx context.Context, p Pair, dir string, opt OpenOptions) (Endpoint, func(), error) {
	if !slices.Contains(t.pairs, p) {
		return Endpoint{}, nil, fmt.Errorf("%w: %s", ErrPairNotCarried, p)
	}
	ep, err := t.endpoint(ctx, p)
	if err != nil {
		return Endpoint{}, nil, err
	}
	binary, err := t.build(ctx)
	if err != nil {
		return Endpoint{}, nil, err
	}
	secrets := t.secrets(ep)
	srv, err := startServer(ctx, binary, t.opts.WorkDir, RenderServerConfig(ep, t.token(p.Provider)), opt)
	if err != nil {
		return Endpoint{}, nil, err
	}
	stop := func() { srv.stop(filepath.Join(dir, serverLogName), secrets) }
	if err := waitForLine(srv.log, linkedLine, t.linkWait, srv.exited); err != nil {
		tail := clip(Scrub(lastLine(srv.log), secrets...))
		stop()
		if ctx.Err() != nil {
			return Endpoint{}, nil, fmt.Errorf("server: %w", ctx.Err())
		}
		return Endpoint{}, nil, fmt.Errorf("%w; last server line: %s", err, tail)
	}
	return ep, stop, nil
}

// endpoint is a fresh room, key and channel for the pair.
func (t *LocalTarget) endpoint(ctx context.Context, p Pair) (Endpoint, error) {
	room, err := t.room(ctx, p.Provider)
	if err != nil {
		return Endpoint{}, err
	}
	key, err := NewKey()
	if err != nil {
		return Endpoint{}, err
	}
	channel, err := NewChannel()
	if err != nil {
		return Endpoint{}, err
	}
	// VP8FPS and VP8Batch are the app's vp8channel numbers.
	return Endpoint{Provider: p.Provider, Transport: p.Transport, Room: room, Key: key, Channel: channel,
		DNS: t.opts.DNS, VP8FPS: 60, VP8Batch: 64}, nil
}

// room is where the pair's server goes: a fresh room on a Jitsi host that
// answers, or the pool entry of the run in the form its provider joins by.
func (t *LocalTarget) room(ctx context.Context, provider string) (string, error) {
	switch provider {
	case providerJitsi:
		return JitsiRoom(ctx, t.jitsiHosts, t.probe)
	case providerTelemost:
		entry, err := PoolRoom(t.opts.TelemostRooms, t.opts.RunNumber)
		if err != nil {
			return "", fmt.Errorf("telemost: %w", err)
		}
		return TelemostURL(entry), nil
	case providerWBStream:
		if t.opts.WBStreamToken == "" {
			return "", ErrNoWBStreamToken
		}
		entry, err := PoolRoom(t.opts.WBStreamRooms, t.opts.RunNumber)
		if err != nil {
			return "", fmt.Errorf("wbstream: %w", err)
		}
		id := WBStreamRoomID(entry)
		if id == "" {
			return "", fmt.Errorf("wbstream: %w: the entry names no room id", ErrPoolRoom)
		}
		return id, nil
	default:
		return "", fmt.Errorf("%w: provider %s", ErrPairNotCarried, provider)
	}
}

// token is the account token the provider's server joins with, if any.
func (t *LocalTarget) token(provider string) string {
	if provider != providerWBStream {
		return ""
	}
	return t.opts.WBStreamToken
}

// secrets is what a server's log and errors lose before they leave: the
// endpoint's, the WB token and the Jitsi hosts of an override, which come
// from a secret too.
func (t *LocalTarget) secrets(ep Endpoint) []string {
	out := append(ep.Secrets(), t.opts.WBStreamToken)
	if len(jitsiHostList(t.opts.JitsiHosts)) > 0 {
		out = append(out, t.jitsiHosts...)
	}
	return out
}

// RenderServerConfig writes the mode: srv YAML for an endpoint. token is a
// provider account token for the server alone (WB Stream), empty for none.
// vp8channel and seichannel take the app's options, and the UDP relay is on
// so a client's UDP ASSOCIATE reaches the exit.
func RenderServerConfig(ep Endpoint, token string) string {
	auth := "auth: { provider: " + ep.Provider + " }"
	if token != "" {
		auth = "auth: { provider: " + ep.Provider + ", token: " + strconv.Quote(token) + " }"
	}
	room := "room: { id: " + strconv.Quote(ep.Room) + " }"
	if ep.Channel != "" {
		room = "room: { id: " + strconv.Quote(ep.Room) + ", channel: " + strconv.Quote(ep.Channel) + " }"
	}
	lines := []string{
		"mode: srv", auth, room,
		"crypto: { key: " + strconv.Quote(ep.Key) + " }",
		"net: { transport: " + ep.Transport + ", dns: " + strconv.Quote(ep.DNS) + " }",
		"udp: { enabled: true }",
	}
	switch ep.Transport {
	case transportVP8: // the app's numbers, 60 and 64, where the endpoint names none
		lines = append(lines, fmt.Sprintf("vp8: { fps: %d, batch_size: %d }", cmp.Or(ep.VP8FPS, 60), cmp.Or(ep.VP8Batch, 64)))
	case transportSEI:
		lines = append(lines, "sei: { fps: 60, batch_size: 64, fragment_size: 900, ack_timeout_ms: 2000 }")
	}
	return strings.Join(append(lines, "debug: true"), "\n") + "\n"
}

// server is one child mode: srv process and the private directory it runs
// from: its YAML and its raw log.
type server struct {
	dir    string
	log    string
	cancel context.CancelFunc
	exited chan struct{}
	once   sync.Once
}

// startServer writes cfg into a fresh private directory under work and runs
// binary on it, stdout and stderr into the raw log. Cancelling ctx, or stop,
// sends SIGTERM so the server leaves its room; stopGrace later it is killed.
func startServer(ctx context.Context, binary, work, cfg string, opt OpenOptions) (*server, error) {
	dir, err := os.MkdirTemp(work, "srv-")
	if err != nil {
		return nil, fmt.Errorf("server directory: %w", err)
	}
	cfgPath, logPath := filepath.Join(dir, "srv.yaml"), filepath.Join(dir, serverLogName)
	logFile, err := createPrivate(cfgPath, cfg, logPath)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(runCtx, binary, cfgPath)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.Env = append(os.Environ(), bridgeDelayEnv+"="+opt.BridgeDelay.String())
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = stopGrace
	err = cmd.Start()
	_ = logFile.Close() // the child holds its own descriptor
	if err != nil {
		cancel()
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("start server: %w", err)
	}
	s := &server{dir: dir, log: logPath, cancel: cancel, exited: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		close(s.exited)
	}()
	return s, nil
}

// createPrivate writes the config and opens the log, both readable by the
// owner alone: the config holds the room, the key and the WB token.
func createPrivate(cfgPath, cfg, logPath string) (*os.File, error) {
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		return nil, fmt.Errorf("write server config: %w", err)
	}
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create server log: %w", err)
	}
	return f, nil
}

// stop ends the server, writes the scrubbed copy of its log to artifact and
// removes the private directory. Only the first call does anything.
func (s *server) stop(artifact string, secrets []string) {
	s.once.Do(func() {
		s.cancel()
		<-s.exited
		_ = ScrubFile(s.log, artifact, secrets...)
		_ = os.RemoveAll(s.dir)
	})
}

// WaitForLine polls a log file until it contains substr.
func WaitForLine(path, substr string, timeout time.Duration) error {
	return waitForLine(path, substr, timeout, nil)
}

// waitForLine is WaitForLine that also gives up when done closes: the
// process writing the log has ended, and the line will not come.
func waitForLine(path, substr string, timeout time.Duration, done <-chan struct{}) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(linePoll)
	defer tick.Stop()
	for {
		if logHas(path, substr) {
			return nil
		}
		select {
		case <-done:
			return ErrServerExited
		case <-deadline.C:
			return fmt.Errorf("%w: %q within %s", ErrLineNotSeen, substr, timeout)
		case <-tick.C:
		}
	}
}

// logHas reports whether the log at path contains substr yet.
func logHas(path, substr string) bool {
	raw, err := os.ReadFile(path)
	return err == nil && strings.Contains(string(raw), substr)
}

// lastLine is the last non-blank line of a log.
func lastLine(path string) string {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "(no log: " + err.Error() + ")"
	}
	text := strings.TrimSpace(string(raw))
	return strings.TrimSpace(text[strings.LastIndexByte(text, '\n')+1:])
}

// clip cuts a line an error quotes to tailBytes. Scrub before clipping: a
// cut through a secret would leave its head unscrubbed.
func clip(line string) string {
	if len(line) <= tailBytes {
		return line
	}
	return strings.ToValidUTF8(line[:tailBytes], "") + "..."
}
