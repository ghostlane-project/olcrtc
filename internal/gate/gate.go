// Package gate runs the engine the way users break it: a real relay, a real
// server, bulk transfer with connects on top, a phone's memory budget, and a
// report a release is compared against. See the design in the olcbox
// repository, docs/superpowers/specs/2026-09-15-release-gate-design.md.
package gate

import (
	"cmp"
	"context"
	"net"
	"net/http"
	"slices"
	"sync"
	"time"
)

// ai-generated: the whole file (the shared types, the scenario registry and
// the plan).

// Pair is one provider and one transport of it.
type Pair struct {
	Provider  string
	Transport string
}

// String names the pair provider/transport, the way subtests and logs do.
func (p Pair) String() string { return p.Provider + "/" + p.Transport }

// Endpoint is everything a client needs to join a server's room.
type Endpoint struct {
	Provider  string
	Transport string
	Room      string // a URL or an id, as the provider wants it
	Key       string
	DNS       string
	VP8FPS    int
	VP8Batch  int
}

// Tunnel is a running client: a SOCKS5 listener and a way to stop it.
type Tunnel struct {
	SocksAddr string
	Stop      func()
}

// Client is a way of running the engine's client: the CLI path or the phone's.
type Client interface {
	Name() string // "cli" or "mobile"
	Start(ctx context.Context, ep Endpoint) (*Tunnel, error)
}

// OpenOptions tunes how a target brings a server up. Only Local honours them.
type OpenOptions struct {
	// BridgeDelay makes the server open its relay bridge this much later than
	// it would; the child process is started with OLCRTC_TEST_BRIDGE_DELAY.
	BridgeDelay time.Duration
}

// Target is where the server side of a pair comes from.
type Target interface {
	Name() string     // "local" or "link"
	Platform() string // the first element of a cell id, e.g. "engine-linux"
	Pairs() []Pair
	// Open brings up the server side for p (or, for a link, checks that the
	// link is p) and returns what the client needs plus a stop function.
	Open(ctx context.Context, p Pair, dir string, opt OpenOptions) (Endpoint, func(), error)
	Load() LoadURLs
}

// LoadURLs is what the load scenarios pull from and push to through the tunnel.
type LoadURLs struct {
	Small    string // a ~1 KB resource
	Big      string // BigBytes long
	Sink     string // accepts POST bodies
	BigBytes int64
}

// Metrics is what a scenario measured, by the keys the verdict knows.
type Metrics map[string]float64

// DialFunc dials through the tunnel.
type DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// Env is one cell's world: the pair, the client, the tunnel and the helpers.
type Env struct {
	Target   Target
	Pair     Pair
	Client   Client
	Endpoint Endpoint
	Tunnel   *Tunnel
	Load     LoadURLs
	HTTP     *http.Client
	Dial     DialFunc
	Dir      string
	Logf     func(format string, args ...any)
	// Delayed opens a fresh server for the pair with the given options and
	// returns its endpoint; nil for targets that cannot (Link). S6 uses it.
	Delayed func(ctx context.Context, opt OpenOptions) (Endpoint, func(), error)
}

// Scenario is one named cell body.
type Scenario struct {
	ID   string
	Name string
	// Applies says whether the scenario runs for a target, a pair and a
	// client flavour; nil means it runs everywhere.
	Applies func(target Target, p Pair, client string) bool
	Run     func(ctx context.Context, env *Env) (Metrics, error)
}

var (
	registryMu sync.Mutex //nolint:gochecknoglobals // guards registry
	registry   []Scenario //nolint:gochecknoglobals // filled from init in scenarios.go; tests swap it
)

// Register adds a scenario. Called from init in scenarios.go.
func Register(s Scenario) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry = append(registry, s)
}

// Scenarios returns the registered scenarios sorted by ID.
func Scenarios() []Scenario {
	registryMu.Lock()
	out := slices.Clone(registry)
	registryMu.Unlock()
	slices.SortFunc(out, func(a, b Scenario) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

// CellID names a cell: platform/provider/transport/client/scenario. Never a
// room, a key or a link: this is what the report and the release show.
func CellID(platform string, p Pair, client, scenario string) string {
	return platform + "/" + p.Provider + "/" + p.Transport + "/" + client + "/" + scenario
}

// PlanCells enumerates every cell a target and a set of clients will run, in
// scenario order, so the recorder can hold each one to an outcome.
func PlanCells(t Target, clients []string) []Cell {
	platform, pairs := t.Platform(), t.Pairs()
	var cells []Cell
	for _, s := range Scenarios() {
		for _, p := range pairs {
			for _, c := range clients {
				if s.Applies != nil && !s.Applies(t, p, c) {
					continue
				}
				cells = append(cells, Cell{
					ID: CellID(platform, p, c, s.ID), Platform: platform,
					Provider: p.Provider, Transport: p.Transport, Client: c, Scenario: s.ID,
					Status: StatusPlanned,
				})
			}
		}
	}
	return cells
}
