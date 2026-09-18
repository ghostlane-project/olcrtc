package gate

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/openlibrecommunity/olcrtc/internal/app/session"
	"github.com/openlibrecommunity/olcrtc/internal/config"
)

// ai-generated: whole file, unit cover for the local target: its plan, its
// rooms, the server config it writes and the child it runs. A shell script
// stands in for cmd/olcrtc; every room, key and token here is made up.

func TestRenderServerConfigGolden(t *testing.T) {
	key := strings.Repeat("ab", 32)
	ep := Endpoint{Provider: "telemost", Transport: "vp8channel", Room: "https://telemost.yandex.ru/j/1",
		Key: key, Channel: "gate-000000000001", DNS: "8.8.8.8:53", VP8FPS: 60, VP8Batch: 64}
	want := `mode: srv
auth: { provider: telemost }
room: { id: "https://telemost.yandex.ru/j/1", channel: "gate-000000000001" }
crypto: { key: "` + key + `" }
net: { transport: vp8channel, dns: "8.8.8.8:53" }
udp: { enabled: true }
vp8: { fps: 60, batch_size: 64 }
debug: true
`
	if got := RenderServerConfig(ep, ""); got != want {
		t.Fatalf("config:\n%s\nwant:\n%s", got, want)
	}
	sei := RenderServerConfig(Endpoint{Provider: "jitsi", Transport: "seichannel", Room: "r", Key: key, DNS: "1.1.1.1:53"}, "")
	if !strings.Contains(sei, "sei: { fps: 60, batch_size: 64, fragment_size: 900, ack_timeout_ms: 2000 }") ||
		strings.Contains(sei, "vp8:") || strings.Contains(sei, "channel:") {
		t.Fatalf("sei config:\n%s", sei)
	}
	wb := RenderServerConfig(Endpoint{Provider: "wbstream", Transport: "vp8channel", Room: "fake-wb-room-1", Key: key,
		DNS: "8.8.8.8:53"}, "fake-wb-token")
	if !strings.Contains(wb, `auth: { provider: wbstream, token: "fake-wb-token" }`) ||
		!strings.Contains(wb, "vp8: { fps: 60, batch_size: 64 }") {
		t.Fatalf("wbstream config:\n%s", wb)
	}
}

// TestRenderedConfigsPassTheEnginesOwnLoader loads each rendered config the
// way cmd/olcrtc does, strict loader and validation included, so a key the
// schema does not know or a value it refuses fails here, not in a live run.
func TestRenderedConfigsPassTheEnginesOwnLoader(t *testing.T) {
	session.RegisterDefaults()
	key := strings.Repeat("cd", 32)
	for _, ep := range []Endpoint{
		{Provider: "jitsi", Transport: "datachannel", Room: "https://meet.example.invalid/gate-000000000001",
			Channel: "gate-000000000002", DNS: "8.8.8.8:53"},
		{Provider: "wbstream", Transport: "vp8channel", Room: "fake-wb-room-1", Channel: "gate-000000000003",
			DNS: "8.8.8.8:53", VP8FPS: 30, VP8Batch: 16},
		{Provider: "wbstream", Transport: "seichannel", Room: "fake-wb-room-1", DNS: "8.8.8.8:53"},
	} {
		ep.Key = key
		token := ""
		if ep.Provider == "wbstream" {
			token = "fake-wb-token"
		}
		path := filepath.Join(t.TempDir(), "srv.yaml")
		if err := os.WriteFile(path, []byte(RenderServerConfig(ep, token)), 0o600); err != nil {
			t.Fatal(err)
		}
		file, err := config.Load(path)
		if err != nil {
			t.Fatalf("%s: %v", ep, err)
		}
		cfg := session.ApplyDefaults(config.Apply(file))
		if err := session.Validate(cfg); err != nil {
			t.Fatalf("%s: %v", ep, err)
		}
		if cfg.Mode != "srv" || cfg.Provider != ep.Provider || cfg.Transport != ep.Transport || cfg.RoomID != ep.Room ||
			cfg.ChannelID != ep.Channel || cfg.KeyHex != key || cfg.DNSServer != ep.DNS || cfg.ProviderToken != token ||
			cfg.UDPDisabled || !file.Debug {
			t.Fatalf("%s: loaded %+v", ep, cfg)
		}
		if ep.Transport == "vp8channel" && (cfg.VP8.FPS != ep.VP8FPS || cfg.VP8.BatchSize != ep.VP8Batch) {
			t.Fatalf("%s: vp8 %+v", ep, cfg.VP8)
		}
		if ep.Transport == "seichannel" && (cfg.SEI.FragmentSize != 900 || cfg.SEI.AckTimeoutMS != 2000) {
			t.Fatalf("%s: sei %+v", ep, cfg.SEI)
		}
	}
}

func TestWaitForLine(t *testing.T) {
	p := filepath.Join(t.TempDir(), "srv.log")
	if err := os.WriteFile(p, []byte("starting\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		f, err := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			return
		}
		_, _ = f.WriteString("2026/09/15 Link connected\n")
		_ = f.Close()
	}()
	if err := WaitForLine(p, "Link connected", 3*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := WaitForLine(p, "never", 200*time.Millisecond); !errors.Is(err, ErrLineNotSeen) {
		t.Fatalf("WaitForLine returned %v without the line", err)
	}
}

func TestLocalTargetPairsFollowTheSupportTable(t *testing.T) {
	lt, err := NewLocalTarget(LocalOptions{
		WorkDir: t.TempDir(), JitsiHosts: []string{"meet.example.invalid"},
		Providers:  []string{"jitsi", "telemost", "wbstream"},
		Transports: []string{"datachannel", "videochannel", "seichannel", "vp8channel"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []Pair{
		{"jitsi", "datachannel"}, {"jitsi", "videochannel"}, {"jitsi", "seichannel"}, {"jitsi", "vp8channel"},
		{"telemost", "videochannel"}, {"telemost", "vp8channel"},
		{"wbstream", "videochannel"}, {"wbstream", "seichannel"}, {"wbstream", "vp8channel"},
	}
	if got := lt.Pairs(); !slices.Equal(got, want) {
		t.Fatalf("Pairs = %v\nwant %v", got, want)
	}
	if lt.Platform() != "engine-linux" || lt.Name() != "local" || lt.Load() != (LoadURLs{}) {
		t.Fatal("names or load")
	}
	two, err := NewLocalTarget(LocalOptions{WorkDir: t.TempDir(), JitsiHosts: []string{"meet.example.invalid"},
		Providers: []string{"wbstream", "jitsi"}, Transports: []string{"vp8channel", "datachannel"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := two.Pairs(); !slices.Equal(got, []Pair{{"wbstream", "vp8channel"}, {"jitsi", "vp8channel"},
		{"jitsi", "datachannel"}}) {
		t.Fatalf("Pairs = %v, want the given order less wbstream/datachannel", got)
	}
}

func TestLocalTargetRefusesAPlanItCannotRun(t *testing.T) {
	hosts := []string{"meet.example.invalid"}
	noHosts := filepath.Join(t.TempDir(), "instances.yaml")
	if err := os.WriteFile(noHosts, []byte("instances: []\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, opts := range map[string]LocalOptions{
		"only an unsupported pair":   {Providers: []string{"telemost"}, Transports: []string{"datachannel"}},
		"a provider left with none":  {Providers: []string{"jitsi", "telemost"}, Transports: []string{"datachannel"}},
		"a transport nobody carries": {Providers: []string{"wbstream"}, Transports: []string{"datachannel", "vp8channel"}},
		"an unknown provider":        {Providers: []string{"skype"}, Transports: []string{"vp8channel"}},
		"an unknown transport":       {Providers: []string{"jitsi"}, Transports: []string{"smokechannel"}},
		"a provider twice":           {Providers: []string{"jitsi", "jitsi"}, Transports: []string{"vp8channel"}},
		"no provider":                {Transports: []string{"vp8channel"}},
		"no transport":               {Providers: []string{"jitsi"}},
		"no jitsi host":              {Providers: []string{"jitsi"}, Transports: []string{"datachannel"}, JitsiHosts: []string{" "}},
		"no work directory":          {Providers: []string{"telemost"}, Transports: []string{"vp8channel"}},
	} {
		if name != "no work directory" {
			opts.WorkDir = t.TempDir()
		}
		want := ErrLocalOptions
		if name == "no jitsi host" { // a blank override and a list that names none
			opts.Instances, want = noHosts, ErrNoJitsiHost
		} else {
			opts.JitsiHosts = hosts
		}
		if _, err := NewLocalTarget(opts); !errors.Is(err, want) {
			t.Fatalf("%s: err = %v, want %v", name, err, want)
		}
	}
}

func TestLocalTargetRoomsFollowTheProvider(t *testing.T) {
	lt, err := NewLocalTarget(LocalOptions{
		WorkDir: t.TempDir(), Providers: []string{"jitsi", "telemost", "wbstream"}, Transports: []string{"vp8channel"},
		JitsiHosts:    []string{"down.example.invalid", "up.example.invalid"},
		TelemostRooms: []string{"fake-telemost-1", "https://telemost.yandex.ru/j/fake-telemost-2"},
		WBStreamRooms: []string{"https://stream.wb.ru/room/fake-wb-room-1", "fake-wb-room-2"},
		WBStreamToken: "fake-wb-token", RunNumber: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	lt.probe = func(_ context.Context, host string) bool { return host == "up.example.invalid" }
	ctx := context.Background()
	if r, err := lt.room(ctx, "jitsi"); err != nil || !strings.HasPrefix(r, "https://up.example.invalid/gate-") {
		t.Fatalf("jitsi room = %q %v", r, err)
	}
	if r, err := lt.room(ctx, "telemost"); err != nil || r != "https://telemost.yandex.ru/j/fake-telemost-2" {
		t.Fatalf("telemost room = %q %v", r, err)
	}
	if r, err := lt.room(ctx, "wbstream"); err != nil || r != "fake-wb-room-2" {
		t.Fatalf("wbstream room = %q %v", r, err)
	}
	lt.opts.RunNumber = 4
	if r, err := lt.room(ctx, "telemost"); err != nil || r != "https://telemost.yandex.ru/j/fake-telemost-1" {
		t.Fatalf("telemost room = %q %v", r, err)
	}
	if r, err := lt.room(ctx, "wbstream"); err != nil || r != "fake-wb-room-1" {
		t.Fatalf("wbstream room = %q %v", r, err)
	}
	lt.opts.TelemostRooms = nil
	if _, err := lt.room(ctx, "telemost"); !errors.Is(err, ErrPoolRoom) {
		t.Fatalf("an empty pool: err = %v", err)
	}
}

// TestLocalTargetEndpointsAreFreshPerPair asks twice for each pair: every
// endpoint carries its own key and channel (spec section 2, A4), so a server
// left over from an earlier pair or run can never handshake. A Jitsi pair
// gets a fresh room too; a pool room is the run's, the same each time.
func TestLocalTargetEndpointsAreFreshPerPair(t *testing.T) {
	lt, err := NewLocalTarget(LocalOptions{
		WorkDir: t.TempDir(), Providers: []string{"jitsi", "telemost", "wbstream"}, Transports: []string{"vp8channel"},
		JitsiHosts: []string{"up.example.invalid"}, TelemostRooms: []string{"fake-telemost-1"},
		WBStreamRooms: []string{"fake-wb-room-1"}, WBStreamToken: "fake-wb-token",
	})
	if err != nil {
		t.Fatal(err)
	}
	lt.probe = func(context.Context, string) bool { return true }
	for _, p := range []Pair{{"jitsi", "vp8channel"}, {"telemost", "vp8channel"}, {"wbstream", "vp8channel"}} {
		first, err1 := lt.endpoint(context.Background(), p)
		second, err2 := lt.endpoint(context.Background(), p)
		if err1 != nil || err2 != nil {
			t.Fatalf("%s: %v, %v", p, err1, err2)
		}
		if first.Key == second.Key || first.Channel == second.Channel {
			t.Fatalf("%s: same key %t, same channel %t, want both fresh per pair", p,
				first.Key == second.Key, first.Channel == second.Channel)
		}
		if fresh := first.Room != second.Room; fresh != (p.Provider == "jitsi") {
			t.Fatalf("%s: fresh room %t, want a fresh one on jitsi alone", p, fresh)
		}
	}
}

// writeFakeServer stands in for cmd/olcrtc: a script that prints its config
// (room, key, channel, token) into its log and then runs body.
func writeFakeServer(t *testing.T, body string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("the fake server is a shell script")
	}
	p := filepath.Join(t.TempDir(), "olcrtc")
	script := "#!/bin/sh\ncat \"$1\"\n" + body + "\n"
	if err := os.WriteFile(p, []byte(script), 0o700); err != nil { //nolint:gosec // the fake server must run
		t.Fatal(err)
	}
	return p
}

// fakeLocal is a local target with one pair whose server is the fake.
func fakeLocal(t *testing.T, work string, opts LocalOptions, body string) *LocalTarget {
	t.Helper()
	opts.WorkDir = work
	lt, err := NewLocalTarget(opts)
	if err != nil {
		t.Fatal(err)
	}
	lt.binary = writeFakeServer(t, body)
	return lt
}

func readTargetFile(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestLocalTargetRunsTheServerAndLeavesOnlyAScrubbedLog(t *testing.T) {
	work, dir := t.TempDir(), t.TempDir()
	// The fake's loop ends by itself after about 30 s, so a test binary that
	// dies before its cleanups run leaves nothing looping behind.
	lt := fakeLocal(t, work, LocalOptions{
		Providers: []string{"wbstream"}, Transports: []string{"vp8channel"},
		WBStreamRooms: []string{"https://stream.wb.ru/room/fake-wb-room-1"}, WBStreamToken: "fake-wb-token-0001",
	}, `trap 'echo "leaving fake-wb-room-1"; exit 0' TERM
echo "bridge delay $OLCRTC_TEST_BRIDGE_DELAY"; echo "joining fake-wb-room-1"; echo "Link connected"
i=0; while [ "$i" -lt 600 ]; do sleep 0.05; i=$((i+1)); done`)
	ep, stop, err := lt.Open(context.Background(), Pair{"wbstream", "vp8channel"}, dir, OpenOptions{BridgeDelay: 1500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop) // a check that fails before the stop below still stops the server
	if ep.Room != "fake-wb-room-1" || len(ep.Key) != 64 || !strings.HasPrefix(ep.Channel, "gate-") ||
		ep.DNS != "8.8.8.8:53" || ep.VP8FPS != 60 || ep.VP8Batch != 64 {
		t.Fatalf("endpoint = %+v channel %q", ep, ep.Channel)
	}
	configs, _ := filepath.Glob(filepath.Join(work, "*", "srv.yaml"))
	if len(configs) != 1 || !strings.Contains(readTargetFile(t, configs[0]), `token: "fake-wb-token-0001"`) {
		t.Fatalf("private configs = %v", configs)
	}
	if _, err := os.Stat(filepath.Join(dir, "srv.yaml")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the config reached the artifact directory: %v", err)
	}
	stopped := time.Now()
	stop()
	stop()
	if took := time.Since(stopped); took > stopGrace/2 {
		t.Fatalf("stop took %s: the server was killed, not asked to leave", took)
	}
	log := readTargetFile(t, filepath.Join(dir, "srv.log"))
	for _, secret := range []string{"fake-wb-room-1", ep.Key, ep.Channel, "fake-wb-token-0001"} {
		if strings.Contains(log, secret) {
			t.Fatalf("scrubbed log keeps %q:\n%s", secret, log)
		}
	}
	for _, kept := range []string{"Link connected", "bridge delay 1.5s", "joining <room>", "<key>", "leaving <room>"} {
		if !strings.Contains(log, kept) {
			t.Fatalf("scrubbed log lost %q:\n%s", kept, log)
		}
	}
	if left, _ := os.ReadDir(work); len(left) != 0 {
		t.Fatalf("private files left after stop: %v", left)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 1 || entries[0].Name() != "srv.log" {
		t.Fatalf("artifact directory holds %v", entries)
	}
}

func TestLocalTargetReportsAServerThatDiesBeforeTheLink(t *testing.T) {
	dir := t.TempDir()
	lt := fakeLocal(t, t.TempDir(), LocalOptions{
		Providers: []string{"telemost"}, Transports: []string{"vp8channel"}, TelemostRooms: []string{"fake-telemost-1"},
	}, `echo "get connection info: status 404: no conference `+strings.Repeat("x", 340)+
		` fake-telemost-1 and what follows the cut"; exit 1`)
	start := time.Now()
	_, _, err := lt.Open(context.Background(), Pair{"telemost", "vp8channel"}, dir, OpenOptions{})
	if !errors.Is(err, ErrServerExited) || time.Since(start) > 10*time.Second {
		t.Fatalf("err = %v after %s", err, time.Since(start))
	}
	// The room straddles the point the reason is cut at: scrubbed first, it
	// survives as <room>; cut first, its head would leak.
	if !strings.Contains(err.Error(), "status 404: no conference xxx") || !strings.Contains(err.Error(), "<room>") ||
		strings.Contains(err.Error(), "fake-tel") || strings.Contains(err.Error(), "what follows") {
		t.Fatalf("the reason must carry the provider's answer, clipped, and no room: %v", err)
	}
	if log := readTargetFile(t, filepath.Join(dir, "srv.log")); strings.Contains(log, "fake-telemost-1") ||
		!strings.Contains(log, "status 404") {
		t.Fatalf("scrubbed log:\n%s", log)
	}
}

func TestLocalTargetStopsAServerThatNeverLinks(t *testing.T) {
	dir := t.TempDir()
	lt := fakeLocal(t, t.TempDir(), LocalOptions{
		Providers: []string{"jitsi"}, Transports: []string{"datachannel"}, JitsiHosts: []string{"up.example.invalid"},
	}, `echo "jitsi: joining MUC up.example.invalid/x"; exec sleep 30`)
	lt.probe = func(context.Context, string) bool { return true }
	lt.linkWait = 300 * time.Millisecond
	start := time.Now()
	_, stop, err := lt.Open(context.Background(), Pair{"jitsi", "datachannel"}, dir, OpenOptions{})
	if stop != nil {
		t.Cleanup(stop)
	}
	// The host of an override comes from a secret (GATE_JITSI_HOSTS): the
	// line the error quotes and the log keep it only as <room>.
	if !errors.Is(err, ErrLineNotSeen) || !strings.Contains(err.Error(), "jitsi: joining MUC <room>/x") ||
		strings.Contains(err.Error(), "up.example.invalid") {
		t.Fatalf("err = %v", err)
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Fatalf("a server that never links held Open for %s", took)
	}
	if log := readTargetFile(t, filepath.Join(dir, "srv.log")); strings.Contains(log, "gate-") ||
		strings.Contains(log, "up.example.invalid") {
		t.Fatalf("scrubbed log keeps the room, the channel or the host:\n%s", log)
	}
}

func TestLocalTargetNamesACancelledRunNotADeadServer(t *testing.T) {
	lt := fakeLocal(t, t.TempDir(), LocalOptions{
		Providers: []string{"telemost"}, Transports: []string{"vp8channel"}, TelemostRooms: []string{"fake-telemost-1"},
	}, `echo "telemost: joining"; exec sleep 30`)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	time.AfterFunc(300*time.Millisecond, cancel)
	start := time.Now()
	_, stop, err := lt.Open(ctx, Pair{"telemost", "vp8channel"}, t.TempDir(), OpenOptions{})
	if stop != nil {
		t.Cleanup(stop)
	}
	if !errors.Is(err, context.Canceled) || time.Since(start) > 10*time.Second {
		t.Fatalf("err = %v after %s, want the cancellation", err, time.Since(start))
	}
}

func TestLocalTargetFailsWBStreamWithoutAToken(t *testing.T) {
	lt, err := NewLocalTarget(LocalOptions{WorkDir: t.TempDir(), Providers: []string{"wbstream"},
		Transports: []string{"vp8channel"}, WBStreamRooms: []string{"fake-wb-room-1"}})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = lt.Open(context.Background(), Pair{"wbstream", "vp8channel"}, t.TempDir(), OpenOptions{})
	want := "wbstream: OLCRTC_GATE_WBSTREAM_TOKEN is not set (WB refuses a guest as the first participant of an idle room)"
	if !errors.Is(err, ErrNoWBStreamToken) || err.Error() != want {
		t.Fatalf("err = %v", err)
	}
}

func TestLocalTargetRefusesAPairItDoesNotCarry(t *testing.T) {
	lt, err := NewLocalTarget(LocalOptions{WorkDir: t.TempDir(), Providers: []string{"telemost"},
		Transports: []string{"vp8channel"}, TelemostRooms: []string{"fake-telemost-1"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := lt.Open(context.Background(), Pair{"telemost", "datachannel"}, t.TempDir(), OpenOptions{}); !errors.Is(err, ErrPairNotCarried) {
		t.Fatalf("err = %v", err)
	}
}
