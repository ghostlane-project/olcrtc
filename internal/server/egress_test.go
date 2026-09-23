package server

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"syscall"
	"testing"
	"time"
)

// ai-generated: the whole file (egress hardening: the private-target block on
// TCP CONNECT).

const (
	testPublicIP     = "93.184.216.34"
	testPublicIPAlt  = "1.1.1.1"
	testRebindHost   = "rebind.test"
	testPublicHost   = "public.test"
	testTwoAddrsHost = "two-addrs.test"
)

// stubLookup answers LookupIP from answers, from later instead for every
// call after a name's first (a resolver that changes its answer), and a
// not-found DNS error for a name it does not know. It counts the calls.
type stubLookup struct {
	answers map[string][]net.IP
	later   map[string][]net.IP

	mu    sync.Mutex
	asked map[string]int
	total int
}

func (l *stubLookup) LookupIP(_ context.Context, network, host string) ([]net.IP, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.total++
	if l.asked == nil {
		l.asked = make(map[string]int)
	}
	l.asked[host]++
	answer, ok := l.answers[host]
	if next, changed := l.later[host]; changed && l.asked[host] > 1 {
		answer, ok = next, true
	}
	if !ok {
		return nil, &net.DNSError{Err: "nxdomain", Name: host, Server: "192.0.2.53:53", IsNotFound: true}
	}
	out := make([]net.IP, 0, len(answer))
	for _, ip := range answer {
		if network == "ip4" && ip.To4() == nil {
			continue
		}
		out = append(out, ip)
	}
	return out, nil
}

func (l *stubLookup) calls() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.total
}

// dialRecorder stands in for the protected dialer: it records every address
// a CONNECT dials and answers with one end of a pipe, or with the error set
// for that address.
type dialRecorder struct {
	mu     sync.Mutex
	dialed []string
	fail   map[string]error
}

func (d *dialRecorder) dial(_ context.Context, network, address string) (net.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dialed = append(d.dialed, network+" "+address)
	if err := d.fail[address]; err != nil {
		return nil, err
	}
	local, remote := net.Pipe()
	go func() {
		_, _ = remote.Write([]byte("hi\n"))
		_ = remote.Close()
	}()
	return local, nil
}

func (d *dialRecorder) addresses() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.dialed...)
}

func TestBlockedAddr(t *testing.T) {
	blocked := []string{
		"127.0.0.1", "127.8.9.10", "0.0.0.0", "0.1.2.3",
		"10.0.0.1", "172.16.0.1", "172.31.255.255", "192.168.1.1",
		"169.254.169.254", "100.64.0.1", "100.100.100.200", "100.127.255.255",
		"192.0.0.8", "192.0.2.1", "198.18.0.1", "198.51.100.1", "203.0.113.1",
		"224.0.0.1", "239.255.255.250", "240.0.0.1", "255.255.255.255",
		"::", "::1", "fe80::1", "fe80::1%eth0", "fc00::1", "fd12:3456::1", "ff02::1",
		"::ffff:127.0.0.1", "::ffff:10.0.0.1", "::ffff:169.254.169.254", "::ffff:100.64.0.1", "::ffff:0.0.0.0",
	}
	for _, text := range blocked {
		if !blockedAddr(netip.MustParseAddr(text)) {
			t.Errorf("blockedAddr(%s) = false, want true", text)
		}
	}
	if !blockedAddr(netip.Addr{}) {
		t.Error("blockedAddr(zero Addr) = false, want true")
	}
	allowed := []string{
		testPublicIP, testPublicIPAlt, "8.8.8.8", "100.63.255.255", "100.128.0.0",
		"172.15.255.255", "172.32.0.0", "192.169.0.1", "2606:4700:4700::1111", "2a00:1450:4001::1",
		"::ffff:1.1.1.1",
	}
	for _, text := range allowed {
		if blockedAddr(netip.MustParseAddr(text)) {
			t.Errorf("blockedAddr(%s) = true, want false", text)
		}
	}
}

// Every literal the policy refuses is refused before anything is dialed,
// whatever form it takes: IPv4, IPv6, IPv4-mapped IPv6, zoned.
func TestDialRefusesBlockedLiterals(t *testing.T) {
	for _, host := range []string{
		"127.0.0.1", "::1", "10.1.2.3", "169.254.169.254", "100.64.0.1",
		"0.0.0.0", "::ffff:127.0.0.1", "fe80::1%lo", "224.0.0.251",
	} {
		t.Run(host, func(t *testing.T) {
			rec := &dialRecorder{}
			lookup := &stubLookup{}
			s := &Server{resolver: lookup, dialTarget: rec.dial}
			_, err := s.dial(context.Background(), ConnectRequest{Addr: host, Port: 10085})
			if !errors.Is(err, errBlockedTarget) {
				t.Fatalf("dial(%s) error = %v, want %v", host, err, errBlockedTarget)
			}
			if got := rec.addresses(); len(got) != 0 || lookup.calls() != 0 {
				t.Fatalf("dial(%s) reached %v after %d lookups, want nothing", host, got, lookup.calls())
			}
		})
	}
}

// A name is judged by what it resolves to: one refused address among the
// answers refuses the target, and nothing is dialed.
func TestDialRefusesNamesResolvingInside(t *testing.T) {
	lookup := &stubLookup{answers: map[string][]net.IP{
		"loopback.test": {net.IPv4(127, 0, 0, 1)},
		"metadata.test": {net.IPv4(169, 254, 169, 254)},
		"cgnat.test":    {net.IPv4(100, 64, 1, 1)},
		"private.test":  {net.IPv4(10, 0, 0, 5)},
		"mixed.test":    {net.ParseIP(testPublicIP), net.IPv4(192, 168, 0, 1)},
		"mapped.test":   {net.ParseIP("::ffff:127.0.0.1")},
	}}
	for host := range lookup.answers {
		t.Run(host, func(t *testing.T) {
			rec := &dialRecorder{}
			s := &Server{resolver: lookup, dialTarget: rec.dial}
			_, err := s.dial(context.Background(), ConnectRequest{Addr: host, Port: 443})
			if !errors.Is(err, errBlockedTarget) {
				t.Fatalf("dial(%s) error = %v, want %v", host, err, errBlockedTarget)
			}
			if got := rec.addresses(); len(got) != 0 {
				t.Fatalf("dial(%s) reached %v, want nothing", host, got)
			}
		})
	}
}

// A public target still dials, and what is dialed is the checked literal:
// the name is resolved once, by the check, never again by the dialer.
func TestDialPublicTargetDialsTheCheckedAddress(t *testing.T) {
	lookup := &stubLookup{answers: map[string][]net.IP{
		testPublicHost: {net.ParseIP(testPublicIP), net.ParseIP("2606:4700:4700::1111")},
	}}
	tests := []struct {
		host string
		want string
	}{
		{host: testPublicHost, want: "tcp4 " + testPublicIP + ":443"},
		{host: testPublicIPAlt, want: "tcp4 " + testPublicIPAlt + ":443"},
		{host: "::ffff:" + testPublicIPAlt, want: "tcp4 " + testPublicIPAlt + ":443"},
	}
	for _, tt := range tests {
		t.Run(tt.host, func(t *testing.T) {
			rec := &dialRecorder{}
			s := &Server{resolver: lookup, dialTarget: rec.dial}
			conn, err := s.dial(context.Background(), ConnectRequest{Addr: tt.host, Port: 443})
			if err != nil {
				t.Fatalf("dial(%s) error = %v", tt.host, err)
			}
			_ = conn.Close()
			if got := rec.addresses(); len(got) != 1 || got[0] != tt.want {
				t.Fatalf("dial(%s) dialed %v, want [%s]", tt.host, got, tt.want)
			}
		})
	}
	if lookup.calls() != 1 {
		t.Fatalf("lookups = %d, want 1: only the name is resolved, and once", lookup.calls())
	}
}

// A resolver that answers a public address to the check and a private one
// after it (DNS rebinding) cannot move the dial: there is no second lookup
// for one CONNECT, and the next CONNECT is judged on its own answer.
func TestDialResolvesTheNameOnce(t *testing.T) {
	lookup := &stubLookup{
		answers: map[string][]net.IP{testRebindHost: {net.ParseIP(testPublicIP)}},
		later:   map[string][]net.IP{testRebindHost: {net.IPv4(127, 0, 0, 1)}},
	}
	rec := &dialRecorder{}
	s := &Server{resolver: lookup, dialTarget: rec.dial}
	conn, err := s.dial(context.Background(), ConnectRequest{Addr: testRebindHost, Port: 80})
	if err != nil {
		t.Fatalf("first dial error = %v", err)
	}
	_ = conn.Close()
	if got := rec.addresses(); len(got) != 1 || got[0] != "tcp4 "+testPublicIP+":80" || lookup.calls() != 1 {
		t.Fatalf("first dial reached %v after %d lookups, want the checked address after one", got, lookup.calls())
	}
	if _, err := s.dial(context.Background(), ConnectRequest{Addr: testRebindHost, Port: 80}); !errors.Is(err, errBlockedTarget) {
		t.Fatalf("second dial error = %v, want %v", err, errBlockedTarget)
	}
	if got := rec.addresses(); len(got) != 1 {
		t.Fatalf("the rebound answer was dialed: %v", got)
	}
}

// A name with several addresses is dialed as before: each checked address in
// order until one answers.
func TestDialTriesTheNextCheckedAddress(t *testing.T) {
	lookup := &stubLookup{answers: map[string][]net.IP{
		testTwoAddrsHost: {net.ParseIP(testPublicIP), net.ParseIP(testPublicIPAlt)},
	}}
	rec := &dialRecorder{fail: map[string]error{testPublicIP + ":443": syscall.ECONNREFUSED}}
	s := &Server{resolver: lookup, dialTarget: rec.dial}
	conn, err := s.dial(context.Background(), ConnectRequest{Addr: testTwoAddrsHost, Port: 443})
	if err != nil {
		t.Fatalf("dial error = %v", err)
	}
	_ = conn.Close()
	want := []string{"tcp4 " + testPublicIP + ":443", "tcp4 " + testPublicIPAlt + ":443"}
	if got := rec.addresses(); len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("dialed %v, want %v", got, want)
	}
}

// Behind an upstream proxy a blocked literal is refused before the proxy is
// dialed; a name goes to the proxy unresolved, as on the UDP path.
func TestDialViaProxyRefusesBlockedLiteral(t *testing.T) {
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer func() { _ = ln.Close() }()
	accepted := make(chan struct{}, 4)
	go func() {
		for {
			conn, acceptErr := ln.Accept()
			if acceptErr != nil {
				return
			}
			accepted <- struct{}{}
			_ = conn.Close()
		}
	}()
	proxy, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener addr type = %T", ln.Addr())
	}
	lookup := &stubLookup{answers: map[string][]net.IP{"inside.test": {net.IPv4(127, 0, 0, 1)}}}
	s := &Server{resolver: lookup, socksProxyAddr: "127.0.0.1", socksProxyPort: proxy.Port}

	_, err = s.dial(context.Background(), ConnectRequest{Addr: "127.0.0.1", Port: 10085})
	if !errors.Is(err, errBlockedTarget) {
		t.Fatalf("dial(127.0.0.1) via proxy error = %v, want %v", err, errBlockedTarget)
	}
	select {
	case <-accepted:
		t.Fatal("a blocked literal reached the proxy")
	case <-time.After(100 * time.Millisecond):
	}

	_, err = s.dial(context.Background(), ConnectRequest{Addr: "inside.test", Port: 443})
	if errors.Is(err, errBlockedTarget) || lookup.calls() != 0 {
		t.Fatalf("a name via proxy: error = %v after %d lookups, want it handed to the proxy", err, lookup.calls())
	}
	select {
	case <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("the name never reached the proxy")
	}
}
