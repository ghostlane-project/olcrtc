package engine

import (
	"errors"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

// ai-generated: the tests below (net.ice_local_ip).

func TestICEIPFilterAdmitsOnlyTheLocalIP(t *testing.T) {
	filter := iceIPFilter(PionSettingsOptions{IPv4Only: true, LocalIP: netip.MustParseAddr("192.0.2.10")})
	if filter == nil {
		t.Fatal("a local IP set no filter")
	}
	for ip, want := range map[string]bool{"192.0.2.10": true, "192.0.2.11": false, "10.0.0.1": false, "2001:db8::1": false} {
		if got := filter(net.ParseIP(ip)); got != want {
			t.Fatalf("filter(%s) = %v, want %v", ip, got, want)
		}
	}
}

// Without the key the filter is IPv4Only's, as before: every IPv4 address.
func TestICEIPFilterWithoutLocalIPIsIPv4Only(t *testing.T) {
	filter := iceIPFilter(PionSettingsOptions{IPv4Only: true})
	if filter == nil || !filter(net.ParseIP("10.0.0.1")) || !filter(net.ParseIP("192.0.2.10")) || filter(net.ParseIP("2001:db8::1")) {
		t.Fatal("the IPv4-only filter changed")
	}
	if iceIPFilter(PionSettingsOptions{}) != nil {
		t.Fatal("no options built a filter")
	}
}

func TestNewPionSettingsLocalIPAloneBuildsAHook(t *testing.T) {
	apply, err := NewPionSettings(PionSettingsOptions{LocalIP: netip.MustParseAddr("192.0.2.10")})
	if err != nil {
		t.Fatalf("NewPionSettings: %v", err)
	}
	if apply == nil {
		t.Fatal("a local IP alone left the SDK's settings untouched")
	}
}

func TestPionSessionOptionsFillsTheSessionFields(t *testing.T) {
	local := netip.MustParseAddr("192.0.2.10")
	cfg := Config{DTLSProfile: DTLSProfileOff, ICELocalIP: local}
	opts := cfg.Pion().Options(PionSettingsOptions{IPv4Only: true, DisableMulticast: true})
	if opts.LocalIP != local || opts.DTLSProfile != DTLSProfileOff || !opts.IPv4Only || !opts.DisableMulticast {
		t.Fatalf("options = %+v", opts)
	}
	if (Config{}).Pion().Options(PionSettingsOptions{}).LocalIP.IsValid() {
		t.Fatal("a session without the key passed a local IP")
	}
}

func TestCheckLocalIPUnsetPasses(t *testing.T) {
	if err := CheckLocalIP(netip.Addr{}, true); err != nil {
		t.Fatalf("no key: %v", err)
	}
}

func fakeInterfaces(ifcs ...localInterface) func() ([]localInterface, error) {
	return func() ([]localInterface, error) { return ifcs, nil }
}

// CheckLocalIP judges an address the way pion gathers: on an interface that
// is up, is not loopback and, behind the protected net, is not a tunnel.
func TestCheckLocalIPJudgesTheInterfaceICEGathersFrom(t *testing.T) {
	addr := netip.MustParseAddr("192.0.2.10")
	other := netip.MustParseAddr("192.0.2.11")
	on := []netip.Addr{addr}
	cases := []struct {
		name      string
		ifcs      []localInterface
		protected bool
		want      error
	}{
		{"up", []localInterface{{name: "eth0", flags: net.FlagUp, addrs: on}}, true, nil},
		{"absent", []localInterface{{name: "eth0", flags: net.FlagUp, addrs: []netip.Addr{other}}}, true, ErrICELocalIPNotLocal},
		{"down", []localInterface{{name: "eth1", addrs: on}}, true, ErrICELocalIPDown},
		{"loopback", []localInterface{{name: "lo", flags: net.FlagUp | net.FlagLoopback, addrs: on}}, true, ErrICELocalIPLoopback},
		{"hidden", []localInterface{{name: "tun0", flags: net.FlagUp, addrs: on}}, true, ErrICELocalIPHidden},
		{"tunnel without the protected net", []localInterface{{name: "tun0", flags: net.FlagUp, addrs: on}}, false, nil},
		{"down and up", []localInterface{
			{name: "eth1", addrs: on}, {name: "eth0", flags: net.FlagUp, addrs: on},
		}, true, nil},
	}
	for _, c := range cases {
		err := checkLocalIP(addr, c.protected, fakeInterfaces(c.ifcs...))
		if c.want == nil && err != nil {
			t.Fatalf("%s: err = %v, want none", c.name, err)
		}
		if c.want != nil && (!errors.Is(err, c.want) || !strings.Contains(err.Error(), addr.String())) {
			t.Fatalf("%s: err = %v, want %v naming the address", c.name, err, c.want)
		}
	}
}

// A peer connection built with the key gathers host candidates on that one
// address and on no other interface of the host.
func TestLocalIPGathersOnlyThatAddress(t *testing.T) {
	local, _ := hostIPv4s(t)
	got := gatherHostAddrs(t, PionSettingsOptions{IPv4Only: true, DisableMulticast: true, LocalIP: local})
	if len(got) == 0 {
		t.Fatal("no host candidate gathered on the local IP")
	}
	for _, addr := range got {
		if addr != local.String() {
			t.Fatalf("host candidate on %s, want only %s", addr, local)
		}
	}
	all := gatherHostAddrs(t, PionSettingsOptions{IPv4Only: true, DisableMulticast: true})
	if len(all) <= len(got) {
		t.Fatalf("without the key %d host candidates, with it %d: the key left nothing out", len(all), len(got))
	}
}

// hostIPv4s picks the IPv4 addresses ICE gathers on here, judged the way
// CheckLocalIP judges them (up, not loopback, not hidden), so the test does
// not depend on the order or state of the host's interfaces.
func hostIPv4s(t *testing.T) (netip.Addr, []netip.Addr) {
	t.Helper()
	ifcs, err := hostInterfaces()
	if err != nil {
		t.Fatalf("hostInterfaces: %v", err)
	}
	protected := UsesProtectedNet(nil)
	var found []netip.Addr
	for _, ifc := range ifcs {
		if gatherRefusal(ifc, protected) != nil {
			continue
		}
		for _, ip := range ifc.addrs {
			if ip.Is4() && !ip.IsLinkLocalUnicast() {
				found = append(found, ip)
			}
		}
	}
	if len(found) < 2 {
		t.Skipf("the host has %d IPv4 address(es) ICE gathers on (up, not loopback, not hidden); two are needed, one to keep and one to leave out", len(found))
	}
	return found[0], found[1:]
}

func gatherHostAddrs(t *testing.T, opts PionSettingsOptions) []string {
	t.Helper()
	apply, err := NewPionSettings(opts)
	if err != nil {
		t.Fatalf("NewPionSettings: %v", err)
	}
	settings := webrtc.SettingEngine{}
	apply(&settings)
	pc, err := webrtc.NewAPI(webrtc.WithSettingEngine(settings)).NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("NewPeerConnection: %v", err)
	}
	defer func() { _ = pc.Close() }()
	if _, err = pc.CreateDataChannel("probe", nil); err != nil {
		t.Fatalf("CreateDataChannel: %v", err)
	}
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatalf("CreateOffer: %v", err)
	}
	done := webrtc.GatheringCompletePromise(pc)
	if err = pc.SetLocalDescription(offer); err != nil {
		t.Fatalf("SetLocalDescription: %v", err)
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("gathering did not complete")
	}
	var addrs []string
	for _, line := range strings.Split(pc.LocalDescription().SDP, "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) >= 8 && strings.HasPrefix(fields[0], "a=candidate:") && fields[7] == "host" {
			addrs = append(addrs, fields[4])
		}
	}
	return addrs
}

func TestParseICELocalIPRefusesWhatNoInterfaceGathersOn(t *testing.T) {
	for _, bad := range []string{
		"not-an-ip", "192.0.2.300", "192.0.2.10/24", " 192.0.2.10",
		"2001:db8::1", "fe80::1",
		"0.0.0.0", "::ffff:0.0.0.0", // unspecified, plain and mapped
		"127.0.0.1", "::ffff:127.0.0.1", // loopback
		"224.0.0.1", "239.1.2.3", "::ffff:224.0.0.1", // multicast
		"255.255.255.255", "::ffff:255.255.255.255", // limited broadcast
		"169.254.1.1", "::ffff:169.254.1.1", // link-local
	} {
		if _, err := ParseICELocalIP(bad); !errors.Is(err, ErrICELocalIP) {
			t.Errorf("ParseICELocalIP(%q) = %v, want ErrICELocalIP", bad, err)
		}
	}
}

func TestParseICELocalIPAcceptsAHostAddress(t *testing.T) {
	for in, want := range map[string]netip.Addr{
		"":                  {},
		"192.0.2.10":        netip.MustParseAddr("192.0.2.10"),
		"::ffff:192.0.2.10": netip.MustParseAddr("192.0.2.10"),
		"10.0.0.1":          netip.MustParseAddr("10.0.0.1"),
	} {
		got, err := ParseICELocalIP(in)
		if err != nil || got != want {
			t.Errorf("ParseICELocalIP(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
}
