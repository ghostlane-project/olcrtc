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

func TestCheckLocalIP(t *testing.T) {
	if err := CheckLocalIP(netip.Addr{}); err != nil {
		t.Fatalf("no key: %v", err)
	}
	if err := CheckLocalIP(netip.MustParseAddr("127.0.0.1")); err != nil {
		t.Fatalf("loopback is an address of this host: %v", err)
	}
	err := CheckLocalIP(netip.MustParseAddr("192.0.2.254"))
	if !errors.Is(err, ErrICELocalIPNotLocal) || !strings.Contains(err.Error(), "192.0.2.254") {
		t.Fatalf("an address the host does not have: err = %v", err)
	}
}

// A peer connection built with the key gathers host candidates on that one
// address and on no other interface of the host.
func TestLocalIPGathersOnlyThatAddress(t *testing.T) {
	local, others := hostIPv4s(t)
	if len(others) == 0 {
		t.Skip("the host has one non-loopback IPv4 address: nothing to leave out")
	}
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

func hostIPv4s(t *testing.T) (netip.Addr, []netip.Addr) {
	t.Helper()
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatalf("InterfaceAddrs: %v", err)
	}
	var found []netip.Addr
	for _, a := range addrs {
		n, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		ip, ok := netip.AddrFromSlice(n.IP)
		if ok && ip.Unmap().Is4() && !ip.IsLoopback() {
			found = append(found, ip.Unmap())
		}
	}
	if len(found) == 0 {
		t.Skip("the host has no non-loopback IPv4 address")
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
