package engine

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
)

// ai-generated: the whole file (net.ice_local_ip).

// ErrICELocalIPNotLocal is returned at start when net.ice_local_ip names an
// address no interface of the host has: ICE would gather nothing, and a server
// that cannot join must say so instead of failing every join in silence.
var ErrICELocalIPNotLocal = errors.New("net.ice_local_ip is not an address of this host")

// PionSession is the session's part of every pion API a carrier builds: the
// fields that come from the config rather than from the carrier.
type PionSession struct {
	DTLSProfile DTLSProfile
	LocalIP     netip.Addr
}

// Pion returns the session's part of the pion settings.
func (c Config) Pion() PionSession {
	return PionSession{DTLSProfile: c.DTLSProfile, LocalIP: c.ICELocalIP}
}

// Options is a carrier's own options with the session's fields filled in. It
// is the one way a carrier builds its PionSettingsOptions, so a field added to
// the session reaches every carrier.
func (p PionSession) Options(o PionSettingsOptions) PionSettingsOptions {
	o.DTLSProfile = p.DTLSProfile
	o.LocalIP = p.LocalIP
	return o
}

// CheckLocalIP returns ErrICELocalIPNotLocal when addr is set and no
// interface of the host has it; an unset addr passes.
func CheckLocalIP(addr netip.Addr) error {
	if !addr.IsValid() {
		return nil
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return fmt.Errorf("net.ice_local_ip: list the host's addresses: %w", err)
	}
	for _, a := range addrs {
		n, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		if ip, ok := netip.AddrFromSlice(n.IP); ok && ip.Unmap() == addr.Unmap() {
			return nil
		}
	}
	return fmt.Errorf("%w: %s", ErrICELocalIPNotLocal, addr)
}

// iceIPFilter is the address filter ICE gathers through: IPv4 only when the
// carrier asks for it, and only LocalIP when the session names one. Nil when
// neither applies, which leaves pion's own choice.
func iceIPFilter(opts PionSettingsOptions) func(net.IP) bool {
	if !opts.IPv4Only && !opts.LocalIP.IsValid() {
		return nil
	}
	local := opts.LocalIP.Unmap()
	return func(ip net.IP) bool {
		if opts.IPv4Only && ip.To4() == nil {
			return false
		}
		if !local.IsValid() {
			return true
		}
		addr, ok := netip.AddrFromSlice(ip)
		return ok && addr.Unmap() == local
	}
}
