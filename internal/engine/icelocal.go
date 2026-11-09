package engine

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"

	"github.com/openlibrecommunity/olcrtc/internal/protect"
)

// ai-generated: the whole file (net.ice_local_ip).

// The refusals at start of a net.ice_local_ip ICE would gather nothing on: a
// server that cannot join must say so instead of failing every join in silence.
var (
	// ErrICELocalIPNotLocal: no interface of the host has the address.
	ErrICELocalIPNotLocal = errors.New("net.ice_local_ip is not an address of this host")
	// ErrICELocalIPDown: the interface that has it is down.
	ErrICELocalIPDown = errors.New("net.ice_local_ip is on an interface that is down")
	// ErrICELocalIPLoopback: the interface that has it is a loopback one, which
	// ICE does not gather on.
	ErrICELocalIPLoopback = errors.New("net.ice_local_ip is on a loopback interface, which ICE does not gather on")
	// ErrICELocalIPHidden: the interface that has it is a tunnel the protected
	// net hides from ICE.
	ErrICELocalIPHidden = errors.New("net.ice_local_ip is on a tunnel interface the protected net hides from ICE")
)

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

// CheckLocalIP returns nil when addr is unset or ICE can gather on it: an
// interface that is up, is not loopback and, when the protected net is in use
// (UsesProtectedNet), is not a tunnel it hides. Otherwise the error says which.
func CheckLocalIP(addr netip.Addr, protected bool) error {
	return checkLocalIP(addr, protected, hostInterfaces)
}

// localInterface is what CheckLocalIP reads of one interface.
type localInterface struct {
	name  string
	flags net.Flags
	addrs []netip.Addr
}

func checkLocalIP(addr netip.Addr, protected bool, list func() ([]localInterface, error)) error {
	if !addr.IsValid() {
		return nil
	}
	ifcs, err := list()
	if err != nil {
		return fmt.Errorf("net.ice_local_ip: list the host's interfaces: %w", err)
	}
	refusal, where := ErrICELocalIPNotLocal, ""
	for _, ifc := range ifcs {
		if !slices.Contains(ifc.addrs, addr.Unmap()) {
			continue
		}
		why := gatherRefusal(ifc, protected)
		if why == nil {
			return nil
		}
		refusal, where = why, " on "+ifc.name
	}
	return fmt.Errorf("%w: %s%s", refusal, addr, where)
}

// gatherRefusal is why ICE would not gather on ifc, nil when it would: the
// checks pion's own interface walk and the protected net make.
func gatherRefusal(ifc localInterface, protected bool) error {
	switch {
	case ifc.flags&net.FlagUp == 0:
		return ErrICELocalIPDown
	case ifc.flags&net.FlagLoopback != 0:
		return ErrICELocalIPLoopback
	case protected && protect.IsTunnelInterface(ifc.name):
		return ErrICELocalIPHidden
	default:
		return nil
	}
}

// hostInterfaces lists the host's interfaces with their addresses, unmapped.
func hostInterfaces() ([]localInterface, error) {
	ifcs, err := net.Interfaces()
	if err != nil {
		return nil, fmt.Errorf("interfaces: %w", err)
	}
	out := make([]localInterface, 0, len(ifcs))
	for _, ifc := range ifcs {
		addrs, err := ifc.Addrs()
		if err != nil {
			return nil, fmt.Errorf("addresses of %s: %w", ifc.Name, err)
		}
		li := localInterface{name: ifc.Name, flags: ifc.Flags}
		for _, a := range addrs {
			if n, ok := a.(*net.IPNet); ok {
				if ip, ok := netip.AddrFromSlice(n.IP); ok {
					li.addrs = append(li.addrs, ip.Unmap())
				}
			}
		}
		out = append(out, li)
	}
	return out, nil
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
