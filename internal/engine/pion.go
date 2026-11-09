package engine

import (
	"fmt"
	"net/netip"
	"runtime"

	"github.com/pion/ice/v4"
	"github.com/pion/logging"
	"github.com/pion/webrtc/v4"

	"github.com/openlibrecommunity/olcrtc/internal/protect"
)

// PionSettingsOptions describes per-engine SettingEngine differences.
type PionSettingsOptions struct {
	Resolver         protect.Lookup
	LoggerFactory    logging.LoggerFactory
	IPv4Only         bool
	ProxyDialer      bool
	DisableMulticast bool
	// DTLSProfile selects a fixed ClientHello profile for this session; empty or
	// "off" keeps the stock Pion handshake. Validated before any dialing.
	DTLSProfile DTLSProfile
	// LocalIP, when valid, is net.ice_local_ip, the address ICE gathers on: host
	// candidates and the bases of the UDP reflexive and relay ones; TCP and TLS
	// TURN are dialed by the routing table. What it bounds is the number of TURN
	// allocations.
	// The zero value gathers on every address, as before.
	//
	// ai-generated: the field (net.ice_local_ip).
	LocalIP netip.Addr
}

// PionSettings applies shared network settings to a pion SettingEngine.
type PionSettings func(*webrtc.SettingEngine)

// UsesProtectedNet reports whether pion settings built with resolver gather
// through the protected net, which hides tunnel interfaces from ICE.
//
// ai-generated: split out of NewPionSettings so CheckLocalIP judges with the
// same view (net.ice_local_ip).
func UsesProtectedNet(resolver protect.Lookup) bool {
	return protect.HasProtector() || resolver != nil || runtime.GOOS == "android"
}

// NewPionSettings prepares protected networking and per-engine pion settings.
func NewPionSettings(opts PionSettingsOptions) (PionSettings, error) {
	if err := ValidateDTLSProfile(opts.DTLSProfile); err != nil {
		return nil, err
	}
	var protectedNet *protect.ProtectedNet
	if UsesProtectedNet(opts.Resolver) { // ai-generated: the condition moved into UsesProtectedNet
		var err error
		protectedNet, err = protect.NewProtectedNet(opts.Resolver)
		if err != nil {
			return nil, fmt.Errorf("protected net: %w", err)
		}
	}
	// ai-generated (net.ice_local_ip): the LocalIP term; a local IP alone
	// needs the hook that sets the ICE filter.
	if opts.LoggerFactory == nil && !opts.IPv4Only && !opts.LocalIP.IsValid() && protectedNet == nil {
		// A chosen DTLS profile still has to reach SDK-owned settings, so only
		// the stock profile keeps the nil hook.
		if opts.DTLSProfile == "" || opts.DTLSProfile == DTLSProfileOff {
			return nil, nil //nolint:nilnil // nil hook preserves SDK-owned pion settings
		}
		return dtlsOnlySettings(opts.DTLSProfile), nil
	}

	return networkSettings(opts, protectedNet), nil
}

// networkSettings is the settings hook for a request that reached the
// partial path: base options, the DTLS profile, and the protected net when
// one was built.
func networkSettings(opts PionSettingsOptions, protectedNet *protect.ProtectedNet) func(*webrtc.SettingEngine) {
	return func(settings *webrtc.SettingEngine) {
		if opts.LoggerFactory != nil {
			settings.LoggerFactory = opts.LoggerFactory
		}
		if opts.IPv4Only {
			settings.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
		}
		// ai-generated (net.ice_local_ip): the filter, IPv4-only and/or the
		// one local IP; nil keeps pion gathering on every address.
		if filter := iceIPFilter(opts); filter != nil {
			settings.SetIPFilter(filter)
		}
		applyDTLS(settings, opts.DTLSProfile)
		if protectedNet == nil {
			return
		}
		settings.SetNet(protectedNet)
		if opts.ProxyDialer {
			settings.SetICEProxyDialer(protect.NewProxyDialer(opts.Resolver))
		}
		if opts.DisableMulticast {
			settings.SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)
		}
	}
}

// dtlsOnlySettings is the settings hook for a profile that must reach
// SDK-owned pion settings with nothing else requested.
func dtlsOnlySettings(profile DTLSProfile) func(*webrtc.SettingEngine) {
	return func(settings *webrtc.SettingEngine) { applyDTLS(settings, profile) }
}

// applyDTLS installs a profile whose validation already succeeded; a failure
// here is a programmer error, not a dial.
func applyDTLS(settings *webrtc.SettingEngine, profile DTLSProfile) {
	if err := ApplyDTLSProfile(settings, profile); err != nil {
		panic(err)
	}
}
