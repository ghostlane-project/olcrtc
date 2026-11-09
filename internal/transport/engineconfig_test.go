package transport

import (
	"net/netip"
	"testing"
)

// ai-generated: the ICE mode a side asks for reaches the engine builder with the
// rest of the provider-facing fields.
func TestEngineConfigCarriesTheDefaultICE(t *testing.T) {
	if got := (Config{RoomURL: "room", DefaultICE: "tcp"}).EngineConfig().DefaultICE; got != "tcp" {
		t.Fatalf("EngineConfig().DefaultICE = %q, want tcp", got)
	}
	if got := (Config{RoomURL: "room"}).EngineConfig().DefaultICE; got != "" {
		t.Fatalf("a side that asks for nothing: DefaultICE = %q, want empty", got)
	}
}

// ai-generated: net.ice_local_ip reaches the engine builder.
func TestEngineConfigCarriesTheICELocalIP(t *testing.T) {
	local := netip.MustParseAddr("192.0.2.10")
	if got := (Config{RoomURL: "room", ICELocalIP: local}).EngineConfig().ICELocalIP; got != local {
		t.Fatalf("EngineConfig().ICELocalIP = %v, want %v", got, local)
	}
}
