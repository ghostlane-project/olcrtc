package main

import (
	"bytes"
	"log"
	"strings"
	"testing"

	"github.com/openlibrecommunity/olcrtc/internal/app/session"
)

// One profile with a bad net.ice_local_ip is skipped on reload, with a log
// line, and the other profiles still load. ai-generated (net.ice_local_ip).
func TestLoadProfilesSkipsAProfileWithABadICELocalIP(t *testing.T) {
	session.RegisterDefaults()
	var out bytes.Buffer
	oldWriter, oldFlags := log.Writer(), log.Flags()
	t.Cleanup(func() {
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
	})
	log.SetOutput(&out)
	log.SetFlags(0)

	path := writeYAML(t, `
mode: srv
crypto:
  key: "0000000000000000000000000000000000000000000000000000000000000001"
net:
  dns: 8.8.8.8:53
profiles:
  - name: good
    auth:
      provider: jitsi
    room:
      id: https://meet.example/room
    net:
      transport: datachannel
      ice_local_ip: "192.0.2.10"
  - name: bad
    auth:
      provider: jitsi
    room:
      id: https://meet.example/room
    net:
      transport: datachannel
      ice_local_ip: "224.0.0.1"
`)
	profiles, err := loadProfiles(path)
	if err != nil {
		t.Fatalf("loadProfiles: %v, want the good profile kept and the bad one skipped", err)
	}
	if len(profiles) != 1 || profiles[0].Name != "good" {
		t.Fatalf("profiles = %+v, want only \"good\"", profiles)
	}
	if got := profiles[0].Config.ICELocalIP; got != "192.0.2.10" {
		t.Fatalf("good profile ICELocalIP = %q", got)
	}
	logged := out.String()
	if !strings.Contains(logged, `skipping invalid failover profile "bad"`) || !strings.Contains(logged, "net.ice_local_ip") {
		t.Fatalf("log = %q, want the skip of \"bad\" naming net.ice_local_ip", logged)
	}
}
