package session

import (
	"bytes"
	"context"
	"errors"
	"log"
	"reflect"
	"strings"
	"testing"

	"github.com/openlibrecommunity/olcrtc/internal/engine"
	"github.com/openlibrecommunity/olcrtc/internal/server"
)

// The server's per-stream traffic line keeps the session and the byte counts
// and drops the destination: an exit's log is no record of where its users
// go. ai-generated: the whole test (egress hardening).
func TestLogTrafficNamesNoDestination(t *testing.T) {
	var buf bytes.Buffer
	oldWriter, oldFlags := log.Writer(), log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
	})

	logTraffic("sid-1", "secret-destination.test:4321", 11, 17)

	got := buf.String()
	if got != "traffic: session=sid-1 in=11 out=17\n" {
		t.Fatalf("traffic line = %q", got)
	}
	if strings.Contains(got, "secret-destination") || strings.Contains(got, "4321") {
		t.Fatalf("traffic line names the destination: %q", got)
	}
}

// A server that starts again - a lifecycle rotation, a restart inside one
// process - starts from the keys file as it is now, not from the keys the
// process was given when it began: a key the file lost since must not come
// back.
//
// ai-generated: the whole test (reload of crypto.keys_file on SIGHUP).
func TestServerKeysAreReadAgainFromTheFileTheyCameFrom(t *testing.T) {
	cfg := Config{
		KeysHex: []string{"stale"},
		KeyReload: &server.KeyReload{
			Read: func() ([]string, error) { return []string{"fresh"}, nil },
		},
	}
	got, err := serverKeys(cfg)
	if err != nil || !reflect.DeepEqual(got, []string{"fresh"}) {
		t.Fatalf("serverKeys() = %v, %v, want the keys the file holds now", got, err)
	}
}

func TestServerKeysWithoutAFileAreTheKeysItWasGiven(t *testing.T) {
	cfg := Config{KeysHex: []string{"given"}}
	got, err := serverKeys(cfg)
	if err != nil || !reflect.DeepEqual(got, []string{"given"}) {
		t.Fatalf("serverKeys() = %v, %v, want the configured keys", got, err)
	}
}

// A file that cannot be read at a restart stops the run: a server never
// falls back to a ring its operator has replaced.
func TestServerKeysFailWhenTheFileCannotBeRead(t *testing.T) {
	errGone := errors.New("keys file: gone")
	cfg := Config{
		KeysHex:   []string{"stale"},
		KeyReload: &server.KeyReload{Read: func() ([]string, error) { return nil, errGone }},
	}
	if _, err := serverKeys(cfg); !errors.Is(err, errGone) {
		t.Fatalf("serverKeys() error = %v, want the read's error", err)
	}
}

// A server whose net.ice_local_ip is no address of the host would gather
// nothing; it refuses to start and says why. ai-generated (net.ice_local_ip).
func TestRunRefusesAnICELocalIPTheHostDoesNotHave(t *testing.T) {
	err := Run(context.Background(), Config{Mode: "srv", ICELocalIP: "192.0.2.254"})
	if !errors.Is(err, engine.ErrICELocalIPNotLocal) {
		t.Fatalf("Run: err = %v, want ErrICELocalIPNotLocal", err)
	}
}

// Validate refuses a net.ice_local_ip no interface gathers on, so a profile
// holding one is skipped on its own. ai-generated (net.ice_local_ip).
func TestValidateRefusesABadICELocalIP(t *testing.T) {
	RegisterDefaults()
	good := ApplyDefaults(Config{
		Mode: ModeSrv, Provider: "jitsi", Transport: "datachannel", RoomID: "https://meet.example/room",
		KeyHex: "0000000000000000000000000000000000000000000000000000000000000001", DNSServer: "8.8.8.8:53",
	})
	good.ICELocalIP = "192.0.2.10"
	if err := Validate(good); err != nil {
		t.Fatalf("Validate with a good ice_local_ip: %v", err)
	}
	for _, bad := range []string{"224.0.0.1", "::ffff:0.0.0.0", "not-an-ip"} {
		cfg := good
		cfg.ICELocalIP = bad
		if err := Validate(cfg); !errors.Is(err, engine.ErrICELocalIP) {
			t.Errorf("Validate(ice_local_ip %q) = %v, want ErrICELocalIP", bad, err)
		}
	}
}
