package builtin

import (
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/openlibrecommunity/olcrtc/internal/engine"
)

// ai-generated: the tests below (net.ice_local_ip).

func TestEngineConfigCarriesTheICELocalIP(t *testing.T) {
	local := netip.MustParseAddr("192.0.2.10")
	got := engineConfig(Config{ICELocalIP: local}, engine.Credentials{URL: "u"}, nil)
	if got.ICELocalIP != local || got.URL != "u" {
		t.Fatalf("engine config = %+v", got)
	}
}

// Every carrier builds its pion settings through the session's helper
// (engine.Config.Pion().Options or a PionSession it was handed), so a session
// field such as the ICE local IP reaches every carrier and none is left behind.
func TestEveryCarrierBuildsPionSettingsThroughTheSessionHelper(t *testing.T) {
	call := regexp.MustCompile(`engine\.NewPionSettings\(([^\n]*)`)
	files, err := filepath.Glob(filepath.Join("..", "*", "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		body, err := os.ReadFile(f) // #nosec G304 -- the repository's own sources
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range call.FindAllStringSubmatch(string(body), -1) {
			seen++
			if !strings.Contains(m[1], ".Options(engine.PionSettingsOptions{") {
				t.Errorf("%s: engine.NewPionSettings(%s ...) does not go through the session's Options helper", f, m[1])
			}
		}
	}
	if seen < 7 {
		t.Fatalf("found %d carrier calls of engine.NewPionSettings, want at least 7", seen)
	}
}
