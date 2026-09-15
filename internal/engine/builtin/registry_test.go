package builtin_test

import (
	"slices"
	"testing"

	"github.com/openlibrecommunity/olcrtc/internal/auth"
	"github.com/openlibrecommunity/olcrtc/internal/engine"
	"github.com/openlibrecommunity/olcrtc/internal/engine/builtin"
)

// Every auth provider names the engine that consumes its credentials, and
// that engine has to exist in the same binary, whatever build tags it was
// made with. The lean mobile build once left the livekit engine out on the
// belief that no phone-side provider used it; wbstream does, and the app
// failed at session start with "engine new: engine not found" (olcbox#22).
func TestEveryAuthProviderHasItsEngine(t *testing.T) {
	builtin.RegisterDefaults()
	engines := engine.Available()
	for _, name := range auth.Available() {
		provider, err := auth.Get(name)
		if err != nil {
			t.Fatalf("auth.Get(%q) error = %v", name, err)
		}
		if want := provider.Engine(); !slices.Contains(engines, want) {
			t.Errorf("auth provider %q needs engine %q, which this build does not register (engines: %v)",
				name, want, engines)
		}
	}
}
