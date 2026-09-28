package vkcalls

import (
	"encoding/base64"
	"errors"
	"testing"
)

// The registry vectors are frames captured from the live SFU (spike
// hybrid-09): kind-1 maps of a camera stream description to its compact id.
func TestParseRegistry(t *testing.T) {
	cases := []struct {
		name    string
		b64     string
		want    Registry
		wantErr bool
	}{
		{
			name: "camera stream compact 1",
			// 01 81 b5 <21-byte key "u588003930589:sCAMERA"> 01, live capture
			b64:  "AYG1dTU4ODAwMzkzMDU4OTpzQ0FNRVJBAQ==",
			want: Registry{"u588003930589:sCAMERA": 1},
		},
		{
			name:    "empty frame",
			b64:     "",
			wantErr: true,
		},
		{
			name:    "truncated",
			b64:     "AQE=",
			wantErr: true,
		},
		{
			name:    "not a map",
			b64:     "AZAA",
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := base64.StdEncoding.DecodeString(tc.b64)
			if err != nil {
				t.Fatal(err)
			}
			got, err := ParseRegistry(raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("registry size %d, want %d", len(got), len(tc.want))
			}
			for key, id := range tc.want {
				if got[key] != id {
					t.Fatalf("key %q: compact %d, want %d", key, got[key], id)
				}
			}
		})
	}
}

// TestParseRegistryOtherKind checks non-registry frames are marked
// distinctly: the notification channel carries several kinds.
func TestParseRegistryOtherKind(t *testing.T) {
	if _, err := ParseRegistry([]byte{0x02, 0x91, 0x00}); !errors.Is(err, ErrNotRegistry) {
		t.Fatalf("want ErrNotRegistry, got %v", err)
	}
}

// The golden bytes are the compact-addressed form the live SFU accepted
// (spike hybrid-11): update-display-layout, sequence 1, one stream at
// compact id 0, priority 1, 640x360, fit=cv.
func TestEncodeLayoutGolden(t *testing.T) {
	frame, err := EncodeLayout(1, []LayoutEntry{{Key: "u604532170515:sCAMERA", Compact: 0, ByCompact: true, Priority: 1, Width: 640, Height: 360}})
	if err != nil {
		t.Fatal(err)
	}
	const golden = "000001c091c40a000001d10280d1016800c0"
	if got := hexOf(frame); got != golden {
		t.Fatalf("frame %s, want %s", got, golden)
	}
}

// TestEncodeLayoutStringKey pins the string-addressed form the SDK falls back
// to without a registry: priority 0 is a set value, encoded as fixint 0.
func TestEncodeLayoutStringKey(t *testing.T) {
	frame, err := EncodeLayout(2, []LayoutEntry{{Key: "42:sCAMERA", Priority: 0}})
	if err != nil {
		t.Fatal(err)
	}
	const golden = "000002c091c410aa34323a7343414d4552410000c0c000c0"
	if got := hexOf(frame); got != golden {
		t.Fatalf("frame %s, want %s", got, golden)
	}
}

func TestEncodeLayoutEmpty(t *testing.T) {
	if _, err := EncodeLayout(1, nil); err == nil {
		t.Fatal("expected error for empty layout")
	}
}

// TestRegistryLayoutFor pins the subscribe-all policy: every registry stream,
// compact-addressed, stable key order.
func TestRegistryLayoutFor(t *testing.T) {
	r := Registry{"b:sCAMERA": 1, "a:sCAMERA": 0}
	entries, err := r.LayoutFor()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("entries %d, want 2", len(entries))
	}
	if entries[0].Key != "a:sCAMERA" || !entries[0].ByCompact || entries[0].Compact != 0 {
		t.Fatalf("first entry %+v", entries[0])
	}
	if entries[1].Key != "b:sCAMERA" || entries[1].Compact != 1 {
		t.Fatalf("second entry %+v", entries[1])
	}
	if entries[0].Priority != 1 || entries[0].Width != 640 || entries[0].Height != 360 {
		t.Fatalf("layout fields %+v", entries[0])
	}
}

func hexOf(b []byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, 0, len(b)*2)
	for _, v := range b {
		out = append(out, digits[v>>4], digits[v&0x0f])
	}
	return string(out)
}
