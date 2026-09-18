package gate

import (
	"errors"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ai-generated: whole file, cover for the scrubber and the logs it writes.

func logPrintf(format string, a ...any) { log.Printf(format, a...) }

func TestScrubReplacesRoomsAndKeys(t *testing.T) {
	key := strings.Repeat("ab", 32)
	room := "https://meet.example.org/gate-7f3a9c"
	in := "jitsi: joining MUC meet.example.org/gate-7f3a9c as X … key=" + key + " room=" + room
	out := Scrub(in, key, room, "gate-7f3a9c")
	if strings.Contains(out, key) || strings.Contains(out, "gate-7f3a9c") {
		t.Fatalf("scrubbed = %q", out)
	}
	if !strings.Contains(out, "<key>") || !strings.Contains(out, "<room>") {
		t.Fatalf("placeholders missing: %q", out)
	}
	if Scrub("plain", "") != "plain" {
		t.Fatal("empty secret must be ignored")
	}
}

// A slug replaced before its URL would leave the URL's host behind, so the
// longest secret wins wherever two start at the same place, whatever order
// they were given in.
func TestScrubReplacesTheLongestSecretWhole(t *testing.T) {
	room := "https://meet.example.org/gate-7f3a9c"
	got := Scrub("room="+room+" slug=gate-7f3a9c channel=gate-7f3a9c-ch", "gate-7f3a9c", room, "gate-7f3a9c-ch")
	if want := "room=<room> slug=<room> channel=<room>"; got != want {
		t.Fatalf("Scrub = %q, want %q", got, want)
	}
}

func TestCellLogWriteScrubbed(t *testing.T) {
	c := StartCapture()
	defer c.Stop()
	l := c.Begin()
	logPrintf("joined room secret-room-1 with %s", strings.Repeat("cd", 32))
	p := filepath.Join(t.TempDir(), "cell.log")
	if err := l.WriteScrubbed(p, strings.Repeat("cd", 32), "secret-room-1"); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(p)
	if strings.Contains(string(raw), "secret-room-1") || strings.Contains(string(raw), "cdcd") {
		t.Fatalf("file = %q", string(raw))
	}
	if !strings.Contains(string(raw), "joined room <room> with <key>") {
		t.Fatalf("file lost the line: %q", string(raw))
	}
}

// A server's raw log stays in a private directory; what is uploaded is the
// copy ScrubFile writes, clean of the run's room, key, channel and token.
func TestScrubFileLeavesNoSecretOfTheRun(t *testing.T) {
	key := strings.Repeat("0f", 32)
	slug := "gate-0123456789ab"
	room := "https://meet.example.invalid/" + slug
	channel := "gate-ba9876543210"
	token := "fake-wb-token-not-a-real-one"
	raw := strings.Join([]string{
		"2026/09/18 10:00:00 Connecting transport=datachannel provider=jitsi ...",
		"2026/09/18 10:00:00 jitsi: joining MUC meet.example.invalid/" + slug + " as olcrtc …",
		"2026/09/18 10:00:01 j: rejoin joining room " + slug + " as olcrtc",
		"2026/09/18 10:00:02 room=" + room + " key=" + key + " channel=" + channel + " token=" + token,
		"2026/09/18 10:00:03 Link connected",
	}, "\n") + "\n"
	dir := t.TempDir()
	src, dst := filepath.Join(dir, "srv-raw.log"), filepath.Join(dir, "srv.log")
	if err := os.WriteFile(src, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ScrubFile(src, dst, room, key, slug, channel, token); err != nil {
		t.Fatal(err)
	}
	out, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{key, slug, channel, token, "0123456789ab"} {
		if strings.Contains(string(out), secret) {
			t.Fatalf("scrubbed log still carries %q:\n%s", secret, out)
		}
	}
	for _, kept := range []string{"joining MUC meet.example.invalid/<room> as olcrtc", "room=<room> key=<key>", "Link connected"} {
		if !strings.Contains(string(out), kept) {
			t.Fatalf("scrubbed log lost %q:\n%s", kept, out)
		}
	}
	if err := ScrubFile(filepath.Join(dir, "missing.log"), dst); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("ScrubFile of a missing log = %v, want fs.ErrNotExist", err)
	}
}
