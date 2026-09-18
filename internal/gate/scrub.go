package gate

import (
	"cmp"
	"encoding/hex"
	"fmt"
	"os"
	"slices"
	"strings"
)

// ai-generated: the whole file (the scrubber every log passes before it
// leaves the run).

// What stands in for a secret of the run in a log that leaves it.
const (
	placeholderKey  = "<key>"
	placeholderRoom = "<room>"
)

// keyLen is the length of a key in hex: 32 bytes.
const keyLen = 64

// Scrub replaces every secret of the run in text: a key (64 hex) becomes
// <key>, every other secret (a room URL, a room id, a slug, a channel id, a
// token) becomes <room>. Empty secrets are ignored. Text is scanned once from
// the left and each secret is replaced whole where it starts, the longer one
// where two start at the same place, so a slug cannot leave the rest of its
// room URL behind and a placeholder is never scrubbed again.
func Scrub(text string, secrets ...string) string {
	sorted := slices.DeleteFunc(slices.Clone(secrets), func(s string) bool { return s == "" })
	if len(sorted) == 0 {
		return text
	}
	slices.SortStableFunc(sorted, func(a, b string) int { return cmp.Compare(len(b), len(a)) })
	pairs := make([]string, 0, 2*len(sorted))
	for _, s := range sorted {
		pairs = append(pairs, s, placeholder(s))
	}
	return strings.NewReplacer(pairs...).Replace(text)
}

// placeholder is <key> for a 64-hex key and <room> for any other secret.
func placeholder(secret string) string {
	if len(secret) != keyLen {
		return placeholderRoom
	}
	if _, err := hex.DecodeString(secret); err != nil {
		return placeholderRoom
	}
	return placeholderKey
}

// ScrubFile writes to dst a copy of the log at src with the run's secrets
// replaced: how a raw log kept in a private directory becomes an artifact.
func ScrubFile(src, dst string, secrets ...string) error {
	raw, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("read log: %w", err)
	}
	return writeScrubbed(dst, string(raw), secrets)
}

// writeScrubbed stores text at path with the secrets replaced.
func writeScrubbed(path, text string, secrets []string) error {
	clean := []byte(Scrub(text, secrets...))
	if err := os.WriteFile(path, clean, 0o644); err != nil { //nolint:gosec // scrubbed: an artifact anyone may read
		return fmt.Errorf("write scrubbed log: %w", err)
	}
	return nil
}
