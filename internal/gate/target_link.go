package gate

import (
	"context"
	"fmt"

	"github.com/openlibrecommunity/olcrtc/internal/link"
)

// ai-generated: the whole file (the link target: a server somebody else
// runs, reached through an olcrtc:// link).

// LinkTarget is the fleet's node behind an olcrtc:// link. It offers exactly
// the one pair the link names and hands the client the link's room as the
// app would: as written, with the server's own channel (none).
type LinkTarget struct {
	l    link.Link
	load LoadURLs
	dns  string
}

// NewLinkTarget wraps a parsed link, the public load URLs and the resolver
// the client uses.
func NewLinkTarget(l link.Link, load LoadURLs, dns string) *LinkTarget {
	return &LinkTarget{l: l, load: load, dns: dns}
}

// Name implements Target.
func (t *LinkTarget) Name() string { return targetLink }

// Platform implements Target.
func (t *LinkTarget) Platform() string { return platformEngineLinux }

// Pairs is the link's one pair.
func (t *LinkTarget) Pairs() []Pair {
	return []Pair{{Provider: t.l.Provider, Transport: t.l.Transport}}
}

// Load is the public URLs the fleet's node reaches.
func (t *LinkTarget) Load() LoadURLs { return t.load }

// Open returns the link's endpoint; there is no server to start or stop. A
// pair other than the link's is refused, and the refusal names pairs only,
// never the link's room or key.
func (t *LinkTarget) Open(_ context.Context, p Pair, _ string, _ OpenOptions) (Endpoint, func(), error) {
	if own := t.Pairs()[0]; p != own {
		return Endpoint{}, nil, fmt.Errorf("%w: the link carries %s, not %s", ErrPairNotCarried, own, p)
	}
	return Endpoint{
		Provider: t.l.Provider, Transport: t.l.Transport, Room: t.l.Room, Key: t.l.Key,
		DNS: t.dns, VP8FPS: t.l.VP8FPS, VP8Batch: t.l.VP8Batch,
	}, func() {}, nil
}
