//go:build olcrtc_lean

package session

import "github.com/openlibrecommunity/olcrtc/internal/transport"

// The olcrtc_lean build tag is for the mobile bind, where the process is
// killed for its memory rather than swapped. videochannel carries data as QR
// codes in a video track; a phone never uses it, but the QR codec it links
// builds its charset tables at init, about 1 MB of heap that sat in every
// packet tunnel extension for a transport nobody selected. Under the tag the
// transport is not registered, and asking for it fails at session start
// with ErrTransportNotFound, the same answer any unknown name gets. The
// livekit engine is left out the same way; see the builtin package.
func registerVideoTransport() {}

// The transport is not linked, so there are no options to build for it.
func videoTransportOptions(Config) transport.Options { return nil }
