// Package roomhint reads the hint a room link may carry for the client side
// alone: "…?ice=tcp" asks the engine to reach the relay over ICE-TCP instead
// of UDP, for a network that lets TCP to the relay through and starves UDP (a
// mobile network under an allow list). The server joins the same room by its
// plain link, so the hint is no part of the room's identity: it is cut before
// the link names the room, the binding token included.
//
// ai-generated: the whole package.
package roomhint

import "strings"

// ICE modes a hint can ask for. ICEDefault is a link without one: the engine
// does what it always did.
const (
	ICEDefault = ""
	ICEUDP     = "udp"
	ICETCP     = "tcp"
	ICEBoth    = "both"
)

const iceKey = "ice="

// Split returns the room link without its hint, and the ICE mode the hint
// asks for. A link without a hint, or with one this package does not know,
// comes back byte for byte as it was, so its binding token is unchanged.
func Split(room string) (string, string) {
	trimmed := strings.TrimSpace(room)
	for _, sep := range []string{"?", "&"} {
		at := strings.LastIndex(trimmed, sep+iceKey)
		if at < 0 {
			continue
		}
		switch mode := trimmed[at+len(sep)+len(iceKey):]; mode {
		case ICEUDP, ICETCP, ICEBoth:
			return trimmed[:at], mode
		}
	}
	return room, ICEDefault
}
