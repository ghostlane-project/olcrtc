package max

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/gorilla/websocket"
)

// parseNotification decodes the connection assignment from the first
// signaling frame.
func parseNotification(msg *sigMessage, out *connectionNotification) error {
	return json.Unmarshal(msg.Data, out)
}

// jsonUnmarshal is a tiny alias to keep readLoop tidy.
func jsonUnmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// parseUID reads a numeric uid from the credentials' extras.
func parseUID(s string) int64 {
	if s == "" {
		return 0
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

// shortConv shortens a conversation uuid for logs.
func shortConv(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// stripCandidates removes ICE candidate lines from an SDP: candidates
// trickle one per relayed frame (the ~3.5KB relay cap), so the exchange
// itself stays lean.
func stripCandidates(sdp string) string {
	kept := make([]string, 0, 64)
	for _, ln := range strings.Split(sdp, "\r\n") {
		if strings.HasPrefix(ln, "a=candidate:") {
			continue
		}
		kept = append(kept, ln)
	}
	return strings.Join(kept, "\r\n")
}

// ensure websocket import stays referenced by signaling.go builds.
var _ = websocket.TextMessage
