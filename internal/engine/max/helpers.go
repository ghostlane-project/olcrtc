package max

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/gorilla/websocket"
)

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

// maxCapabilities is the ok-calls capability set the SFU validates
// (schema-extracted and server-confirmed during the 2026-10-18 spike).
func maxCapabilities() map[string]any {
	return map[string]any{
		"estimatedPerformanceIndex": 1000, "audioMix": true, "consumerUpdate": true,
		"producerNotificationDataChannelVersion": 8, "producerCommandDataChannelVersion": 3,
		"consumerScreenDataChannelVersion": 1, "producerScreenDataChannelVersion": 1,
		"asrDataChannelVersion": 0, "animojiDataChannelVersion": 1, "animojiBackendRender": true,
		"onDemandTracks": true, "unifiedPlan": true, "singleSession": true, "videoTracksCount": 1,
		"red": true, "audioShare": false, "fastScreenShare": false, "videoSuspend": true,
		"simulcast": false, "simulcastNativeOrder": true, "consumerFastScreenShare": false,
		"consumerFastScreenShareQualityOnDemand": false, "transparentAudio": false,
	}
}

// clientQuery completes the endpoint's query with the web client's
// parameters: the SFU answers a bare join endpoint with "Parameter
// appVersion is required" (measured 2026-10-18).
func clientQuery(endpoint string) string {
	sep := "?"
	if strings.Contains(endpoint, "?") {
		sep = "&"
	}
	return endpoint + sep + "platform=web&appVersion=26.10.3&version=3&device=web&capabilities=webrtc&clientType=SDK_JS"
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
