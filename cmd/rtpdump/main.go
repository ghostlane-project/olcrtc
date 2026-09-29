// EXPERIMENT (temporary, not for commit): a Pion answerer that records the
// RTP a browser publishes. It serves one page that opens the fake camera,
// POSTs its offer, plays the answer, and every VP8 packet received is
// written to a file as length-prefixed RTP for replay into VK.
// Usage: rtpdump LISTEN_ADDR OUT_FILE
package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"

	"github.com/pion/interceptor"
	"github.com/pion/webrtc/v4"
)

const page = `<!doctype html><html><body><script>
(async () => {
  const s = await navigator.mediaDevices.getUserMedia({video: {width: 640, height: 360, frameRate: 15}, audio: false});
  const pc = new RTCPeerConnection();
  pc.addTransceiver(s.getVideoTracks()[0], {direction: 'sendonly',
    sendEncodings: [{rid: 'l', scaleResolutionDownBy: 2, maxBitrate: 180000}, {rid: 'm', maxBitrate: 500000}]});
  const offer = await pc.createOffer();
  await pc.setLocalDescription(offer);
  await new Promise(r => setTimeout(r, 1500));
  const res = await fetch('/offer', {method: 'POST', body: JSON.stringify(pc.localDescription)});
  await pc.setRemoteDescription(await res.json());
  window.__pc = pc;
  setInterval(async () => {
    const st = [...(await pc.getStats()).values()].filter(x => x.type === 'outbound-rtp');
    document.title = JSON.stringify(st.map(x => [x.rid, x.packetsSent, x.framesEncoded, x.keyFramesEncoded]));
  }, 1000);
})();
</script></body></html>`

func main() {
	out, err := os.Create(os.Args[2])
	if err != nil {
		panic(err)
	}
	var mu sync.Mutex
	m := &webrtc.MediaEngine{}
	// VP8 with a RED wrapper on offer, as the VK SFU offers it: a browser
	// then sends RED (PT 104) and the recording shows the exact block layout.
	for _, c := range []webrtc.RTPCodecParameters{
		{RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000, RTCPFeedback: []webrtc.RTCPFeedback{{Type: "nack"}, {Type: "nack", Parameter: "pli"}, {Type: "ccm", Parameter: "fir"}, {Type: "goog-remb"}, {Type: "transport-cc"}}}, PayloadType: 100},
		{RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: "video/rtx", ClockRate: 90000, SDPFmtpLine: "apt=100"}, PayloadType: 101},
		{RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: "video/red", ClockRate: 90000}, PayloadType: 104},
		{RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: "video/rtx", ClockRate: 90000, SDPFmtpLine: "apt=104"}, PayloadType: 105},
		{RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: "video/ulpfec", ClockRate: 90000}, PayloadType: 106},
	} {
		if err := m.RegisterCodec(c, webrtc.RTPCodecTypeVideo); err != nil {
			panic(err)
		}
	}
	// NACK generator + receiver reports, so the browser retransmits what
	// the loopback drops and the recording has no holes.
	if err := webrtc.ConfigureSimulcastExtensionHeaders(m); err != nil {
		panic(err)
	}
	registry := &interceptor.Registry{}
	if err := webrtc.ConfigureNack(m, registry); err != nil {
		panic(err)
	}
	if err := webrtc.ConfigureRTCPReports(registry); err != nil {
		panic(err)
	}
	api := webrtc.NewAPI(webrtc.WithMediaEngine(m), webrtc.WithInterceptorRegistry(registry))
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, page) })
	http.HandleFunc("/offer", func(w http.ResponseWriter, r *http.Request) {
		var offer webrtc.SessionDescription
		if err := json.NewDecoder(r.Body).Decode(&offer); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		pc, err := api.NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			panic(err)
		}
		pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
			fmt.Fprintln(os.Stderr, "track", track.RID(), track.SSRC(), track.Codec().MimeType)
			for {
				pkt, _, err := track.ReadRTP()
				if err != nil {
					return
				}
				raw, err := pkt.Marshal()
				if err != nil {
					continue
				}
				mu.Lock()
				var hdr [6]byte
				hdr[0] = byte(len(track.RID()))
				copy(hdr[1:], track.RID())
				hdr[3] = byte(pkt.PayloadType)
				binary.BigEndian.PutUint16(hdr[4:], uint16(len(raw))) //nolint:gosec // packets are < 64 KiB
				_, _ = out.Write(hdr[:])
				_, _ = out.Write(raw)
				mu.Unlock()
			}
		})
		if err := pc.SetRemoteDescription(offer); err != nil {
			panic(err)
		}
		answer, err := pc.CreateAnswer(nil)
		if err != nil {
			panic(err)
		}
		done := webrtc.GatheringCompletePromise(pc)
		if err := pc.SetLocalDescription(answer); err != nil {
			panic(err)
		}
		<-done
		_ = json.NewEncoder(w).Encode(pc.LocalDescription())
	})
	fmt.Fprintln(os.Stderr, "listening", os.Args[1])
	panic(http.ListenAndServe(os.Args[1], nil))
}
