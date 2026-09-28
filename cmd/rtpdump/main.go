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
	if err := m.RegisterDefaultCodecs(); err != nil {
		panic(err)
	}
	api := webrtc.NewAPI(webrtc.WithMediaEngine(m))
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
			fmt.Println("track", track.RID(), track.SSRC(), track.Codec().MimeType)
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
	fmt.Println("listening", os.Args[1])
	panic(http.ListenAndServe(os.Args[1], nil))
}
