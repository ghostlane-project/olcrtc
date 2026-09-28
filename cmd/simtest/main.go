// EXPERIMENT (temporary, not for commit): does a Pion sender with two
// simulcast encodings (AddTrack + AddEncoding, rid l/m, mid/rid header
// extensions) deliver BOTH streams to a Pion receiver? Runs one local
// PeerConnection pair, sends 60 packets on each layer and reports which rids
// the receiver saw. If the receiver misses "m", the second encoding is broken
// on our side; if it sees both, the VK SFU is what drops it.
package main

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/sdp/v3"
	"github.com/pion/webrtc/v4"
)

type stamper struct {
	interceptor.NoOp
}

func (stamper) BindLocalStream(info *interceptor.StreamInfo, writer interceptor.RTPWriter) interceptor.RTPWriter {
	var midID, ridID uint8
	for _, ext := range info.RTPHeaderExtensions {
		switch ext.URI {
		case sdp.SDESMidURI:
			midID = uint8(ext.ID)
		case sdp.SDESRTPStreamIDURI:
			ridID = uint8(ext.ID)
		}
	}
	rid, _ := ridOf.Load(info.SSRC)
	fmt.Fprintf(os.Stderr, "sender: bind ssrc=%d mid ext=%d rid ext=%d rid=%v\n", info.SSRC, midID, ridID, rid)
	return interceptor.RTPWriterFunc(func(h *rtp.Header, p []byte, a interceptor.Attributes) (int, error) {
		if midID != 0 {
			_ = h.SetExtension(midID, []byte("0"))
		}
		if ridID != 0 && rid != nil {
			_ = h.SetExtension(ridID, []byte(rid.(string)))
		}
		return writer.Write(h, p, a)
	})
}

type factory struct{}

func (factory) NewInterceptor(string) (interceptor.Interceptor, error) { return &stamper{}, nil }

var ridOf sync.Map

func newAPI(sender bool) *webrtc.API {
	m := &webrtc.MediaEngine{}
	if err := m.RegisterDefaultCodecs(); err != nil {
		panic(err)
	}
	for _, uri := range []string{sdp.SDESMidURI, sdp.SDESRTPStreamIDURI, sdp.SDESRepairRTPStreamIDURI} {
		if err := m.RegisterHeaderExtension(webrtc.RTPHeaderExtensionCapability{URI: uri}, webrtc.RTPCodecTypeVideo); err != nil {
			panic(err)
		}
	}
	r := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(m, r); err != nil {
		panic(err)
	}
	if sender {
		r.Add(factory{})
	}
	return webrtc.NewAPI(webrtc.WithMediaEngine(m), webrtc.WithInterceptorRegistry(r))
}

func main() {
	offerer, err := newAPI(true).NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		panic(err)
	}
	answerer, err := newAPI(false).NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		panic(err)
	}
	trackL, _ := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000}, "video", "proofkit", webrtc.WithRTPStreamID("l"))
	trackM, _ := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000}, "video", "proofkit", webrtc.WithRTPStreamID("m"))
	sender, err := offerer.AddTrack(trackL)
	if err != nil {
		panic(err)
	}
	if err := sender.AddEncoding(trackM); err != nil {
		panic(err)
	}
	for _, e := range sender.GetParameters().Encodings {
		ridOf.Store(uint32(e.SSRC), e.RID)
		fmt.Fprintf(os.Stderr, "sender encoding rid=%s ssrc=%d\n", e.RID, e.SSRC)
	}
	seen := sync.Map{}
	answerer.OnTrack(func(t *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		fmt.Fprintf(os.Stderr, "receiver: track rid=%q ssrc=%d\n", t.RID(), t.SSRC())
		n := 0
		for {
			if _, _, err := t.ReadRTP(); err != nil {
				return
			}
			n++
			seen.Store(t.RID(), n)
		}
	})
	offer, err := offerer.CreateOffer(nil)
	if err != nil {
		panic(err)
	}
	if err := offerer.SetLocalDescription(offer); err != nil {
		panic(err)
	}
	<-webrtc.GatheringCompletePromise(offerer)
	if err := answerer.SetRemoteDescription(*offerer.LocalDescription()); err != nil {
		panic(err)
	}
	answer, err := answerer.CreateAnswer(nil)
	if err != nil {
		panic(err)
	}
	if err := answerer.SetLocalDescription(answer); err != nil {
		panic(err)
	}
	<-webrtc.GatheringCompletePromise(answerer)
	if err := offerer.SetRemoteDescription(*answerer.LocalDescription()); err != nil {
		panic(err)
	}
	fmt.Fprintln(os.Stderr, "offer publish lines:", strings.Join(filter(offerer.LocalDescription().SDP, "a=rid", "a=simulcast", "a=ssrc"), " | "))
	time.Sleep(2 * time.Second)
	var seq uint16
	for i := 0; i < 60; i++ {
		for _, t := range []*webrtc.TrackLocalStaticRTP{trackL, trackM} {
			seq++
			p := &rtp.Packet{Header: rtp.Header{Version: 2, SequenceNumber: seq, Timestamp: uint32(i) * 6000, Marker: true}, Payload: append([]byte{0x90, 0xE0, 0x80, 0x01, 0x01, 0x00}, make([]byte, 200)...)}
			if err := t.WriteRTP(p); err != nil {
				fmt.Fprintln(os.Stderr, "write", t.RID(), err)
			}
		}
		time.Sleep(66 * time.Millisecond)
	}
	time.Sleep(time.Second)
	seen.Range(func(k, v any) bool { fmt.Fprintf(os.Stderr, "received rid=%q packets=%v\n", k, v); return true })
}

func filter(s string, prefixes ...string) []string {
	var out []string
	for _, l := range strings.Split(strings.ReplaceAll(s, "\r\n", "\n"), "\n") {
		for _, p := range prefixes {
			if strings.HasPrefix(l, p) {
				out = append(out, l)
			}
		}
	}
	return out
}
