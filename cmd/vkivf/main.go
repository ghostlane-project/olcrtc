// EXPERIMENT (temporary, not for commit): publishes a libvpx VP8 IVF file
// as the camera of a vkcalls session, looping, for secs seconds.
// Usage: vkivf ROOM_FILE IVF_FILE SECS
package main

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/pion/webrtc/v4/pkg/media/ivfreader"

	"github.com/openlibrecommunity/olcrtc/internal/engine/builtin"
	"github.com/openlibrecommunity/olcrtc/internal/engine/vkcalls"
	"github.com/openlibrecommunity/olcrtc/internal/logger"
)

func main() {
	logger.SetVerbose(true)
	raw, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	secs, _ := strconv.Atoi(os.Args[3])
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(secs)*time.Second)
	defer cancel()
	builtin.RegisterDefaults()
	sess, err := builtin.Open(ctx, "vkcalls", builtin.Config{RoomURL: strings.TrimSpace(string(raw)), Name: cmp.Or(os.Getenv("PUB_NAME"), "ProofKit IVF"), DNSServer: "8.8.8.8:53"})
	if err != nil {
		fmt.Println("open:", err)
		os.Exit(1)
	}
	withTID := os.Getenv("VP8_TID") == "1" || os.Getenv("VP8_RED") == "1"
	vp8PT, _ := strconv.Atoi(os.Getenv("VP8_PT"))
	if vp8PT == 0 {
		vp8PT = 100
	}
	track, err := webrtc.NewTrackLocalStaticSample(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000}, "video", "proofkit")
	if err != nil {
		panic(err)
	}
	withRED := os.Getenv("VP8_RED") == "1"
	rtpMime := webrtc.MimeTypeVP8
	if withRED {
		rtpMime = "video/red"
	}
	rtpTrack, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: rtpMime, ClockRate: 90000}, "video", "proofkit")
	if err != nil {
		panic(err)
	}
	var local webrtc.TrackLocal = track
	if withTID {
		local = rtpTrack
	}
	simIVF := os.Getenv("VP8_SIM_IVF") // second layer's IVF: rid m
	var trackM *webrtc.TrackLocalStaticRTP
	var sampleL, sampleM *webrtc.TrackLocalStaticSample
	if simIVF != "" && os.Getenv("VP8_SAMPLE") == "1" {
		sampleL, err = webrtc.NewTrackLocalStaticSample(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000}, "video", "proofkit", webrtc.WithRTPStreamID("l"))
		if err != nil {
			panic(err)
		}
		sampleM, err = webrtc.NewTrackLocalStaticSample(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000}, "video", "proofkit", webrtc.WithRTPStreamID("m"))
		if err != nil {
			panic(err)
		}
		if v, ok := sess.(interface{ AddVideoTrack(webrtc.TrackLocal) error }); ok {
			_ = v.AddVideoTrack(sampleL)
			_ = v.AddVideoTrack(sampleM)
		}
		withTID = false
		local = sampleL
	} else if simIVF != "" {
		// Both layers share the track id and stream: the engine makes the
		// second one an encoding of the first's sender.
		rtpTrack, err = webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: rtpMime, ClockRate: 90000}, "video", "proofkit", webrtc.WithRTPStreamID("l"))
		if err != nil {
			panic(err)
		}
		trackM, err = webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: rtpMime, ClockRate: 90000}, "video", "proofkit", webrtc.WithRTPStreamID("m"))
		if err != nil {
			panic(err)
		}
		local = rtpTrack
	}
	// VP8_RTP_FILE: replay a recording of a browser's simulcast publish
	// (rtpdump format: rid-len, rid, 0, 0, len16, packet) through two RTP
	// tracks, the VP8 payload untouched; SSRC and PT are rebound by the
	// track, sequence numbers and timestamps re-based per layer.
	rtpFile := os.Getenv("VP8_RTP_FILE")
	if rtpFile != "" {
		replayMime := webrtc.MimeTypeVP8
		if withRED {
			replayMime = "video/red"
		}
		rtpTrack, err = webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: replayMime, ClockRate: 90000}, "video", "proofkit", webrtc.WithRTPStreamID("l"))
		if err != nil {
			panic(err)
		}
		trackM, err = webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: replayMime, ClockRate: 90000}, "video", "proofkit", webrtc.WithRTPStreamID("m"))
		if err != nil {
			panic(err)
		}
		local = rtpTrack
		withTID = false
		sampleL, sampleM = nil, nil
	}
	if sampleL == nil {
		if v, ok := sess.(interface{ AddVideoTrack(webrtc.TrackLocal) error }); ok {
			_ = v.AddVideoTrack(local)
			if trackM != nil {
				_ = v.AddVideoTrack(trackM)
			}
		}
	}
	// Keyframe requests from the SFU (FIR/PLI), fanned out to the layer they
	// name; a layer answers by restarting its file at the next keyframe.
	var kfMu sync.Mutex
	kfWant := map[uint32]bool{}
	kfSeen := map[string]bool{}
	kfLayers := 1
	if simIVF != "" {
		kfLayers = 2
	}
	go func() {
		for ssrc := range vkcalls.KeyframeRequests {
			kfMu.Lock()
			kfWant[ssrc] = true
			kfMu.Unlock()
			fmt.Println("keyframe requested for ssrc", ssrc)
		}
	}()
	layerSSRC := func(rid string) uint32 {
		for _, s := range senderSSRCs(sess) {
			if s.rid == rid {
				return s.ssrc
			}
		}
		return 0
	}
	start := time.Now()
	playLayer := func(path string, write func([]byte)) {
		frames := loadIVF(path)
		i := 0
		tick := time.NewTicker(time.Second / 15)
		defer tick.Stop()
		for ctx.Err() == nil {
			<-tick.C
			kfMu.Lock()
			want := false
			// A keyframe request for any layer restarts every layer at a
			// keyframe, as one encoder feeding all layers does; the request is
			// consumed by the last layer to see it.
			for ssrc := range kfWant {
				if ssrc == layerSSRC("l") || ssrc == layerSSRC("m") {
					want = true
				}
			}
			if want {
				kfSeen[path] = true
				if len(kfSeen) >= kfLayers {
					kfWant = map[uint32]bool{}
					kfSeen = map[string]bool{}
				}
			}
			kfMu.Unlock()
			// VP8_KF_BURST=1: a keyframe every second for the first 10 s, as a
			// live encoder answering early FIRs would.
			if os.Getenv("VP8_KF_BURST") == "1" && time.Since(start) < 10*time.Second && i%15 == 0 {
				want = true
			}
			if want || i >= len(frames) {
				// The next keyframe from here, or the file's start.
				j := i
				for j < len(frames) && frames[j][0]&1 != 0 {
					j++
				}
				if j >= len(frames) {
					j = 0
				}
				i = j
			}
			write(frames[i])
			i++
		}
	}

	if os.Getenv("PUB_AUDIO") == "1" {
		audio, aerr := webrtc.NewTrackLocalStaticSample(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2}, "audio", "proofkit")
		if aerr != nil {
			panic(aerr)
		}
		if v, ok := sess.(interface{ AddVideoTrack(webrtc.TrackLocal) error }); ok {
			_ = v.AddVideoTrack(audio)
		}
		go func() {
			silence := []byte{0xF8, 0xFF, 0xFE}
			t := time.NewTicker(20 * time.Millisecond)
			defer t.Stop()
			for range t.C {
				_ = audio.WriteSample(media.Sample{Data: silence, Duration: 20 * time.Millisecond})
			}
		}()
	}
	var seq uint16 = 1000
	var ts uint32 = 12345
	var pid uint16 = 1
	var tl0 uint8 = 1
	type layerState struct {
		seq   uint16
		ts    uint32
		pid   uint16
		tl0   uint8
		frame int // frame index within the L1T3 cycle
	}
	// Chrome's L1T3 temporal pattern per frame: TID 0,2,1,2; TL0PICIDX
	// advances on TID0; N (non-reference) on every TID2; Y at the start of
	// each 8-frame cycle (recorded 2026-09-28). VP8_L1T3=1 turns it on;
	// otherwise every frame is TID0.
	l1t3 := os.Getenv("VP8_L1T3") == "1"
	writeLayer := func(t *webrtc.TrackLocalStaticRTP, st *layerState, frame []byte) {
		const chunk = 1100
		tid, y, n := uint8(0), uint8(0), uint8(0)
		if l1t3 {
			tid = []uint8{0, 2, 1, 2}[st.frame%4]
			if st.frame%8 < 3 && tid != 0 {
				y = 1
			}
			if tid == 2 {
				n = 1
			}
		}
		for off := 0; off < len(frame); off += chunk {
			end := min(off+chunk, len(frame))
			desc := []byte{0x80 | n<<5, 0xE0, byte(0x80 | (st.pid>>8)&0x7f), byte(st.pid), st.tl0, tid<<6 | y<<5}
			if off == 0 {
				desc[0] |= 0x10
			}
			pkt := &rtp.Packet{Header: rtp.Header{Version: 2, SequenceNumber: st.seq, Timestamp: st.ts, Marker: end == len(frame)},
				Payload: append(desc, frame[off:end]...)}
			st.seq++
			if err := t.WriteRTP(pkt); err != nil {
				fmt.Println("writertp:", err)
			}
		}
		st.ts += 90000 / 15
		st.pid = (st.pid + 1) & 0x7fff
		st.frame++
		if !l1t3 || st.frame%4 == 0 {
			st.tl0++
		}
	}
	if trackM != nil && rtpFile == "" {
		go func() {
			// VP8_M_DELAY: seconds the second layer waits before its first
			// packet, so the base layer is the first stream the SFU sees.
			if d, _ := strconv.Atoi(os.Getenv("VP8_M_DELAY")); d > 0 {
				time.Sleep(time.Duration(d) * time.Second)
			}
			st := &layerState{seq: 5000, ts: 12345, pid: 1, tl0: 1}
			for ctx.Err() == nil {
				f, err := os.Open(simIVF)
				if err != nil {
					panic(err)
				}
				r, _, err := ivfreader.NewWith(f)
				if err != nil {
					panic(err)
				}
				tick := time.NewTicker(time.Second / 15)
				for ctx.Err() == nil {
					frame, _, err := r.ParseNextFrame()
					if err != nil {
						break
					}
					<-tick.C
					writeLayer(trackM, st, frame)
				}
				tick.Stop()
				_ = f.Close()
			}
		}()
	}
	writeTID := func(frame []byte) {
		const chunk = 1100
		for off := 0; off < len(frame); off += chunk {
			end := min(off+chunk, len(frame))
			desc := []byte{0x80, 0xE0, byte(0x80 | (pid>>8)&0x7f), byte(pid), tl0, 0x20}
			if off == 0 {
				desc[0] |= 0x10
			}
			payload := append(desc, frame[off:end]...)
			if withRED {
				// RFC 2198, one primary block: F=0, block PT = the offer's VP8 PT.
				payload = append([]byte{byte(vp8PT)}, payload...)
			}
			pkt := &rtp.Packet{Header: rtp.Header{Version: 2, SequenceNumber: seq, Timestamp: ts, Marker: end == len(frame)},
				Payload: payload}
			seq++
			if err := rtpTrack.WriteRTP(pkt); err != nil {
				fmt.Println("writertp:", err)
			}
		}
		ts += 90000 / 15
		pid = (pid + 1) & 0x7fff
		tl0++
	}
	if err := sess.Connect(ctx); err != nil {
		fmt.Println("connect:", err)
		os.Exit(1)
	}
	fmt.Println("IVF publisher connected")
	if rtpFile != "" {
		replayRTP(ctx, rtpFile, map[string]*webrtc.TrackLocalStaticRTP{"l": rtpTrack, "m": trackM}, withRED, byte(vp8PT))
		_ = sess.Close()
		return
	}
	// The players start only once the session is connected: before that the
	// tracks are bound to nothing and the process never joins the room.
	if sampleM != nil {
		go func() {
			if d, _ := strconv.Atoi(os.Getenv("VP8_M_DELAY")); d > 0 {
				time.Sleep(time.Duration(d) * time.Second)
			}
			playLayer(simIVF, func(fr []byte) { _ = sampleM.WriteSample(media.Sample{Data: fr, Duration: time.Second / 15}) })
		}()
	}
	if sampleL != nil && os.Getenv("VP8_KF") == "1" {
		sent := 0
		playLayer(os.Args[2], func(fr []byte) { _ = sampleL.WriteSample(media.Sample{Data: fr, Duration: time.Second / 15}); sent++ })
		fmt.Println("frames sent:", sent)
		_ = sess.Close()
		return
	}
	frames := 0
	for ctx.Err() == nil {
		f, err := os.Open(os.Args[2])
		if err != nil {
			panic(err)
		}
		r, _, err := ivfreader.NewWith(f)
		if err != nil {
			panic(err)
		}
		tick := time.NewTicker(time.Second / 15)
		for ctx.Err() == nil {
			frame, _, err := r.ParseNextFrame()
			if err == io.EOF {
				break
			}
			if err != nil {
				panic(err)
			}
			<-tick.C
			switch {
			case withTID:
				writeTID(frame)
			case sampleL != nil:
				if err := sampleL.WriteSample(media.Sample{Data: frame, Duration: time.Second / 15}); err != nil {
					fmt.Println("write:", err)
				}
			default:
				if err := track.WriteSample(media.Sample{Data: frame, Duration: time.Second / 15}); err != nil {
					fmt.Println("write:", err)
				}
			}
			frames++
		}
		tick.Stop()
		_ = f.Close()
	}
	fmt.Println("frames sent:", frames)
	_ = sess.Close()
}

// loadIVF reads every frame of an IVF file.
func loadIVF(path string) [][]byte {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	r, _, err := ivfreader.NewWith(f)
	if err != nil {
		panic(err)
	}
	var out [][]byte
	for {
		fr, _, err := r.ParseNextFrame()
		if err != nil {
			return out
		}
		out = append(out, append([]byte(nil), fr...))
	}
}

type layerInfo struct {
	rid  string
	ssrc uint32
}

// senderSSRCs lists the session's simulcast encodings, rid and SSRC.
func senderSSRCs(sess any) []layerInfo {
	if v, ok := sess.(interface {
		Encodings() []webrtc.RTPEncodingParameters
	}); ok {
		var out []layerInfo
		for _, e := range v.Encodings() {
			out = append(out, layerInfo{rid: e.RID, ssrc: uint32(e.SSRC)})
		}
		return out
	}
	return nil
}

// replayRTP plays a rtpdump recording in a loop at the recorded pace (the
// RTP timestamps, 90 kHz), each layer on its own track. Sequence numbers
// and timestamps continue across loops so a receiver sees one stream.
func replayRTP(ctx context.Context, path string, tracks map[string]*webrtc.TrackLocalStaticRTP, withRED bool, vp8PT byte) {
	data, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	type rec struct {
		rid string
		pkt rtp.Packet
	}
	// A recording made against a RED offer already carries RED: its RED
	// payload type is named in RED_PT_IN (Pion's local negotiation picked 119).
	redPT := byte(104)
	if v, _ := strconv.Atoi(os.Getenv("RED_PT_IN")); v > 0 {
		redPT = byte(v)
	}
	var recs []rec
	seen := map[string]map[uint16]bool{}
	dups := 0
	for off := 0; off+6 <= len(data); {
		rl := int(data[off])
		rid := string(data[off+1 : off+1+rl])
		ln := int(data[off+4])<<8 | int(data[off+5])
		var p rtp.Packet
		if err := p.Unmarshal(data[off+6 : off+6+ln]); err != nil {
			panic(err)
		}
		off += 6 + ln
		// The recorder kept retransmissions (RTX answers to its own NACKs)
		// as plain packets; a replay must send each sequence number once.
		if seen[rid] == nil {
			seen[rid] = map[uint16]bool{}
		}
		if seen[rid][p.SequenceNumber] {
			dups++
			continue
		}
		seen[rid][p.SequenceNumber] = true
		recs = append(recs, rec{rid: rid, pkt: p})
	}
	fmt.Println("replay: packets", len(recs), "duplicates dropped", dups)
	type layer struct {
		seq     uint16
		tsBase  uint32 // first recorded ts of this loop
		tsOut   uint32 // output ts at the loop's start
		lastOut uint32
	}
	layers := map[string]*layer{"l": {seq: 1}, "m": {seq: 1}}
	firstTS := map[string]uint32{}
	for _, r := range recs {
		if _, ok := firstTS[r.rid]; !ok {
			firstTS[r.rid] = r.pkt.Timestamp
		}
	}
	start := time.Now()
	loop := 0
	for ctx.Err() == nil {
		loopStart := time.Now()
		for _, r := range recs {
			if ctx.Err() != nil {
				return
			}
			t, ok := tracks[r.rid]
			if !ok || t == nil {
				continue
			}
			// Pace each layer by its own recorded timestamps.
			due := time.Duration(int64(int32(r.pkt.Timestamp-firstTS[r.rid]))) * time.Second / 90000
			if wait := time.Until(loopStart.Add(due)); wait > 0 {
				time.Sleep(wait)
			}
			l := layers[r.rid]
			out := r.pkt
			out.SequenceNumber = l.seq
			l.seq++
			out.Timestamp = l.tsOut + (r.pkt.Timestamp - firstTS[r.rid])
			l.lastOut = out.Timestamp
			// The recording's header extensions are dropped: the engine's
			// stamper adds mid/rid/abs-send-time/twcc for this session.
			out.Extension = false
			out.Extensions = nil
			if withRED && r.pkt.PayloadType != redPT {
				// RFC 2198 RED with one primary block: F=0, block PT = VP8's.
				out.Payload = append([]byte{vp8PT}, out.Payload...)
			}
			if err := t.WriteRTP(&out); err != nil {
				fmt.Println("replay write:", err)
			}
		}
		loop++
		for _, l := range layers {
			l.tsOut = l.lastOut + 90000/15
		}
		fmt.Printf("replay: loop %d done after %s\n", loop, time.Since(start).Round(time.Second))
	}
}
