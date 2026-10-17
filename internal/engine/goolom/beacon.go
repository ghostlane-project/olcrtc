package goolom

// The beacon (olcrtc#58). The Telemost relay throttles a room in which no
// genuinely decodable video is present, whatever else the room carries: an
// engine pair alone is cut to a trickle, the same pair with a browser camera
// in the room flows, and the flow dies the moment the camera does - the
// classifier is a room-level switch keyed on real video, not a verdict on any
// one track (measured 2026-10-17; the same reading explains every earlier
// cut: a guest, an OAuth account and a web session all published pseudo-video
// and none of them real video).
//
// So a server session can publish, beside its data track, one real VP8
// stream: a 160x90 solid-black keyframe, encoded once with libvpx and
// repeated once a second. A keyframe is independently decodable, and the same
// keyframe every second is a valid stream - a slideshow of one still - that
// costs about 500 bit/s. To the relay the room then holds real video, which
// is what a meeting with a camera looks like; the data track rides beside it
// as before. Nothing about the data track itself changes.

import (
	"bytes"
	crand "crypto/rand"
	"embed"
	"encoding/binary"
	"math/rand"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
)

// beaconClip is the beacon's payload: ten seconds of a slowly moving
// gradient under light sensor noise, 64x36, encoded once with libvpx - a
// frame every thirtieth of a second, a keyframe each second, 300 distinct
// frames looping. A still keyframe repeated did not open the relay's room
// gate and the platform cut the frozen stream's subscriber in ~15 s
// (measured 2026-10-17); what held the gate open in the same room was a
// browser camera - near-static, but never the same frame twice. The clip is
// the honest copy of that: ~390 kbit/s, every frame its own bytes, the
// ten-second loop longer than the window anything was cut in.
//
//go:embed beacon_frames.bin
var beaconClip embed.FS

// beaconFrames unpacks the clip: [len u32][frame]... A decode of it that
// comes out wrong is a build error, not a runtime one.
func beaconFrames() [][]byte {
	raw, err := beaconClip.ReadFile("beacon_frames.bin")
	if err != nil || len(raw) < 4 {
		panic("goolom: beacon clip unreadable")
	}
	var frames [][]byte
	for len(raw) >= 4 {
		n := binary.BigEndian.Uint32(raw)
		raw = raw[4:]
		if uint64(n) > uint64(len(raw)) {
			panic("goolom: beacon clip truncated")
		}
		frames = append(frames, raw[:n:n])
		raw = raw[n:]
	}
	if len(frames) == 0 {
		panic("goolom: beacon clip empty")
	}
	return frames
}

// boundBeacon is the beacon's sample track that says when the SFU subscribes
// to it: Bind is pion's word for a receiver taking the track, and whether the
// platform forwards a participant's second track at all is the question the
// live tuning turns on (#58).
type boundBeacon struct {
	*webrtc.TrackLocalStaticSample
}

func (b *boundBeacon) Bind(t webrtc.TrackLocalContext) (webrtc.RTPCodecParameters, error) {
	logger.Infof("goolom: beacon: the SFU subscribed to it")
	return b.TrackLocalStaticSample.Bind(t) //nolint:wrapcheck // pion binding forwarded
}

func (b *boundBeacon) Unbind(t webrtc.TrackLocalContext) error {
	logger.Infof("goolom: beacon: the SFU dropped its subscription")
	return b.TrackLocalStaticSample.Unbind(t) //nolint:wrapcheck // pion unbind forwarded
}

const (
	// beaconAudioEvery is the silent audio track's cadence: one 20 ms Opus
	// frame of comfort silence, the shape a muted microphone sends. A call
	// platform reads a meeting as voices; the keeper speaks none, and the
	// live matrix says video alone does not open the data tracks (#58).
	beaconAudioEvery = 20 * time.Millisecond
	// beaconEvery is the beacon's cadence
	// beaconEvery is the beacon's cadence: thirty keyframes a second, the
	// cadence of a camera. A still a second kept the relay's room gate shut
	// (measured: one packet a second is nothing the classifier reads as
	// video); thirty - thirteen kilobits of the same keyframe - is what a
	// quiet camera looks like on the wire.
	beaconEvery = time.Second / 30
	// beaconTrackID and beaconStreamID name the beacon to the SFU. A stream
	// of its own keeps the beacon apart from the data track in whatever the
	// relay keys its rooms on.
	beaconTrackID  = "olcrtc-beacon"
	beaconStreamID = "olcrtc-beacon"
)

// beaconSilence is one Opus frame of silence, the three bytes a muted mic
// encodes: DTX at its quietest.
var beaconSilence = []byte{0xF8, 0xFF, 0xFE} //nolint:gochecknoglobals // three constant bytes

// mustSeed is the jitter generator's seed: any entropy does, this is not a
// secret, but gosec wants the strong source named.
func mustSeed() []byte {
	b := make([]byte, 8)
	_, _ = crand.Read(b)
	return b
}

// startBeacon publishes the beacon track and writes the frame on its cadence
// until the session ends. Called before the publisher peer connection exists:
// the track is stored with the session's video tracks and attached when that
// connection is negotiated, like any other track added ahead of it. Writes
// before a binding are pion's to drop: the samples that matter are the ones
// after the SFU subscribes.
func (s *Session) startBeacon() {
	base, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8}, beaconTrackID, beaconStreamID,
	)
	if err != nil {
		logger.Debugf("goolom: beacon: track: %v", err)
		return
	}
	track := &boundBeacon{TrackLocalStaticSample: base}
	if err := s.AddVideoTrack(track); err != nil {
		logger.Debugf("goolom: beacon: add track: %v", err)
		return
	}
	frames := beaconFrames()
	if audio, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus}, "olcrtc-beacon-audio", "olcrtc-beacon",
	); err == nil {
		if err := s.AddVideoTrack(audio); err != nil {
			logger.Debugf("goolom: beacon: add audio track: %v", err)
		} else {
			s.beaconWg.Add(1)
			go func() {
				defer s.beaconWg.Done()
				ticker := time.NewTicker(beaconAudioEvery)
				defer ticker.Stop()
				for {
					select {
					case <-s.closeCh:
						return
					case <-ticker.C:
						_ = audio.WriteSample(media.Sample{Data: beaconSilence, Duration: beaconAudioEvery})
					}
				}
			}()
		}
	}
	s.beaconWg.Add(1)
	go func() {
		defer s.beaconWg.Done()
		// A camera's cadence, not a ticker's: the interval jitters by a
		// quarter of itself either way and a frame is dropped now and
		// then. Perfectly even 33.333 ms is a machine signature no
		// capture pipeline produces, and the honest differences between
		// this beacon and the browser camera that held the data tracks
		// open are nearly used up (#58).
		rng := rand.New(rand.NewSource(int64(binary.BigEndian.Uint64(mustSeed())))) //nolint:gosec // jitter, not secrets
		next := time.Now()
		for i := 0; ; i = (i + 1) % len(frames) {
			wait := beaconEvery + time.Duration(rng.Int63n(int64(beaconEvery/2))-int64(beaconEvery/4))
			if rng.Intn(97) == 0 {
				wait += beaconEvery
			}
			timer := time.NewTimer(time.Until(next.Add(wait)))
			next = next.Add(wait)
			select {
			case <-s.closeCh:
				timer.Stop()
				return
			case <-s.beaconKeyframe:
				timer.Stop()
				// The SFU asked (PLI/FIR): a keyframe at once, the way a
				// browser answers, and the loop restarts from it.
				i = 0
				next = time.Now()
				_ = track.WriteSample(media.Sample{Data: bytes.Clone(frames[0]), Duration: beaconEvery})
			case <-timer.C:
				// A lost write (no binding yet) is no loss: the loop goes
				// on and the next frame is its own.
				frame := frames[i]
				_ = track.WriteSample(media.Sample{Data: bytes.Clone(frame), Duration: wait})
			}
		}
	}()
	logger.Infof("goolom: beacon on: %d frames of camera-like cadence, the room's data track rides beside it", len(frames))
}
