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
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
)

const (
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

// beaconFrame is one 160x90 solid-black VP8 keyframe, 57 bytes:
//
//	50 05 00        the uncompressed header: a keyframe, shown
//	9d 01 2a        VP8's start code
//	a0 00 5a 00     160 by 90
//
// Encoded once with libvpx (ffmpeg -f lavfi -i color=black:s=160x90 -frames:v
// 1 -c:v libvpx) and embedded: no encoder ships with the engine, and a still
// frame never needs another one.
var beaconFrame = []byte{ //nolint:gochecknoglobals // one constant frame, embedded once
	0x50, 0x05, 0x00, 0x9d, 0x01, 0x2a, 0xa0, 0x00, 0x5a, 0x00, 0x00, 0x47, 0x08, 0x85, 0x85,
	0x88, 0x85, 0x84, 0x88, 0x02, 0x02, 0x00, 0x06, 0x28, 0x0f, 0x08, 0x75, 0x54, 0x9a, 0xee,
	0x21, 0xd5, 0x52, 0x6b, 0xb8, 0x87, 0x55, 0x49, 0xae, 0xe2, 0x1d, 0x55, 0x26, 0xbb, 0x88,
	0x75, 0x54, 0x9a, 0xee, 0x21, 0xbc, 0x00, 0xfe, 0xff, 0xab, 0x50, 0x80,
}

// startBeacon publishes the beacon track and writes the frame on its cadence
// until the session ends. Called before the publisher peer connection exists:
// the track is stored with the session's video tracks and attached when that
// connection is negotiated, like any other track added ahead of it. Writes
// before a binding are pion's to drop: the samples that matter are the ones
// after the SFU subscribes.
func (s *Session) startBeacon() {
	track, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8}, beaconTrackID, beaconStreamID,
	)
	if err != nil {
		logger.Debugf("goolom: beacon: track: %v", err)
		return
	}
	if err := s.AddVideoTrack(track); err != nil {
		logger.Debugf("goolom: beacon: add track: %v", err)
		return
	}
	s.beaconWg.Add(1)
	go func() {
		defer s.beaconWg.Done()
		ticker := time.NewTicker(beaconEvery)
		defer ticker.Stop()
		for {
			select {
			case <-s.closeCh:
				return
			case <-ticker.C:
				// A lost write (no binding yet) is no loss: the next
				// second brings the same frame.
				_ = track.WriteSample(media.Sample{Data: beaconFrame, Duration: beaconEvery})
			}
		}
	}()
	logger.Infof("goolom: beacon on: a real 160x90 keyframe 30 times a second, the room's data track rides beside it")
}
