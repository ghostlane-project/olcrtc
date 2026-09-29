package vkcalls

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/pion/interceptor"

	"github.com/pion/webrtc/v4"

	"github.com/openlibrecommunity/olcrtc/internal/engine"
	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/internal/protect"
)

// The VK SFU only enables media for guests whose answer covers the offered
// codec set; Pion's default MediaEngine answers a subset. The offer's codecs
// are therefore echoed into the MediaEngine so the shaped answer negotiates
// the full offered vocabulary (spike: codecs-mode negotiation).

var (
	rtpmapLine  = regexp.MustCompile(`^a=rtpmap:(\d+) ([A-Za-z0-9-]+)/(\d+)(?:/(\d+))?`)
	fmtpLine    = regexp.MustCompile(`^a=fmtp:(\d+) (.*)$`)
	feedbackLin = regexp.MustCompile(`^a=rtcp-fb:(\d+) (\S+)(?: (\S+))?`)
	ssrcLine    = regexp.MustCompile(`(?m)^a=ssrc:(\d+)\s`)
)

// offerCodec parses one offered codec block: the rtpmap name and rate, then
// a bare-number channel count (audio) and fmtp:/fb: extras in any order.
func offerCodec(kind string, fields []string, payload uint8) (webrtc.RTPCodecParameters, bool) {
	spec := webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: kind + "/" + fields[0]},
		PayloadType:        webrtc.PayloadType(payload),
	}
	rate, err := strconv.ParseUint(fields[1], 10, 32)
	if err != nil {
		return spec, false
	}
	spec.ClockRate = uint32(rate)
	for _, extra := range fields[2:] {
		switch {
		case strings.HasPrefix(extra, "fmtp:"):
			spec.SDPFmtpLine = strings.TrimPrefix(extra, "fmtp:")
		case strings.HasPrefix(extra, "fb:"):
			parts := strings.Fields(strings.TrimPrefix(extra, "fb:"))
			fb := webrtc.RTCPFeedback{Type: parts[0]}
			if len(parts) > 1 {
				fb.Parameter = parts[1]
			}
			spec.RTCPFeedback = append(spec.RTCPFeedback, fb)
		default:
			channels, err := strconv.ParseUint(extra, 10, 16)
			if err != nil {
				return spec, false
			}
			spec.Channels = uint16(channels)
		}
	}
	return spec, true
}

// offerMediaEngine builds a MediaEngine mirroring the offer's codecs and the
// header extensions the SFU tags its forwarded packets with.
func offerMediaEngine(offer string) (*webrtc.MediaEngine, error) {
	sections, err := splitSections(offer)
	if err != nil {
		return nil, err
	}
	media := &webrtc.MediaEngine{}
	for _, sec := range sections {
		if err := registerSectionCodecs(media, sec); err != nil {
			return nil, err
		}
	}
	for _, uri := range []string{
		"urn:ietf:params:rtp-hdrext:ssrc-audio-level",
		"urn:ietf:params:rtp-hdrext:sdes:mid",
		"urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id",
		"urn:ietf:params:rtp-hdrext:toffset",
		"http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time",
		"http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01",
		"http://www.webrtc.org/experiments/rtp-hdrext/color-space",
		"urn:3gpp:video-orientation",
		"http://www.webrtc.org/experiments/rtp-hdrext/playout-delay",
	} {
		_ = media.RegisterHeaderExtension(webrtc.RTPHeaderExtensionCapability{URI: uri}, webrtc.RTPCodecTypeVideo)
		_ = media.RegisterHeaderExtension(webrtc.RTPHeaderExtensionCapability{URI: uri}, webrtc.RTPCodecTypeAudio)
	}
	return media, nil
}

// registerSectionCodecs registers one m= section's offered codecs in payload
// order, keeping each payload's rtpmap, fmtp and rtcp-fb lines together.
func registerSectionCodecs(media *webrtc.MediaEngine, sec section) error {
	head := strings.Fields(sec[0])
	if len(head) < 4 {
		return nil
	}
	kind := strings.TrimPrefix(head[0], "m=")
	if kind != kindAudio && kind != kindVideo {
		return nil
	}
	codecType := webrtc.RTPCodecTypeVideo
	if kind == kindAudio {
		codecType = webrtc.RTPCodecTypeAudio
	}
	blocks := sectionCodecBlocks(sec)
	for _, payloadStr := range head[3:] {
		payload, err := strconv.ParseUint(payloadStr, 10, 8)
		if err != nil {
			continue
		}
		fields, ok := blocks[uint8(payload)]
		if !ok {
			continue
		}
		spec, ok := offerCodec(kind, fields, uint8(payload))
		if !ok {
			continue
		}
		if err := media.RegisterCodec(spec, codecType); err != nil {
			return fmt.Errorf("vkcalls: register %s pt=%d: %w", spec.MimeType, payload, err)
		}
	}
	return nil
}

// sectionCodecBlocks maps payload types to their codec fields: the rtpmap
// name/rate/channels followed by this payload's fmtp and rtcp-fb lines.
func sectionCodecBlocks(sec section) map[uint8][]string {
	blocks := map[uint8]*[]string{}
	pick := func(line string) (uint8, string, bool) {
		if m := fmtpLine.FindStringSubmatch(line); m != nil {
			pt, err := strconv.ParseUint(m[1], 10, 8)
			return uint8(pt), "fmtp:" + m[2], err == nil
		}
		if m := feedbackLin.FindStringSubmatch(line); m != nil {
			pt, err := strconv.ParseUint(m[1], 10, 8)
			if err != nil {
				return 0, "", false
			}
			extra := "fb:" + m[2]
			if m[3] != "" {
				extra += " " + m[3]
			}
			return uint8(pt), extra, true
		}
		return 0, "", false
	}
	for _, line := range sec {
		if m := rtpmapLine.FindStringSubmatch(line); m != nil {
			pt, parseErr := strconv.ParseUint(m[1], 10, 8)
			if parseErr != nil {
				continue
			}
			fields := []string{m[2], m[3]}
			if m[4] != "" {
				fields = append(fields, m[4])
			}
			blocks[uint8(pt)] = &fields
			continue
		}
		if pt, extra, ok := pick(line); ok {
			if fields, found := blocks[pt]; found {
				*fields = append(*fields, extra)
			}
		}
	}
	out := map[uint8][]string{}
	for pt, fields := range blocks {
		out[pt] = *fields
	}
	return out
}

// newWebRTCAPI builds the Pion API for the bundled peer connection: the
// offer's codecs, DTLS client role (the SDK answers active) and the host's
// protected networking.
func newWebRTCAPI(offer string, cfg engine.Config, resolver protect.Lookup) (*webrtc.API, error) {
	media, err := offerMediaEngine(offer)
	if err != nil {
		return nil, err
	}
	settings := webrtc.SettingEngine{}
	if roleErr := settings.SetAnsweringDTLSRole(webrtc.DTLSRoleClient); roleErr != nil {
		return nil, fmt.Errorf("vkcalls: dtls role: %w", roleErr)
	}
	apply, err := engine.NewPionSettings(engine.PionSettingsOptions{
		Resolver:         resolver,
		DTLSProfile:      cfg.DTLSProfile,
		LoggerFactory:    logger.NewPionLoggerFactory(),
		IPv4Only:         true,
		ProxyDialer:      true,
		DisableMulticast: true,
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // shared builder already adds protected-net context
	}
	if apply != nil {
		apply(&settings)
	}
	// The engine answers as a browser-class endpoint: default interceptors
	// (RTCP receiver/sender reports, NACK, TWCC) keep the SFU's consumer-leg
	// liveness fed — without them the SFU stalls the forward after a minute —
	// and every outbound packet carries the mid and rid header extensions the
	// SFU maps our single simulcast layer by.
	registry := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptorsWithOptions(
		media, registry, engine.DefaultInterceptorOptions()...,
	); err != nil {
		return nil, fmt.Errorf("vkcalls: interceptors: %w", err)
	}
	if err := webrtc.ConfigureTWCCHeaderExtensionSender(media, registry); err != nil {
		return nil, fmt.Errorf("vkcalls: twcc: %w", err)
	}
	registry.Add(sdesStamperFactory{mid: publishMid(offer), rid: "l"})
	opts := []func(*webrtc.API){
		webrtc.WithSettingEngine(settings),
		webrtc.WithMediaEngine(media),
		webrtc.WithInterceptorRegistry(registry),
	}
	return webrtc.NewAPI(opts...), nil
}

// remoteSSRCs lists the distinct SSRC attributions of an offer — the streams
// the accept-producer command reports knowing about.
func remoteSSRCs(offer string) []string {
	seen := map[string]bool{}
	var out []string
	for _, match := range ssrcLine.FindAllStringSubmatch(offer, -1) {
		if !seen[match[1]] {
			seen[match[1]] = true
			out = append(out, match[1])
		}
	}
	return out
}
