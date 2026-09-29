package vkcalls

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/sdp/v3"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
)

// sdesStamper writes the SDES mid and rid header extensions on every
// outgoing video packet. Pion negotiates both extensions but writes neither,
// and the SFU binds a published stream to its section and simulcast layer
// by them, as Chrome's packets carry them.
// The declared simulcast layer: the transport's small frame.
const (
	layerWidth  = 320
	layerHeight = 180
	layerFPS    = 15
	layerKbps   = 180
)

type sdesStamper struct {
	interceptor.NoOp
	mid, rid string
}

type sdesStamperFactory struct{ mid, rid string }

// NewInterceptor implements interceptor.Factory.
func (f sdesStamperFactory) NewInterceptor(string) (interceptor.Interceptor, error) {
	return &sdesStamper{mid: f.mid, rid: f.rid}, nil
}

// BindLocalStream stamps the stream's packets when it is video and the
// extensions were negotiated for it.
func (s *sdesStamper) BindLocalStream(info *interceptor.StreamInfo, w interceptor.RTPWriter) interceptor.RTPWriter {
	if !strings.HasPrefix(strings.ToLower(info.MimeType), "video/") {
		return w
	}
	var midID, ridID, astID uint8
	for _, ext := range info.RTPHeaderExtensions {
		switch ext.URI {
		case sdp.SDESMidURI:
			midID = uint8(ext.ID) //nolint:gosec // extension ids are 1..255
		case sdp.SDESRTPStreamIDURI:
			ridID = uint8(ext.ID) //nolint:gosec // extension ids are 1..255
		case sdp.ABSSendTimeURI:
			astID = uint8(ext.ID) //nolint:gosec // extension ids are 1..255
		}
	}
	if midID == 0 && ridID == 0 && astID == 0 {
		return w
	}
	logger.Debugf("vkcalls: stamping ssrc=%d mid=%s rid=%s", info.SSRC, s.mid, s.rid)
	return interceptor.RTPWriterFunc(func(header *rtp.Header, payload []byte, attrs interceptor.Attributes) (int, error) {
		if midID != 0 && s.mid != "" {
			_ = header.SetExtension(midID, []byte(s.mid))
		}
		if ridID != 0 && s.rid != "" {
			_ = header.SetExtension(ridID, []byte(s.rid))
		}
		return w.Write(header, payload, attrs)
	})
}

// publishMid is the mid of the offer's video section the SFU receives on.
func publishMid(offer string) string {
	sections, err := splitSections(strings.ReplaceAll(offer, "\r\n", "\n"))
	if err != nil {
		return ""
	}
	for _, sec := range sections {
		if sec.kind() == kindVideo && sec.direction() == "recvonly" {
			return sec.mid()
		}
	}
	return ""
}

// EXPERIMENT (temporary): rtcpLogger counts the RTCP the SFU sends, by
// type and media SSRC, and logs the tally every 5 s.
type rtcpLogger struct {
	interceptor.NoOp
	mu     sync.Mutex
	counts map[string]int
	last   time.Time
	writer interceptor.RTCPWriter
	ssrcs  []uint32
}

// BindRTCPWriter keeps the writer so RRT blocks can be answered (EXPERIMENT).
func (l *rtcpLogger) BindRTCPWriter(writer interceptor.RTCPWriter) interceptor.RTCPWriter {
	l.mu.Lock()
	l.writer = writer
	l.mu.Unlock()
	return writer
}

// answerRRT sends the DLRR block RFC 3611 pairs with a receiver reference
// time: the last RR time (middle 32 bits of the NTP we were sent) and the
// delay since, once per local sending SSRC, as browsers do.
func (l *rtcpLogger) answerRRT(from uint32, ntp uint64) {
	l.mu.Lock()
	w := l.writer
	ssrcs := append([]uint32(nil), l.ssrcs...)
	l.mu.Unlock()
	if w == nil || len(ssrcs) == 0 {
		return
	}
	lastRR := uint32(ntp >> 16)                                         //nolint:gosec // middle 32 bits by definition
	reports := []rtcp.DLRRReport{{SSRC: from, LastRR: lastRR, DLRR: 1}} // ~15 µs: answered at once
	pkts := make([]rtcp.Packet, 0, len(ssrcs))
	for _, ssrc := range ssrcs {
		xr := &rtcp.ExtendedReport{SenderSSRC: ssrc,
			Reports: []rtcp.ReportBlock{&rtcp.DLRRReportBlock{Reports: reports}}}
		pkts = append(pkts, xr)
	}
	if _, err := w.Write(pkts, interceptor.Attributes{}); err != nil {
		logger.Debugf("vkcalls: dlrr write: %v", err)
		return
	}
	logger.Debugf("vkcalls: dlrr answered for %d ssrcs (lastRR=%d)", len(ssrcs), lastRR)
}

type rtcpLoggerFactory struct{}

func (rtcpLoggerFactory) NewInterceptor(string) (interceptor.Interceptor, error) {
	return &rtcpLogger{counts: map[string]int{}, last: time.Now()}, nil
}

//nolint:gocognit,gocyclo,cyclop // diagnostic scaffolding
func (l *rtcpLogger) BindRTCPReader(reader interceptor.RTCPReader) interceptor.RTCPReader {
	return interceptor.RTCPReaderFunc(func(b []byte, a interceptor.Attributes) (int, interceptor.Attributes, error) {
		n, attrs, err := reader.Read(b, a)
		if err != nil {
			return n, attrs, err //nolint:wrapcheck
		}
		pkts, perr := rtcp.Unmarshal(b[:n])
		if perr == nil { //nolint:nestif // diagnostic scaffolding
			l.mu.Lock()
			for _, p := range pkts {
				l.noteKeyframeRequest(p)
				name := fmt.Sprintf("%T", p)
				for _, ssrc := range p.DestinationSSRC() {
					l.counts[fmt.Sprintf("%s->%d", strings.TrimPrefix(name, "*rtcp."), ssrc)]++
				}
				if remb, ok := p.(*rtcp.ReceiverEstimatedMaximumBitrate); ok {
					l.counts[fmt.Sprintf("REMB bitrate=%.0f", remb.Bitrate)]++
				}
				if rr, ok := p.(*rtcp.ReceiverReport); ok {
					for _, rep := range rr.Reports {
						logger.Debugf("vkcalls: sfu RR from=%d ssrc=%d fraction=%d lost=%d highest=%d jitter=%d lsr=%d dlsr=%d",
							rr.SSRC, rep.SSRC, rep.FractionLost, rep.TotalLost, rep.LastSequenceNumber, rep.Jitter,
							rep.LastSenderReport, rep.Delay)
					}
				}
				if xr, ok := p.(*rtcp.ExtendedReport); ok && expOn("dlrr") {
					for _, rep := range xr.Reports {
						if rrt, ok := rep.(*rtcp.ReceiverReferenceTimeReportBlock); ok {
							l.answerRRT(xr.SenderSSRC, rrt.NTPTimestamp)
						}
					}
				}
				if nack, ok := p.(*rtcp.TransportLayerNack); ok {
					logger.Debugf("vkcalls: sfu NACK ssrc=%d pairs=%d", nack.MediaSSRC, len(nack.Nacks))
				}
				if xr, ok := p.(*rtcp.ExtendedReport); ok {
					for _, rep := range xr.Reports {
						logger.Debugf("vkcalls: sfu XR from=%d block=%T %+v", xr.SenderSSRC, rep, rep)
					}
				}
				if raw, ok := p.(*rtcp.RawPacket); ok {
					logger.Debugf("vkcalls: sfu RAW rtcp %x", []byte(*raw))
				}
			}
			if time.Since(l.last) > 5*time.Second {
				logger.Debugf("vkcalls: rtcp from sfu: %v", l.counts)
				l.counts = map[string]int{}
				l.last = time.Now()
			}
			l.mu.Unlock()
		}
		return n, attrs, err //nolint:wrapcheck
	})
}

// EXPERIMENT (temporary): logs the local streams' SSRCs as bound.
func (l *rtcpLogger) BindLocalStream(info *interceptor.StreamInfo, writer interceptor.RTPWriter) interceptor.RTPWriter {
	logger.Debugf("vkcalls: local stream ssrc=%d mime=%s", info.SSRC, info.MimeType)
	l.mu.Lock()
	l.ssrcs = append(l.ssrcs, info.SSRC)
	l.mu.Unlock()
	return writer
}

// EXPERIMENT (temporary): encodeChangeSimulcast is the change-simulcast
// producer command: 07 00 seq mediaSource(1=camera) count, then per layer
// rid(str) width height fps bitrate.
func encodeChangeSimulcast(sequence int, rid string, width, height, fps, bitrate int) []byte {
	out := appendMPInt(nil, 7)
	out = appendMPInt(out, 0)
	out = appendMPInt(out, sequence)
	out = appendMPInt(out, 1)
	out = appendMPInt(out, 1)
	out = append(out, 0xA0|byte(len(rid))) //nolint:gosec // wire-format bit assembly, truncation is the point
	out = append(out, rid...)
	out = appendMPInt(out, width)
	out = appendMPInt(out, height)
	out = appendMPInt(out, fps)
	// The web client's serializeChangeSimulcast writes eT.enc(bitrate/1e3):
	// the layer bitrate travels in kbit/s, not bit/s.
	return appendMPInt(out, bitrate/1000)
}

// layerProfile is the simulcast layer the engine declares, in the change
// command and the answer's rid constraints: the small layer the transport
// publishes. The SFU forwards it whatever the transport's frame rate - the
// CI gate measured the full matrix with this declaration.
func layerProfile() (int, int, int, int) {
	return layerWidth, layerHeight, layerFPS, layerKbps
}

// encodePerfStatReport is the SDK's report-perf-stat producer command (type
// 1): [1, 0, sequence, framesDecoded, framesReceived]. The SDK sends it every
// statisticsInterval (5 s) from getStats; the SFU feeds its consumer-leg
// liveness with it — without the reports the forward is stalled within a
// minute (spike tun-rr-01).
func encodePerfStatReport(sequence int, framesDecoded, framesReceived uint32) []byte {
	out := appendMPInt(nil, 1)
	out = appendMPInt(out, 0)
	out = appendMPInt(out, sequence)
	out = appendMPInt(out, int(framesDecoded))
	return appendMPInt(out, int(framesReceived))
}

// KeyframeRequests (EXPERIMENT) delivers the SSRC of every FIR/PLI the SFU
// sends, so a publisher can answer with a keyframe as an encoder would.
var KeyframeRequests = make(chan uint32, 64) //nolint:gochecknoglobals // a diagnostic counter

func (l *rtcpLogger) noteKeyframeRequest(p rtcp.Packet) {
	var ssrc uint32
	switch v := p.(type) {
	case *rtcp.FullIntraRequest:
		if len(v.FIR) > 0 {
			ssrc = v.FIR[0].SSRC
		} else {
			ssrc = v.MediaSSRC
		}
	case *rtcp.PictureLossIndication:
		ssrc = v.MediaSSRC
	default:
		return
	}
	select {
	case KeyframeRequests <- ssrc:
	default:
	}
}
