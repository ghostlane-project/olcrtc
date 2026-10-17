package goolom

// The abs-send-time header extension (RFC 5450's, as WebRTC uses it),
// stamped on every outgoing RTP packet the way a browser's stack does.
// The platform offers exactly this one extension in its SDP; an engine that
// declines it sends bare RTP headers, a byte-level difference visible in
// every media packet on the wire (olcrtc#58).

import (
	"strings"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
)

const absSendTimeURI = "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time"

type absSendTimeInterceptor struct {
	interceptor.NoOp
}

// NewInterceptor satisfies the registry's factory interface.
func (a *absSendTimeInterceptor) NewInterceptor(string) (interceptor.Interceptor, error) {
	return &absSendTimeInterceptor{}, nil
}

// BindLocalStream wraps the writer of every outgoing stream: each packet is
// stamped before it goes out.
func (a *absSendTimeInterceptor) BindLocalStream(info *interceptor.StreamInfo,
	writer interceptor.RTPWriter,
) interceptor.RTPWriter {
	id := 0
	for _, ext := range info.RTPHeaderExtensions {
		if strings.EqualFold(ext.URI, absSendTimeURI) {
			id = ext.ID
			break
		}
	}
	if id == 0 {
		return writer
	}
	return interceptor.RTPWriterFunc(func(header *rtp.Header, payload []byte,
		attributes interceptor.Attributes) (int, error) {
		_ = header.SetExtension(uint8(id), absSendTimeNow()) //nolint:gosec // an SDP extension id, one byte by definition
		return writer.Write(header, payload, attributes)
	})
}

// absSendTimeNow is the extension's 3-byte value: NTP time mod 64 s as
// a 6.18 fixed-point, the formula every WebRTC stack shares.
func absSendTimeNow() []byte {
	ms := time.Now().UnixNano() / int64(time.Millisecond)
	v := uint32(float64(ms&0xFFFF) / 65536.0 * float64(1<<24))
	return []byte{byte(v >> 16), byte(v >> 8), byte(v)} //nolint:gosec // the extension's own 24-bit field
}
