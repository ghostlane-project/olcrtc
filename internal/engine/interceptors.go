package engine

import (
	"github.com/pion/interceptor/pkg/nack"
	"github.com/pion/webrtc/v4"

	"github.com/openlibrecommunity/olcrtc/internal/hostprofile"
)

const (
	// constrainedNackResponderSize is how many sent packets pion keeps a copy
	// of, per outgoing stream, to answer a NACK with. pion's default is 1024,
	// each held in a full-MTU buffer whether the packet filled it or not:
	// 2 MB at the upload peak of the phone profile, the single largest item
	// in that burst. The data on our tracks is KCP, which retransmits on its
	// own, so an answered NACK only ever saves KCP a round trip; 256 packets
	// is most of a second at the SFU's ceiling, more than any NACK arrives
	// behind. Must be one of the powers of two nack accepts.
	constrainedNackResponderSize = 256
)

// DefaultInterceptorOptions returns what an engine passes to
// [webrtc.RegisterDefaultInterceptorsWithOptions]: nothing on a server, and on
// the constrained profile a NACK responder that keeps a fraction of the
// default's packet copies. Same interceptor set either way.
func DefaultInterceptorOptions() []webrtc.InterceptorOption {
	if !hostprofile.BuffersAreConstrained() {
		return nil
	}
	return []webrtc.InterceptorOption{
		webrtc.WithNackResponderOptions(nack.ResponderSize(constrainedNackResponderSize)),
	}
}
