package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/xtaci/smux"

	"github.com/openlibrecommunity/olcrtc/internal/logger"
	"github.com/openlibrecommunity/olcrtc/internal/runtime"
	"github.com/openlibrecommunity/olcrtc/internal/tunnelcore"
)

// connectJob is one CONNECT on its way to an outbound: the target, what the
// SOCKS client already sent while its first bytes were being sniffed, and
// whether the SOCKS reply is already on the wire.
type connectJob struct {
	host    string
	port    int
	head    []byte
	replied bool
}

// fail answers a CONNECT that will not be served: the SOCKS reply when it is
// still owed, nothing when it was sent before the outcome was known.
func (j connectJob) fail(conn net.Conn, reply []byte) {
	if !j.replied {
		_, _ = conn.Write(reply)
	}
}

// open answers success when it is still owed and replays the bytes read
// while sniffing into the outbound, which must see them before anything
// else. It reports false when either write failed.
func (j connectJob) open(conn net.Conn, outbound io.Writer) bool {
	if !j.replied {
		if _, err := conn.Write(replySuccess(j.host)); err != nil {
			return false
		}
	}
	if len(j.head) > 0 {
		if _, err := outbound.Write(j.head); err != nil {
			return false
		}
	}
	return true
}

func (c *Client) tunnel(ctx context.Context, conn net.Conn, session *smux.Session, job connectJob) {
	stream, err := session.OpenStream()
	if err != nil {
		logger.Warnf("OpenStream failed: %v", err)
		job.fail(conn, replyHostUnreachable(job.host))
		return
	}
	defer func() { _ = stream.Close() }()
	logger.Infof("sid=%d tunnel to %s:%d", stream.ID(), job.host, job.port)
	if err := c.sendConnectRequest(stream, job.host, job.port); err != nil {
		logger.Warnf("sid=%d connect failed: %v", stream.ID(), err)
		job.fail(conn, replyForConnectError(err, job.host))
		return
	}
	if !job.open(conn, stream) {
		return
	}
	if _, err := tunnelcore.CopyBidirectional(ctx, conn, stream); errors.Is(err, tunnelcore.ErrHalfOpenIdle) {
		logger.Debugf("sid=%d closed: half-open and silent for %s", stream.ID(), tunnelcore.HalfOpenGrace)
	}
}

func (c *Client) sendConnectRequest(stream *smux.Stream, targetAddr string, targetPort int) error {
	request, err := json.Marshal(map[string]any{
		"cmd": "connect", "addr": targetAddr, "port": targetPort,
	})
	if err != nil {
		return fmt.Errorf("sid=%d marshal connect req: %w", stream.ID(), err)
	}
	_ = stream.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if _, err := stream.Write(request); err != nil {
		return fmt.Errorf("sid=%d write connect req: %w", stream.ID(), err)
	}
	_ = stream.SetWriteDeadline(time.Time{})
	ack := make([]byte, 1)
	_ = stream.SetReadDeadline(time.Now().Add(runtime.ConnectAckTimeout()))
	if _, err := io.ReadFull(stream, ack); err != nil {
		return fmt.Errorf("sid=%d: %w (read_err=%w)", stream.ID(), ErrRemoteNotReady, err)
	}
	_ = stream.SetReadDeadline(time.Time{})
	if ack[0] != tunnelcore.ConnectAckOK {
		return &connectAckError{code: ack[0], streamID: stream.ID()}
	}
	return nil
}

type connectAckError struct {
	code     byte
	streamID uint32
}

func (e *connectAckError) Error() string {
	return fmt.Sprintf("sid=%d: %s (connect ack=0x%02x)", e.streamID, ErrRemoteNotReady, e.code)
}

func (e *connectAckError) Unwrap() error { return ErrRemoteNotReady }

func replyForConnectError(err error, target string) []byte {
	var ackErr *connectAckError
	if errors.As(err, &ackErr) {
		return socks5Reply(ackErr.code, target)
	}
	return replyHostUnreachable(target)
}
