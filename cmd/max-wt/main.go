// Command max-wt speaks the ok-calls signaling over WebTransport: the same
// JSON commands as the WebSocket flavor ride a single bidirectional stream,
// framed as [varint length][deflate-raw payload], with the session
// identified by the peerId query parameter.
package main

import (
	"bytes"
	"compress/flate"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/quic-go/webtransport-go"
)

func main() {
	peerID := flag.String("peer-id", "", "peer id from the join answer")
	wtBase := flag.String("wt", "https://videowebrtc.okcdn.ru:23456/wt", "wt_endpoint")
	conversationID := flag.String("conv", "", "conversation uuid")
	token := flag.String("token", "", "signaling token from the join answer")
	hold := flag.Duration("hold", 30*time.Second, "read window")
	sendSDP := flag.String("sdp-file", "", "optional file with an SDP offer to transmit")
	flag.Parse()
	if *peerID == "" {
		fatal("-peer-id required")
	}

	ua := "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15"
	q := url.Values{}
	q.Set("platform", "web")
	q.Set("appVersion", "26.10.3")
	q.Set("version", "3")
	q.Set("device", "web")
	q.Set("capabilities", "webrtc")
	q.Set("clientType", "SDK_JS")
	q.Set("compression", "deflate-raw")
	q.Set("ua", ua)
	q.Set("peerId", *peerID)
	if *conversationID != "" {
		q.Set("conversationId", *conversationID)
	}
	if *token != "" {
		q.Set("token", *token)
	}
	target := *wtBase + "?" + q.Encode()
	fmt.Printf("dialing %s\n", target)

	dialer := &webtransport.Transport{}
	resp, sess, err := dialer.Dial(context.Background(), target, nil)
	if err != nil {
		fatal("dial: %v (resp=%v)", err, resp)
	}
	fmt.Println("WT session established")

	stream, err := sess.OpenStreamSync(context.Background())
	if err != nil {
		fatal("stream: %v", err)
	}
	defer stream.Close()

	send := func(payload string) error {
		deflated, err := deflateRaw([]byte(payload))
		if err != nil {
			return err
		}
		var head []byte
		n := len(deflated)
		switch {
		case n < 1<<6:
			head = []byte{byte(n)}
		case n < 1<<14:
			head = []byte{byte(n>>8) | 0x40, byte(n)}
		case n < 1<<30:
			head = []byte{byte(n>>24) | 0x80, byte(n >> 16), byte(n >> 8), byte(n)}
		default:
			head = append([]byte{0xc0}, make([]byte, 7)...)
			binary.BigEndian.PutUint64(head[1:], uint64(n))
		}
		_, err = stream.Write(append(head, deflated...))
		return err
	}

	seq := 0
	seq++
	if err := send(fmt.Sprintf(`{"command":"accept-call","sequence":%d}`, seq)); err != nil {
		fatal("accept-call: %v", err)
	}
	fmt.Println("sent accept-call")
	if *sendSDP != "" {
		sdp, err := os.ReadFile(*sendSDP)
		if err == nil {
			seq++
			msg := fmt.Sprintf(`{"command":"transmit-data","sequence":%d,"data":{"sdp":%q,"type":"offer"}}`, seq, string(sdp))
			if err := send(msg); err != nil {
				fatal("transmit-data: %v", err)
			}
			fmt.Printf("sent transmit-data (%d byte SDP)\n", len(sdp))
		}
	}

	// read loop: [varint len][deflate-raw payload]*
	buf := make([]byte, 0, 8192)
	tmp := make([]byte, 4096)
	deadline := time.Now().Add(*hold)
	_ = stream.SetReadDeadline(deadline)
	for time.Now().Before(deadline) {
		n, err := stream.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
			for {
				payload, rest, ok := takeFrame(buf)
				if !ok {
					break
				}
				buf = rest
				plain, err := inflateRaw(payload)
				if err != nil {
					fmt.Printf("frame %dB (inflate err: %v)\n", len(payload), err)
					continue
				}
				s := string(plain)
				if len(s) > 400 {
					s = s[:400] + "..."
				}
				fmt.Printf("< %s\n", s)
				if strings.TrimSpace(string(plain)) == "ping" {
					_ = send("pong")
					fmt.Println("> pong")
				}
			}
		}
		if err != nil {
			if os.IsTimeout(err) {
				break
			}
			fmt.Printf("read end: %v\n", err)
			break
		}
	}
	fmt.Println("done")
}

// takeFrame splits one length-prefixed frame off the buffer head.
func takeFrame(buf []byte) ([]byte, []byte, bool) {
	if len(buf) == 0 {
		return nil, nil, false
	}
	marker := buf[0] & 0xc0
	var size, hdr int
	switch marker {
	case 0x00:
		size, hdr = int(buf[0]&0x3f), 1
	case 0x40:
		if len(buf) < 2 {
			return nil, nil, false
		}
		size, hdr = int(buf[0]&0x3f)<<8|int(buf[1]), 2
	case 0x80:
		if len(buf) < 4 {
			return nil, nil, false
		}
		size, hdr = int(buf[0]&0x3f)<<24|int(buf[1])<<16|int(buf[2])<<8|int(buf[3]), 4
	case 0xc0:
		if len(buf) < 8 {
			return nil, nil, false
		}
		v := binary.BigEndian.Uint64(buf[1:9]) & ((1 << 62) - 1)
		size, hdr = int(v), 8
	}
	if len(buf) < hdr+size {
		return nil, nil, false
	}
	return buf[hdr : hdr+size], buf[hdr+size:], true
}

func deflateRaw(b []byte) ([]byte, error) {
	var out bytes.Buffer
	w, _ := flate.NewWriter(&out, flate.DefaultCompression)
	if _, err := w.Write(b); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func inflateRaw(b []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(b))
	defer r.Close()
	return io.ReadAll(r)
}

func fatal(f string, args ...any) {
	fmt.Fprintf(os.Stderr, "FATAL: "+f+"\n", args...)
	os.Exit(1)
}
