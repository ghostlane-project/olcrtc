// Command max-wslogin speaks the oneme messaging WebSocket directly: it
// replays the browser's handshake and login frames, trying the token both
// as the {key,value} object the page serializes and as a flat string, and
// collects the fresh token the login reply carries.
package main

import (
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	variant := flag.String("variant", "flat", "flat: token as string, wrapped: {key,value} object")
	tokenFile := flag.String("token-file", "", "file with a token value to swap in (same length required)")
	flag.Parse()

	raw, err := os.ReadFile("/tmp/max-uniqframes.hex")
	if err != nil {
		fatal("read frames: %v", err)
	}
	var frames [][]byte
	for _, l := range strings.Fields(string(raw)) {
		b, err := hex.DecodeString(l)
		if err != nil {
			fatal("hex: %v", err)
		}
		frames = append(frames, b)
	}
	var hello, login []byte
	for _, f := range frames {
		op := int(f[4])<<8 | int(f[5])
		if op == 6 && hello == nil {
			hello = f
		}
		if op == 19 && login == nil {
			login = f
		}
	}
	if hello == nil || login == nil {
		fatal("missing frames")
	}

	if *tokenFile != "" {
		tv, err := os.ReadFile(*tokenFile)
		if err != nil {
			fatal("token file: %v", err)
		}
		tv = []byte(strings.TrimSpace(string(tv)))
		login, err = swapToken(login, string(tv))
		if err != nil {
			fatal("swap: %v", err)
		}
		fmt.Printf("swapped token value (len %d)\n", len(tv))
	}

	switch *variant {
	case "wrapped":
		bad := []byte{0x0b, 0x00, 0xf1, 0xff, 0xff, 0xdd}
		good := append([]byte{0xa5}, []byte("token")...)
		idx := bytesIndex(login, bad)
		if idx < 0 {
			fatal("undefined marker not found")
		}
		copy(login[idx:], good)
		fmt.Printf("variant wrapped: token.key -> \"token\"\n")
	case "flat":
		login, err = flattenToken(login)
		if err != nil {
			fatal("flatten: %v", err)
		}
		fmt.Println("variant flat: token as plain string")
	}

	header := http.Header{}
	header.Set("Origin", "https://web.max.ru")
	header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15")
	conn, _, err := websocket.DefaultDialer.Dial("wss://api.oneme.ru/websocket", header)
	if err != nil {
		fatal("dial: %v", err)
	}
	defer conn.Close()
	sendChunked := func(msg []byte, baseSeq uint16, name string) {
		first := append([]byte{}, msg...)
		first[2], first[3] = byte(baseSeq>>8), byte(baseSeq)
		second := append([]byte{}, msg...)
		second[2], second[3] = byte((baseSeq+1)>>8), byte(baseSeq+1)
		for _, f := range [][]byte{first, second} {
			if err := conn.WriteMessage(websocket.BinaryMessage, f); err != nil {
				fatal("%s: %v", name, err)
			}
		}
		fmt.Printf("sent %s x2 chunks (%d bytes each)\n", name, len(msg))
	}
	sendChunked(hello, 0, "hello")
	sendChunked(login, 1, "login")

	deadline := time.Now().Add(20 * time.Second)
	var bestToken string
	for time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, data, err := conn.ReadMessage()
		if err != nil {
			if strings.Contains(err.Error(), "timeout") {
				continue
			}
			fmt.Printf("read ended: %v\n", err)
			break
		}
		cmd, op := 0, 0
		if len(data) >= 6 {
			cmd = int(data[1])
			op = int(data[4])<<8 | int(data[5])
		}
		fmt.Printf("recv cmd=%d opcode=%d %dB: %s\n", cmd, op, len(data), summarize(data))
		if t := findToken(data); t != "" && len(t) > len(bestToken) && strings.HasPrefix(t, "An_") {
			bestToken = t
			fmt.Printf("*** TOKEN (len %d): %s...%s\n", len(t), t[:14], t[len(t)-6:])
		}
	}
	if bestToken != "" {
		_ = os.WriteFile("/root/max-calls-token.txt", []byte(bestToken), 0o600)
		fmt.Printf("SAVED token to /root/max-calls-token.txt\n")
	} else {
		fmt.Println("no token in replies")
	}
}

// swapToken replaces the da02c7-prefixed token string inside the login frame.
func swapToken(frame []byte, newToken string) ([]byte, error) {
	marker := append([]byte{0xda}, 0x02, 0xc7)
	idx := bytesIndex(frame, marker)
	if idx < 0 {
		return nil, fmt.Errorf("token marker not found")
	}
	oldLen := 711
	if len(newToken) != oldLen {
		return nil, fmt.Errorf("token length %d != %d (same-length swap only)", len(newToken), oldLen)
	}
	out := append([]byte{}, frame...)
	copy(out[idx+3:], newToken)
	return out, nil
}

// flattenToken rewrites payload {token:{key,value}} into {token:"..."}.
func flattenToken(frame []byte) ([]byte, error) {
	// payload starts at 10; find `82 a3"key" <6 undefined bytes> a5"value"` right after a5"token"
	needle := append([]byte{0x82, 0xa3}, []byte("key")...)
	needle = append(needle, 0x0b, 0x00, 0xf1, 0xff, 0xff, 0xdd)
	needle = append(needle, 0xa5)
	needle = append(needle, []byte("value")...)
	idx := bytesIndex(frame, needle)
	if idx < 0 {
		return nil, fmt.Errorf("wrapped-token shape not found")
	}
	// remove needle (17 bytes); the da02c7 token string follows directly
	out := append([]byte{}, frame[:idx]...)
	out = append(out, frame[idx+len(needle):]...)
	newLen := uint32(len(out) - 10)
	binary.BigEndian.PutUint32(out[6:10], newLen<<8) // 1 chunk byte + 3 len bytes
	return out, nil
}

func summarize(data []byte) string {
	var b strings.Builder
	plain := 0
	for i := 0; i < len(data); i++ {
		c := data[i]
		if c >= 0x20 && c < 0x7f {
			b.WriteByte(c)
			plain++
		} else if plain >= 4 {
			b.WriteString(" | ")
			plain = 0
		}
	}
	out := b.String()
	if len(out) > 200 {
		out = out[:200] + "..."
	}
	return out
}

func findToken(data []byte) string {
	best := ""
	start, length := -1, 0
	flush := func(end int) {
		if length >= 40 {
			s := string(data[start:end])
			if len(s) > len(best) {
				best = s
			}
		}
		length = 0
	}
	for i := 0; i < len(data); i++ {
		c := data[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '.' || c == '=' || c == '$' {
			if length == 0 {
				start = i
			}
			length++
		} else {
			flush(i)
		}
	}
	flush(len(data))
	return best
}

func bytesIndex(hay, needle []byte) int {
	for i := 0; i+len(needle) <= len(hay); i++ {
		match := true
		for j := range needle {
			if hay[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

func fatal(f string, args ...any) {
	fmt.Fprintf(os.Stderr, "FATAL: "+f+"\n", args...)
	os.Exit(1)
}
