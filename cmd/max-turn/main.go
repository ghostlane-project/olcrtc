// Command max-turn measures the VK media relay pipe: two TURN allocations
// on the SFU-provided servers exchange UDP as fast as the link allows,
// which is the exact path P2P-room media rides.
package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/pion/turn/v5/internal/stun"
	"github.com/pion/turn/v5"
)

func main() {
	user := os.Getenv("TURN_USER")
	pass := os.Getenv("TURN_PASS")
	servers := strings.Split(os.Getenv("TURN_URLS"), ",")
	if user == "" || len(servers) < 1 || servers[0] == "" {
		// fall back to a fresh join
		fmt.Println("FATAL: TURN_USER/TURN_PASS/TURN_URLS required")
		os.Exit(1)
	}
	if len(servers) == 1 {
		servers = append(servers, servers[0])
	}

	alloc := func(server string) (*turn.Client, net.PacketConn, net.Addr) {
		conn, err := net.ListenPacket("udp4", "0.0.0.0:0")
		if err != nil {
			panic(err)
		}
		cfg := &turn.ClientConfig{
			STUNServerAddr: server,
			TURNServerAddr: server,
			Username:       user,
			Password:       pass,
			Conn:           conn,
			ListeningPort:  0,
			LoggerFactory:  nil,
		}
		c, err := turn.NewClient(cfg)
		if err != nil {
			panic(err)
		}
		if err := c.Listen(); err != nil {
			panic(err)
		}
		relay, err := c.Allocate()
		if err != nil {
			panic(fmt.Sprintf("allocate on %s: %v", server, err))
		}
		return c, relay, conn.LocalAddr()
	}

	fmt.Println("allocating on", servers[0], "and", servers[1])
	ca, relayA, _ := alloc(servers[0])
	defer ca.Close()
	cb, relayB, _ := alloc(servers[1])
	defer cb.Close()

	// permissions: each side allows the other's relayed address
	if err := ca.CreatePermission(relayB.LocalAddr()); err != nil {
		panic("perm A: " + err.Error())
	}
	if err := cb.CreatePermission(relayA.LocalAddr()); err != nil {
		panic("perm B: " + err.Error())
	}
	fmt.Printf("relayA=%v relayB=%v\n", relayA.LocalAddr(), relayB.LocalAddr())

	// B echoes everything it receives
	go func() {
		buf := make([]byte, 2048)
		for {
			n, from, err := relayB.ReadFrom(buf)
			if err != nil {
				return
			}
			_, _ = relayB.WriteTo(buf[:n], from)
		}
	}()

	// A pumps, counts echoes
	payload := make([]byte, 1200)
	for i := range payload {
		payload[i] = byte(i)
	}
	buf := make([]byte, 2048)
	sent, echoed := 0, 0
	deadline := time.Now().Add(20 * time.Second)
	warmup := time.Now().Add(3 * time.Second)
	var t0 time.Time
	for time.Now().Before(deadline) {
		if _, err := relayA.WriteTo(payload, relayB.LocalAddr()); err == nil {
			sent++
		}
		_ = relayA.SetReadDeadline(time.Now().Add(2 * time.Millisecond))
		for {
			n, _, err := relayA.ReadFrom(buf)
			if err != nil {
				break
			}
			if n > 0 && !t0.IsZero() {
				echoed += n
			} else if n > 0 {
				// first echo: start the clock
			}
		}
		if warmup.IsZero() {
			t0 = time.Now()
		}
		if time.Now().Before(warmup) && t0.IsZero() {
			t0 = time.Now()
		}
	}
	d := time.Since(t0)
	fmt.Printf("sent=%d pkts (%.1f Mbit/s offered), echoed=%d bytes (%.1f kbit/s RTT-loop)\n",
		sent, float64(sent*1200*8)/d.Seconds()/1e6, echoed, float64(echoed*8)/d.Seconds()/1000)
	if echoed > 500_000 {
		fmt.Println("TURN PIPE: PASS")
	} else if echoed > 50_000 {
		fmt.Println("TURN PIPE: THROTTLED?")
	} else {
		fmt.Println("TURN PIPE: FAIL")
	}
}
