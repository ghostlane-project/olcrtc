// Command beacon-keeper joins a Telemost room and does one thing: publishes
// the beacon's real-video clip, nothing else (olcrtc#58). The relay forwards
// a room's tracks only while the room holds genuinely decodable video, and a
// beacon published beside a tunnel's data track by the same participant is
// detected and cut in ~15 s; a participant that publishes ONLY real video is
// what a browser with a camera is, which held the room open in the
// measurements. The keeper is that participant, for a room whose real
// participants are tunnels.
//
// Usage: beacon-keeper <room-url> — runs until killed; the engine reconnects
// like any session.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/openlibrecommunity/olcrtc/internal/auth"
	authTelemost "github.com/openlibrecommunity/olcrtc/internal/auth/telemost"
	"github.com/openlibrecommunity/olcrtc/internal/engine"
	engineGoolom "github.com/openlibrecommunity/olcrtc/internal/engine/goolom"
	"github.com/openlibrecommunity/olcrtc/internal/logger"
)

func main() {
	if len(os.Args) != 2 {
		logger.Errorf("usage: beacon-keeper <room-url>")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	run := func() int {

		creds, err := authTelemost.Provider{}.Issue(ctx, auth.Config{RoomURL: os.Args[1], DNSServer: "8.8.8.8:53"})
		if err != nil {
			logger.Errorf("keeper: auth: %v", err)
			return 1
		}

		sess, err := engineGoolom.New(ctx, engine.Config{
			URL:    creds.URL,
			Token:  creds.Token,
			Extra:  creds.Extra,
			Name:   "keeper",
			Beacon: true,
		})
		if err != nil {
			logger.Errorf("keeper: engine: %v", err)
			return 1
		}
		if err := sess.Connect(ctx); err != nil {
			logger.Errorf("keeper: connect: %v", err)
			return 1
		}
		logger.Infof("keeper: in the room, beacon on")

		<-ctx.Done()
		_ = sess.Close()
		return 0
	}
	os.Exit(run())
}
