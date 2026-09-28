package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/openlibrecommunity/olcrtc/pkg/olcrtc/client"
)

func main() {
	cfg := client.Config{
		Transport: "vp8channel", Provider: "vkcalls",
		RoomURL: os.Getenv("TUN_ROOM"), ChannelID: os.Getenv("TUN_CHANNEL"), KeyHex: os.Getenv("TUN_KEY"),
		LocalAddr: "127.0.0.1:1080", DNSServer: "8.8.8.8:53", DeviceID: "tun-cli",
		TransportOptions: client.VP8Options{FPS: 60, BatchSize: 64},
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Println("cnc: starting")
	if err := client.New(cfg).RunWithAddress(ctx, func(addr string) { fmt.Println("cnc: socks ready on", addr); os.Stdout.Sync() }); err != nil {
		fmt.Println("cnc: ended:", err)
		os.Exit(1)
	}
}
