// Command max-wt-serve stands up the vendored (draft-16) WebTransport
// server to bisect dialect compatibility against Chrome.
package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
)

func selfSigned() tls.Certificate {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, _ := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func main() {
	cert := selfSigned()
	_ = os.WriteFile("/tmp/max-wt-serve.der", cert.Certificate[0], 0o644)
	s := webtransport.Server{
		H3: &http3.Server{
			Addr:      "127.0.0.1:23456",
			TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}},
		},
	}
	s.H3.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("[serve] request: proto=%q upgrade=%q path=%q origin=%q\n", r.Proto, r.Header.Get("Upgrade"), r.URL.Path, r.Header.Get("Origin"))
		sess, err := s.Upgrade(w, r)
		if err != nil {
			fmt.Println("upgrade:", err)
			return
		}
		fmt.Println("SESSION ESTABLISHED")
		go func() {
			for {
				str, err := sess.AcceptStream(context.Background())
				if err != nil {
					return
				}
				go func() {
					buf := make([]byte, 512)
					for {
						n, err := str.Read(buf)
						if err != nil {
							return
						}
						fmt.Printf("recv %d bytes: %q\n", n, string(buf[:n]))
						_, _ = str.Write(buf[:n])
					}
				}()
			}
		}()
	})
	fmt.Println("draft-16 WT server on https://127.0.0.1:23456/wt")
	panic(s.H3.ListenAndServe())
}
