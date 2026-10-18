package main

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"crypto/x509"
	"fmt"

	"os"

	"github.com/quic-go/quic-go"
)

func main() {
	_ = os.Stdout
	tlsCfg := &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"h3"}}
	conn, err := quic.DialAddr(context.Background(), "155.212.205.229:23456", tlsCfg, nil)
	if err != nil {
		fmt.Println("dial:", err)
		return
	}
	defer conn.CloseWithError(0, "")
	state := conn.ConnectionState().TLS
	for i, cert := range state.PeerCertificates {
		fmt.Printf("cert[%d] CN=%q issuer=%q DNS=%v\n", i, cert.Subject.CommonName, cert.Issuer.CommonName, cert.DNSNames[:min(4, len(cert.DNSNames))])
		pem := "-----BEGIN CERTIFICATE-----\n" + base64.StdEncoding.EncodeToString(cert.Raw) + "\n-----END CERTIFICATE-----\n"
		os.WriteFile(fmt.Sprintf("/tmp/okcdn-cert-%d.pem", i), []byte(pem), 0o644)
	}
	_, verifyErr := certVerify(state.PeerCertificates)
	fmt.Println("chain verify:", verifyErr)
}

func certVerify(certs []*x509.Certificate) (bool, error) {
	if len(certs) == 0 {
		return false, fmt.Errorf("no certs")
	}
	pool := x509.NewCertPool()
	for _, c := range certs[1:] {
		pool.AddCert(c)
	}
	_, err := certs[0].Verify(x509.VerifyOptions{Roots: pool, Intermediates: pool})
	return err == nil, err
}

func min(a, b int) int { if a < b { return a }; return b }
