package main

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/valkey-io/valkey-go"
)

func parseCertPEM(pemBytes []byte) ([]*x509.Certificate, error) {
	var certs []*x509.Certificate
	rest := pemBytes
	for {
		block, rest2 := pem.Decode(rest)
		rest = rest2
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		crt, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, err
		}
		certs = append(certs, crt)
	}
	return certs, nil
}

func mustString(t *testing.T, c valkey.Client, cmd valkey.Completed) string {
	t.Helper()
	m, err := c.Do(context.Background(), cmd).ToMessage()
	if err != nil {
		t.Fatalf("command %v: %v", cmd.Commands(), err)
	}
	s, err := m.ToString()
	if err != nil {
		t.Fatalf("ToString on %v: %v", cmd.Commands(), err)
	}
	return s
}

func mustStringE(t *testing.T, c valkey.Client, cmd valkey.Completed) (string, error) {
	t.Helper()
	m, err := c.Do(context.Background(), cmd).ToMessage()
	if err != nil {
		return "", err
	}
	return m.ToString()
}
