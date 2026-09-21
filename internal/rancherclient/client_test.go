package rancherclient

import (
	"crypto/tls"
	"testing"
)

func TestTransportInsecureSkipVerify(t *testing.T) {
	t.Parallel()

	tr, err := Transport("", true)
	if err != nil {
		t.Fatalf("Transport: %v", err)
	}
	if !tr.TLSClientConfig.InsecureSkipVerify {
		t.Error("InsecureSkipVerify = false, want true")
	}
	if tr.TLSClientConfig.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %x, want TLS 1.2", tr.TLSClientConfig.MinVersion)
	}
}

func TestTransportVerifies(t *testing.T) {
	t.Parallel()

	tr, err := Transport("", false)
	if err != nil {
		t.Fatalf("Transport: %v", err)
	}
	if tr.TLSClientConfig.InsecureSkipVerify {
		t.Error("InsecureSkipVerify = true, want false")
	}
	if tr.TLSClientConfig.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %x, want TLS 1.2", tr.TLSClientConfig.MinVersion)
	}
}
