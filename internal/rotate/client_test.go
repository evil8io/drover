package rotate

import (
	"crypto/tls"
	"net/http"
	"testing"
)

func TestNewClientInsecureSkipVerify(t *testing.T) {
	t.Parallel()

	client, err := NewClient("", true)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	tr := client.Transport.(*http.Transport)
	if !tr.TLSClientConfig.InsecureSkipVerify {
		t.Error("InsecureSkipVerify = false, want true")
	}
	if tr.TLSClientConfig.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %x, want TLS 1.2", tr.TLSClientConfig.MinVersion)
	}
}

func TestNewClientVerifies(t *testing.T) {
	t.Parallel()

	client, err := NewClient("", false)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	tr := client.Transport.(*http.Transport)
	if tr.TLSClientConfig.InsecureSkipVerify {
		t.Error("InsecureSkipVerify = true, want false")
	}
	if tr.TLSClientConfig.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %x, want TLS 1.2", tr.TLSClientConfig.MinVersion)
	}
}
