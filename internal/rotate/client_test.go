package rotate

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
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

func TestNewClientDoesNotFollowARedirect(t *testing.T) {
	t.Parallel()

	var hitNext atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			http.Redirect(w, r, "/next", http.StatusFound)
		case "/next":
			hitNext.Store(true)
		}
	}))
	t.Cleanup(server.Close)

	client, err := NewClient("", false)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	resp, err := client.Get(server.URL + "/")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusFound {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusFound)
	}
	if hitNext.Load() {
		t.Error("the client followed the redirect to /next")
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
