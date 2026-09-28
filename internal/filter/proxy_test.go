package filter

import (
	"encoding/json"
	"encoding/pem"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPassThrough(t *testing.T) {
	t.Parallel()
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Upstream", "yes")
		w.WriteHeader(http.StatusTeapot)
		_, _ = io.WriteString(w, "pods")
	})

	req := h.request(t, http.MethodGet, "/k8s/clusters/c-1/api/v1/pods?limit=1", nil, http.Header{
		"Authorization":     []string{callerToken},
		"X-Custom":          []string{"value"},
		"X-Forwarded-Proto": []string{"https"},
	})
	req.Host = "rancher.example.com"
	resp, body := h.do(t, req)

	if resp.StatusCode != http.StatusTeapot {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusTeapot)
	}
	if got := resp.Header.Get("X-Upstream"); got != "yes" {
		t.Errorf("X-Upstream = %q, want %q", got, "yes")
	}
	if string(body) != "pods" {
		t.Errorf("body = %q, want %q", body, "pods")
	}

	requests := h.upstream.all()
	if len(requests) != 1 {
		t.Fatalf("upstream requests = %d, want 1", len(requests))
	}
	got := requests[0]
	if got.method != http.MethodGet {
		t.Errorf("method = %q, want %q", got.method, http.MethodGet)
	}
	if got.path != "/k8s/clusters/c-1/api/v1/pods" {
		t.Errorf("path = %q, want %q", got.path, "/k8s/clusters/c-1/api/v1/pods")
	}
	if want := (url.Values{"limit": []string{"1"}}); got.query.Encode() != want.Encode() {
		t.Errorf("query = %v, want %v", got.query, want)
	}
	if v := got.header.Get("Authorization"); v != callerToken {
		t.Errorf("Authorization = %q, want %q", v, callerToken)
	}
	if v := got.header.Get("X-Custom"); v != "value" {
		t.Errorf("X-Custom = %q, want %q", v, "value")
	}
	if v := got.header.Get("X-Forwarded-Proto"); v != "https" {
		t.Errorf("X-Forwarded-Proto = %q, want %q", v, "https")
	}
	if v := got.header.Get("X-Forwarded-For"); !strings.HasSuffix(v, "127.0.0.1") {
		t.Errorf("X-Forwarded-For = %q, want a value that ends with the client IP", v)
	}
	if got.host != "rancher.example.com" {
		t.Errorf("host = %q, want %q", got.host, "rancher.example.com")
	}
}

func TestForwardedForAppendsClientIP(t *testing.T) {
	t.Parallel()
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := h.request(t, http.MethodGet, "/v3/settings", nil, http.Header{
		"X-Forwarded-For":  []string{"10.0.0.1"},
		"X-Forwarded-Host": []string{"rancher.example.com"},
	})
	resp, _ := h.do(t, req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	got := h.upstream.all()[0]
	if v := got.header.Get("X-Forwarded-For"); v != "10.0.0.1, 127.0.0.1" {
		t.Errorf("X-Forwarded-For = %q, want %q", v, "10.0.0.1, 127.0.0.1")
	}
	if v := got.header.Get("X-Forwarded-Host"); v != "rancher.example.com" {
		t.Errorf("X-Forwarded-Host = %q, want %q", v, "rancher.example.com")
	}
}

func TestHealthz(t *testing.T) {
	t.Parallel()
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("the upstream got the health request")
	})

	resp, body := h.do(t, h.request(t, http.MethodGet, "/healthz", nil, nil))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if string(body) != "ok" {
		t.Errorf("body = %q, want %q", body, "ok")
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type = %q, want text/plain", ct)
	}
	if h.upstream.count() != 0 {
		t.Errorf("upstream requests = %d, want 0", h.upstream.count())
	}
}

// deadlineRecorder is a ResponseRecorder that records the read deadlines that
// a handler sets through http.ResponseController.
type deadlineRecorder struct {
	*httptest.ResponseRecorder
	mu        sync.Mutex
	deadlines []time.Time
}

func (r *deadlineRecorder) SetReadDeadline(deadline time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deadlines = append(r.deadlines, deadline)
	return nil
}

// hasDeadline reports whether the handler set a read deadline that is not
// zero.
func (r *deadlineRecorder) hasDeadline() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.ContainsFunc(r.deadlines, func(deadline time.Time) bool { return !deadline.IsZero() })
}

// TestReadDeadlineOnTheReviewPathOnly checks that a POST gets the body read
// deadline on the review path only. A passed deadline cancels the request,
// and an exec with an empty body waits for its protocol switch longer than
// the deadline.
func TestReadDeadlineOnTheReviewPathOnly(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "review", path: reviewPath, want: true},
		{name: "exec", path: "/k8s/clusters/c-1/api/v1/namespaces/a/pods/p/exec", want: false},
		{name: "other", path: "/v3/tokens", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			})
			recorder := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
			req := h.request(t, http.MethodPost, test.path, strings.NewReader(""), http.Header{"Authorization": []string{callerToken}})
			h.svc.ServeHTTP(recorder, req)
			if got := recorder.hasDeadline(); got != test.want {
				t.Errorf("read deadline = %v, want %v", got, test.want)
			}
		})
	}
}

// TestUpstreamTLSErrorAnswersAFixedText checks that the 502 of a failed
// certificate check names no host and no name of the certificate, and that
// the log line has the full error.
func TestUpstreamTLSErrorAnswersAFixedText(t *testing.T) {
	t.Parallel()
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("the upstream got a request")
	}))
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	t.Cleanup(server.Close)

	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	certificate := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	if err := os.WriteFile(caFile, certificate, 0o600); err != nil {
		t.Fatalf("write the CA file: %v", err)
	}
	tokenFile := filepath.Join(dir, "token")
	writeToken(t, tokenFile, "service")
	target, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse the upstream URL: %v", err)
	}
	// The certificate of the test server names 127.0.0.1 and example.com,
	// so localhost fails the check.
	target.Host = "localhost:" + target.Port()

	logs := &syncBuffer{}
	svc, err := New(Config{
		Upstream:  target,
		CAFile:    caFile,
		TokenFile: tokenFile,
		Logger:    slog.New(slog.NewTextHandler(logs, nil)),
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	proxy := httptest.NewServer(svc)
	t.Cleanup(proxy.Close)

	for _, path := range []string{"/v3/settings", listPath} {
		req, err := http.NewRequest(http.MethodGet, proxy.URL+path, nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("Authorization", callerToken)
		resp, err := proxy.Client().Do(req)
		if err != nil {
			t.Fatalf("do request: %v", err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatalf("read the body: %v", err)
		}
		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("status of %s = %d, want 502", path, resp.StatusCode)
		}
		var status statusBody
		if err := json.Unmarshal(body, &status); err != nil {
			t.Fatalf("parse the body %q: %v", body, err)
		}
		if want := serviceName + ": the upstream request failed"; status.Message != want {
			t.Errorf("message of %s = %q, want %q", path, status.Message, want)
		}
	}
	if !strings.Contains(logs.String(), "x509") {
		t.Errorf("logs = %q, want the certificate error", logs.String())
	}
}
