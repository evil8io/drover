package filter

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
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
