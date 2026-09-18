package filter

import (
	"bytes"
	"fmt"
	"io"
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

const (
	listPath    = "/k8s/clusters/c-1/api/v1/namespaces"
	stevePath   = "/k8s/clusters/c-1/v1/namespaces"
	reviewPath  = "/k8s/clusters/c-1/apis/authorization.k8s.io/v1/selfsubjectaccessreviews"
	callerToken = "Bearer caller"
	serviceAuth = "Bearer service"
)

type recorded struct {
	method string
	path   string
	query  url.Values
	header http.Header
	body   []byte
	host   string
}

type upstream struct {
	server   *httptest.Server
	mu       sync.Mutex
	requests []recorded
}

func newUpstream(t *testing.T, handler http.HandlerFunc) *upstream {
	t.Helper()
	up := &upstream{}
	up.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		up.mu.Lock()
		up.requests = append(up.requests, recorded{
			method: r.Method,
			path:   r.URL.Path,
			query:  r.URL.Query(),
			header: r.Header.Clone(),
			body:   body,
			host:   r.Host,
		})
		up.mu.Unlock()
		r.Body = io.NopCloser(bytes.NewReader(body))
		handler(w, r)
	}))
	t.Cleanup(up.server.Close)
	return up
}

func (u *upstream) all() []recorded {
	u.mu.Lock()
	defer u.mu.Unlock()
	return slices.Clone(u.requests)
}

func (u *upstream) count() int {
	return len(u.all())
}

func (u *upstream) countPath(path string) int {
	count := 0
	for _, request := range u.all() {
		if request.path == path {
			count++
		}
	}
	return count
}

type fakeClock struct {
	mu sync.Mutex
	at time.Time
}

func newClock() *fakeClock {
	return &fakeClock{at: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(d)
}

type harness struct {
	upstream  *upstream
	proxy     *httptest.Server
	clock     *fakeClock
	tokenFile string
	logs      *syncBuffer
}

// syncBuffer collects log output from concurrent requests.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func newHarness(t *testing.T, handler http.HandlerFunc) *harness {
	t.Helper()
	tokenFile := filepath.Join(t.TempDir(), "token")
	writeToken(t, tokenFile, "service")
	return newHarnessWithTokenFile(t, handler, tokenFile)
}

// newHarnessWithoutToken builds a harness whose token file does not exist yet.
func newHarnessWithoutToken(t *testing.T, handler http.HandlerFunc) *harness {
	t.Helper()
	tokenFile := filepath.Join(t.TempDir(), "token")
	return newHarnessWithTokenFile(t, handler, tokenFile)
}

func newHarnessWithTokenFile(t *testing.T, handler http.HandlerFunc, tokenFile string) *harness {
	t.Helper()
	up := newUpstream(t, handler)

	target, err := url.Parse(up.server.URL)
	if err != nil {
		t.Fatalf("parse upstream URL: %v", err)
	}
	clock := newClock()
	logs := &syncBuffer{}
	handler2, err := New(Config{
		Upstream:  target,
		TokenFile: tokenFile,
		Logger:    slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Now:       clock.Now,
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	proxy := httptest.NewServer(handler2)
	t.Cleanup(proxy.Close)
	return &harness{upstream: up, proxy: proxy, clock: clock, tokenFile: tokenFile, logs: logs}
}

func writeToken(t *testing.T, path, token string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(token+"\n"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
}

func (h *harness) request(t *testing.T, method, target string, body io.Reader, header http.Header) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, h.proxy.URL+target, body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	for name, values := range header {
		for _, value := range values {
			req.Header.Add(name, value)
		}
	}
	return req
}

func (h *harness) do(t *testing.T, req *http.Request) (*http.Response, []byte) {
	t.Helper()
	resp, err := h.proxy.Client().Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return resp, body
}

// callerHeader returns the headers of a project member.
func callerHeader() http.Header {
	return http.Header{"Authorization": []string{callerToken}}
}

// listUpstream routes the three requests of the list flow. The native attempt
// gets 403.
func listUpstream(steve, privileged http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == stevePath:
			steve(w, r)
		case r.Header.Get("Authorization") == serviceAuth:
			privileged(w, r)
		default:
			http.Error(w, "forbidden", http.StatusForbidden)
		}
	}
}

func steveCollectionJSON(next string, names ...string) string {
	items := make([]string, 0, len(names))
	for _, name := range names {
		items = append(items, fmt.Sprintf(`{"id":%q,"type":"namespace","metadata":{"name":%q}}`, name, name))
	}
	return fmt.Sprintf(`{"type":"collection","resourceType":"namespace","count":%d,"continue":%q,"data":[%s]}`,
		len(names), next, strings.Join(items, ","))
}

func steveHandler(names ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, steveCollectionJSON("", names...))
	}
}

func namespaceListHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Source", "privileged")
	_, _ = io.WriteString(w, `{"kind":"NamespaceList","items":[]}`)
}
