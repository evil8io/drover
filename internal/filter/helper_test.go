package filter

import (
	"bytes"
	"context"
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

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// testMetrics returns a *metrics whose instruments record into a
// MeterProvider that drops every measurement, for a test that does not
// assert on the filter metrics.
func testMetrics(t *testing.T) *metrics {
	t.Helper()
	m, err := newMetrics(noop.NewMeterProvider())
	if err != nil {
		t.Fatalf("new metrics: %v", err)
	}
	return m
}

const (
	listPath              = "/k8s/clusters/c-1/api/v1/namespaces"
	stevePath             = "/k8s/clusters/c-1/v1/namespaces"
	reviewPath            = "/k8s/clusters/c-1/apis/authorization.k8s.io/v1/selfsubjectaccessreviews"
	selfSubjectReviewPath = "/k8s/clusters/c-1/apis/authentication.k8s.io/v1/selfsubjectreviews"
	callerToken           = "Bearer caller"
	serviceAuth           = "Bearer service"

	// callerUsername is the identity that selfSubjectReviewHandler answers by
	// default, for a test that does not care about a specific user id.
	callerUsername = "u-caller"
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

// privileged returns the last recorded request. fetchAllowed ends the fill
// with the request it sends on the service token, so a test that indexes
// this request stays correct when a step joins the fill before it.
func (u *upstream) privileged(t *testing.T) recorded {
	t.Helper()
	requests := u.all()
	if len(requests) == 0 {
		t.Fatal("no upstream request")
	}
	return requests[len(requests)-1]
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
	svc       *Service
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

// newHarnessOpt is newHarness with Config overrides, for a test that needs a
// custom cache bound or fetch rate.
func newHarnessOpt(t *testing.T, handler http.HandlerFunc, opts ...func(*Config)) *harness {
	t.Helper()
	tokenFile := filepath.Join(t.TempDir(), "token")
	writeToken(t, tokenFile, "service")
	return newHarnessWithTokenFile(t, handler, tokenFile, opts...)
}

// newHarnessWithSpans is newHarnessOpt with an in-memory span exporter
// installed as the TracerProvider, for a test that reads the span of a request.
func newHarnessWithSpans(t *testing.T, handler http.HandlerFunc) (*harness, *tracetest.InMemoryExporter) {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	h := newHarnessOpt(t, handler, func(cfg *Config) { cfg.TracerProvider = provider })
	return h, exporter
}

// requestSpan returns the exported server span of the request. Each upstream
// call has its own client span, through the same TracerProvider. The server
// kind therefore identifies the request span of otelhttp.NewHandler.
func requestSpan(t *testing.T, exporter *tracetest.InMemoryExporter) tracetest.SpanStub {
	t.Helper()
	for _, span := range exporter.GetSpans() {
		if span.SpanKind == trace.SpanKindServer {
			return span
		}
	}
	t.Fatal("no server span")
	return tracetest.SpanStub{}
}

// spanAttributeString returns the string value of the span attribute named
// key. It reports whether the span has that attribute.
func spanAttributeString(span tracetest.SpanStub, key string) (string, bool) {
	for _, kv := range span.Attributes {
		if string(kv.Key) == key {
			return kv.Value.AsString(), true
		}
	}
	return "", false
}

// metricAttributeKeys returns the attribute keys of every data point of m.
func metricAttributeKeys(m metricdata.Metrics) []string {
	var sets []attribute.Set
	switch data := m.Data.(type) {
	case metricdata.Sum[int64]:
		for _, dp := range data.DataPoints {
			sets = append(sets, dp.Attributes)
		}
	case metricdata.Histogram[float64]:
		for _, dp := range data.DataPoints {
			sets = append(sets, dp.Attributes)
		}
	}
	var keys []string
	for _, set := range sets {
		for _, kv := range set.ToSlice() {
			keys = append(keys, string(kv.Key))
		}
	}
	return keys
}

// newHarnessWithoutToken builds a harness whose token file does not exist yet.
func newHarnessWithoutToken(t *testing.T, handler http.HandlerFunc) *harness {
	t.Helper()
	tokenFile := filepath.Join(t.TempDir(), "token")
	return newHarnessWithTokenFile(t, handler, tokenFile)
}

func newHarnessWithTokenFile(t *testing.T, handler http.HandlerFunc, tokenFile string, opts ...func(*Config)) *harness {
	t.Helper()
	up := newUpstream(t, handler)

	target, err := url.Parse(up.server.URL)
	if err != nil {
		t.Fatalf("parse upstream URL: %v", err)
	}
	clock := newClock()
	logs := &syncBuffer{}
	cfg := Config{
		Upstream:  target,
		TokenFile: tokenFile,
		Logger:    slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Now:       clock.Now,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	svc, err := New(cfg)
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	proxy := httptest.NewServer(svc)
	t.Cleanup(proxy.Close)
	return &harness{upstream: up, proxy: proxy, svc: svc, clock: clock, tokenFile: tokenFile, logs: logs}
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

// doDirect calls Service.ServeHTTP directly, with no network hop. A real
// connection flushes the response before the wrapping span ends. A test
// that reads the exported spans needs this call instead of do.
func (h *harness) doDirect(t *testing.T, req *http.Request) (*http.Response, []byte) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.svc.ServeHTTP(rec, req)
	resp := rec.Result()
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

// listUpstream routes the five requests of the list flow. The native attempt
// gets 403. The caller has no project, because most tests do not need one.
func listUpstream(steve, privileged http.HandlerFunc) http.HandlerFunc {
	return listUpstreamWithProjects(steve, projectsHandler(), privileged)
}

// listUpstreamWithProjects is listUpstream with the given project ids of the caller.
func listUpstreamWithProjects(steve, projects, privileged http.HandlerFunc) http.HandlerFunc {
	return listUpstreamFull(steve, projects, selfSubjectReviewHandler(callerUsername), privileged)
}

// listUpstreamFull is listUpstreamWithProjects with the given handler for the
// caller identity lookup. A test that needs a specific username, or a
// specific failure of that lookup, uses this instead.
func listUpstreamFull(steve, projects, identity, privileged http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == stevePath:
			steve(w, r)
		case r.URL.Path == projectsPath:
			projects(w, r)
		case r.URL.Path == selfSubjectReviewPath:
			identity(w, r)
		case r.Header.Get("Authorization") == serviceAuth:
			privileged(w, r)
		default:
			http.Error(w, "forbidden", http.StatusForbidden)
		}
	}
}

// projectsHandler answers the project ids of the caller, in the test cluster c-1.
func projectsHandler(ids ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		items := make([]string, 0, len(ids))
		for _, id := range ids {
			items = append(items, fmt.Sprintf(`{"id":"c-1:%s"}`, id))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, fmt.Sprintf(`{"type":"collection","data":[%s]}`, strings.Join(items, ",")))
	}
}

// selfSubjectReviewHandler answers a SelfSubjectReview with the given
// username. An empty username omits the username field from the answer.
func selfSubjectReviewHandler(username string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		userInfo := "{}"
		if username != "" {
			userInfo = fmt.Sprintf(`{"username":%q}`, username)
		}
		_, _ = io.WriteString(w, fmt.Sprintf(
			`{"apiVersion":"authentication.k8s.io/v1","kind":"SelfSubjectReview","status":{"userInfo":%s}}`, userInfo))
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

// steveNamespace is one namespace item of steveLabeledHandler, with its
// project label. An empty project omits the label.
type steveNamespace struct {
	name    string
	project string
}

// steveLabeledHandler answers a Steve namespace collection whose items have
// the given project labels.
func steveLabeledHandler(namespaces ...steveNamespace) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		items := make([]string, 0, len(namespaces))
		for _, ns := range namespaces {
			if ns.project == "" {
				items = append(items, fmt.Sprintf(`{"id":%q,"type":"namespace","metadata":{"name":%q}}`, ns.name, ns.name))
				continue
			}
			items = append(items, fmt.Sprintf(`{"id":%q,"type":"namespace","metadata":{"name":%q,"labels":{%q:%q}}}`,
				ns.name, ns.name, projectLabel, ns.project))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, fmt.Sprintf(`{"type":"collection","resourceType":"namespace","count":%d,"continue":"","data":[%s]}`,
			len(namespaces), strings.Join(items, ",")))
	}
}

// namespaceSet is the Steve namespace collection that an upstream handler
// answers. A test changes it while a watch runs.
type namespaceSet struct {
	mu         sync.Mutex
	namespaces []steveNamespace
}

func newNamespaceSet(namespaces ...steveNamespace) *namespaceSet {
	return &namespaceSet{namespaces: namespaces}
}

func (n *namespaceSet) set(namespaces ...steveNamespace) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.namespaces = namespaces
}

func (n *namespaceSet) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		n.mu.Lock()
		namespaces := slices.Clone(n.namespaces)
		n.mu.Unlock()
		steveLabeledHandler(namespaces...)(w, r)
	}
}

func namespaceListHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Source", "privileged")
	_, _ = io.WriteString(w, `{"kind":"NamespaceList","items":[]}`)
}
