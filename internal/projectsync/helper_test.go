package projectsync

import (
	"bytes"
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
	serviceToken = "service"
	userAgent    = "drover/test"

	alphaProject = `{"id":"c-1:p-alpha","clusterId":"c-1","name":"Alpha",` +
		`"labels":{"cost-center":"cc-1","team":"team-a"},` +
		`"annotations":{"owner":"alpha@example.com","note":"outside the allow list"}}`

	betaProject = `{"id":"c-2:p-beta","clusterId":"c-2","name":"Beta",` +
		`"labels":{"cost-center":"cc-2"},"annotations":{}}`

	// alphaNamespaces has the four namespaces of cluster c-1. Only alpha-two
	// needs a patch.
	alphaNamespaces = `{"kind":"NamespaceList","items":[
{"metadata":{"name":"alpha-one","labels":{"field.cattle.io/projectId":"p-alpha","cost-center":"cc-1","tier":"gold"},"annotations":{"owner":"alpha@example.com"}}},
{"metadata":{"name":"alpha-two","labels":{"field.cattle.io/projectId":"p-alpha","cost-center":"old","tier":"gold"},"annotations":{}}},
{"metadata":{"name":"alpha-three","labels":{"other":"value"},"annotations":{}}},
{"metadata":{"name":"alpha-orphan","labels":{"field.cattle.io/projectId":"p-gone"},"annotations":{}}}
]}`

	betaNamespaces = `{"kind":"NamespaceList","items":[
{"metadata":{"name":"beta-one","labels":{"field.cattle.io/projectId":"p-beta"},"annotations":{}}}
]}`

	// nextPage is the pagination link of Rancher. Its host is the server URL of
	// Rancher, which the test server does not answer.
	nextPage = "https://rancher.example.com/v3/projects?marker=2"
)

type recorded struct {
	method string
	path   string
	query  url.Values
	header http.Header
	body   string
}

// fakeRancher answers the project list and the namespace list of two clusters.
// It records every request.
type fakeRancher struct {
	server *httptest.Server

	// paginate serves the project list in two pages.
	paginate bool
	// forbidden is the cluster whose namespace list returns status 403.
	forbidden string

	mu       sync.Mutex
	requests []recorded
}

func newFakeRancher(t *testing.T, options ...func(*fakeRancher)) *fakeRancher {
	t.Helper()
	rancher := &fakeRancher{}
	for _, option := range options {
		option(rancher)
	}
	rancher.server = httptest.NewServer(http.HandlerFunc(rancher.serve))
	t.Cleanup(rancher.server.Close)
	return rancher
}

func paginated() func(*fakeRancher) {
	return func(f *fakeRancher) { f.paginate = true }
}

func forbid(cluster string) func(*fakeRancher) {
	return func(f *fakeRancher) { f.forbidden = cluster }
}

func (f *fakeRancher) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.requests = append(f.requests, recorded{
		method: r.Method,
		path:   r.URL.Path,
		query:  r.URL.Query(),
		header: r.Header.Clone(),
		body:   string(body),
	})
	f.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.Method == http.MethodGet && r.URL.Path == projectsPath:
		f.serveProjects(w, r)
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v1/namespaces"):
		f.serveNamespaces(w, r)
	case r.Method == http.MethodPatch:
		_, _ = io.WriteString(w, `{"kind":"Namespace"}`)
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

func (f *fakeRancher) serveProjects(w http.ResponseWriter, r *http.Request) {
	if !f.paginate {
		_, _ = io.WriteString(w, `{"type":"collection","data":[`+alphaProject+`,`+betaProject+`],"pagination":{}}`)
		return
	}
	if r.URL.Query().Get("marker") == "" {
		_, _ = io.WriteString(w, `{"type":"collection","data":[`+alphaProject+`],"pagination":{"next":"`+nextPage+`"}}`)
		return
	}
	_, _ = io.WriteString(w, `{"type":"collection","data":[`+betaProject+`],"pagination":{}}`)
}

func (f *fakeRancher) serveNamespaces(w http.ResponseWriter, r *http.Request) {
	cluster := strings.Split(strings.TrimPrefix(r.URL.Path, "/k8s/clusters/"), "/")[0]
	if cluster == f.forbidden {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	switch cluster {
	case "c-1":
		_, _ = io.WriteString(w, alphaNamespaces)
	case "c-2":
		_, _ = io.WriteString(w, betaNamespaces)
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

func (f *fakeRancher) all() []recorded {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.requests)
}

func (f *fakeRancher) method(name string) []recorded {
	var out []recorded
	for _, request := range f.all() {
		if request.method == name {
			out = append(out, request)
		}
	}
	return out
}

// syncBuffer collects the log output of one run.
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

// newSyncer returns a syncer that copies the labels cost-center and tier, and
// the annotation owner.
func newSyncer(t *testing.T, rancher *fakeRancher, tokenFile string) (*Syncer, *syncBuffer) {
	t.Helper()
	target, err := url.Parse(rancher.server.URL)
	if err != nil {
		t.Fatalf("parse the server URL: %v", err)
	}
	logs := &syncBuffer{}
	syncer, err := New(Config{
		RancherURL:  target,
		TokenFile:   tokenFile,
		Labels:      []string{"cost-center", "tier"},
		Annotations: []string{"owner"},
		Interval:    time.Second,
		Logger:      slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Version:     "test",
	})
	if err != nil {
		t.Fatalf("new syncer: %v", err)
	}
	return syncer, logs
}

func tokenFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "token")
	writeToken(t, path, content)
	return path
}

func writeToken(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write the token file: %v", err)
	}
}
