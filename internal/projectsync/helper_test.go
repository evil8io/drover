package projectsync

import (
	"bytes"
	"encoding/json"
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

	// The five namespaces of cluster c-1. alpha-two and alpha-moved need a
	// patch. alpha-one is in the wanted state already, and its tier label
	// belongs to the tenant, because the managed annotation does not name it.
	// alpha-moved owns tier, and the project does not set it. Rancher writes
	// the project annotation next to the label on every namespace of a
	// project; alpha-three has neither, and alpha-orphan's annotation names a
	// project that the snapshot does not have.
	alphaOneItem    = `{"metadata":{"name":"alpha-one","resourceVersion":"11","labels":{"field.cattle.io/projectId":"p-alpha","cost-center":"cc-1","tier":"gold"},"annotations":{"owner":"alpha@example.com","drover-managed-labels":"cost-center","drover-managed-annotations":"owner","field.cattle.io/projectId":"c-1:p-alpha"}}}`
	alphaTwoItem    = `{"metadata":{"name":"alpha-two","resourceVersion":"12","labels":{"field.cattle.io/projectId":"p-alpha","cost-center":"old","tier":"gold"},"annotations":{"field.cattle.io/projectId":"c-1:p-alpha"}}}`
	alphaThreeItem  = `{"metadata":{"name":"alpha-three","resourceVersion":"13","labels":{"other":"value"},"annotations":{}}}`
	alphaOrphanItem = `{"metadata":{"name":"alpha-orphan","resourceVersion":"14","labels":{"field.cattle.io/projectId":"p-gone"},"annotations":{"field.cattle.io/projectId":"c-1:p-gone"}}}`
	alphaMovedItem  = `{"metadata":{"name":"alpha-moved","resourceVersion":"15","labels":{"field.cattle.io/projectId":"p-alpha","cost-center":"cc-1","tier":"gold"},"annotations":{"owner":"alpha@example.com","drover-managed-labels":"cost-center,tier","drover-managed-annotations":"owner","field.cattle.io/projectId":"c-1:p-alpha"}}}`

	alphaNamespaces = `{"kind":"NamespaceList","items":[` +
		alphaOneItem + `,` + alphaTwoItem + `,` + alphaThreeItem + `,` + alphaOrphanItem + `,` + alphaMovedItem + `]}`

	betaNamespaces = `{"kind":"NamespaceList","items":[
{"metadata":{"name":"beta-one","resourceVersion":"21","labels":{"field.cattle.io/projectId":"p-beta"},"annotations":{"field.cattle.io/projectId":"c-2:p-beta"}}}
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
	// at is the time that the request arrived.
	at time.Time
}

// fakeRancher answers the project list and the namespace list of two clusters.
// It records every request.
type fakeRancher struct {
	server *httptest.Server

	// paginate serves the project list in two pages.
	paginate bool
	// forbidden is the cluster whose namespace list returns status 403.
	forbidden string
	// hangs are the clusters whose namespace list blocks until the request
	// ends.
	hangs map[string]struct{}
	// events are the watch frames of a cluster, in order. The handler writes
	// them, then it ends the stream.
	events map[string][]string
	// patchStatus is the status of the patch of a namespace, by name. A name
	// without an entry gets status 200.
	patchStatus map[string]int
	// patchBody is the answer body of a patch that fails, by namespace name.
	patchBody map[string]string
	// freshNamespaces are the bodies that answer a GET of one namespace, by
	// name. This is the read that refreshItem makes after a DELETED event
	// without a deletionTimestamp. A name without an entry gets status 404.
	freshNamespaces map[string]string
	// lists are the pages of the namespace list of a cluster, by the continue
	// token that selects the page. The empty token selects the first page. A
	// cluster without an entry gets its fixed list.
	lists map[string]map[string]string
	// projectStreams are the answers of the project watch, one per request,
	// in order. A request after the last answer gets status 200, and its
	// stream stays open until the request ends.
	projectStreams []streamAnswer

	mu       sync.Mutex
	requests []recorded
	// expired counts the answers with status 410 that are left for a continue
	// token.
	expired map[string]int
	// projectWatches counts the project watch requests.
	projectWatches int
}

// streamAnswer is one answer of a watch request. A status other than 0
// answers with that status and body. Otherwise the answer writes frames, then
// it ends the stream.
type streamAnswer struct {
	status int
	body   string
	frames []string
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

// hanging blocks the namespace list of every named cluster until the request
// ends, so that the client hits its own timeout.
func hanging(clusters ...string) func(*fakeRancher) {
	return func(f *fakeRancher) {
		f.hangs = make(map[string]struct{}, len(clusters))
		for _, cluster := range clusters {
			f.hangs[cluster] = struct{}{}
		}
	}
}

// watching serves frames as the namespace watch stream of cluster.
func watching(cluster string, frames ...string) func(*fakeRancher) {
	return func(f *fakeRancher) {
		if f.events == nil {
			f.events = make(map[string][]string)
		}
		f.events[cluster] = frames
	}
}

// watchingProjects serves frames as the next project watch stream.
func watchingProjects(frames ...string) func(*fakeRancher) {
	return func(f *fakeRancher) {
		f.projectStreams = append(f.projectStreams, streamAnswer{frames: frames})
	}
}

// failProjectWatch answers the next project watch request with status and
// body.
func failProjectWatch(status int, body string) func(*fakeRancher) {
	return func(f *fakeRancher) {
		f.projectStreams = append(f.projectStreams, streamAnswer{status: status, body: body})
	}
}

// listing serves pages as the namespace list of cluster, by continue token.
func listing(cluster string, pages map[string]string) func(*fakeRancher) {
	return func(f *fakeRancher) {
		if f.lists == nil {
			f.lists = make(map[string]map[string]string)
		}
		f.lists[cluster] = pages
	}
}

// expiring answers the first times list requests with the continue token next
// with status 410.
func expiring(next string, times int) func(*fakeRancher) {
	return func(f *fakeRancher) {
		if f.expired == nil {
			f.expired = make(map[string]int)
		}
		f.expired[next] = times
	}
}

// failPatch answers the patch of namespace with status and body.
func failPatch(namespace string, status int, body string) func(*fakeRancher) {
	return func(f *fakeRancher) {
		if f.patchStatus == nil {
			f.patchStatus = make(map[string]int)
			f.patchBody = make(map[string]string)
		}
		f.patchStatus[namespace] = status
		f.patchBody[namespace] = body
	}
}

// refreshed serves body as the answer of a GET of the single namespace name.
func refreshed(name, body string) func(*fakeRancher) {
	return func(f *fakeRancher) {
		if f.freshNamespaces == nil {
			f.freshNamespaces = make(map[string]string)
		}
		f.freshNamespaces[name] = body
	}
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
		at:     time.Now(),
	})
	f.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.Method == http.MethodGet && r.URL.Path == projectsPath:
		f.serveProjects(w, r)
	case r.Method == http.MethodGet && r.URL.Path == projectsWatchPath && r.URL.Query().Get("watch") == "true":
		f.serveProjectWatch(w, r)
	case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/api/v1/namespaces"):
		f.serveNamespaces(w, r)
	case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/api/v1/namespaces/"):
		f.serveNamespaceItem(w, r)
	case r.Method == http.MethodPatch:
		f.servePatch(w, r)
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

func (f *fakeRancher) serveProjectWatch(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	n := f.projectWatches
	f.projectWatches++
	f.mu.Unlock()

	if n >= len(f.projectStreams) {
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-r.Context().Done()
		return
	}
	answer := f.projectStreams[n]
	if answer.status != 0 {
		w.WriteHeader(answer.status)
		_, _ = io.WriteString(w, answer.body)
		return
	}
	for _, frame := range answer.frames {
		_, _ = io.WriteString(w, frame)
	}
}

func (f *fakeRancher) servePatch(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
	if status, ok := f.patchStatus[name]; ok {
		w.WriteHeader(status)
		_, _ = io.WriteString(w, f.patchBody[name])
		return
	}
	_, _ = io.WriteString(w, `{"kind":"Namespace"}`)
}

// serveNamespaceItem answers a GET of one namespace, by name from the freshNamespaces
// bodies. A name without an entry gets status 404.
func (f *fakeRancher) serveNamespaceItem(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
	f.mu.Lock()
	body, ok := f.freshNamespaces[name]
	f.mu.Unlock()
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	_, _ = io.WriteString(w, body)
}

func (f *fakeRancher) serveNamespaces(w http.ResponseWriter, r *http.Request) {
	cluster := strings.Split(strings.TrimPrefix(r.URL.Path, "/k8s/clusters/"), "/")[0]
	if cluster == f.forbidden {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if _, ok := f.hangs[cluster]; ok {
		<-r.Context().Done()
		return
	}
	if r.URL.Query().Get("watch") == "true" {
		for _, frame := range f.events[cluster] {
			_, _ = io.WriteString(w, frame)
		}
		return
	}
	selector := r.URL.Query().Get("labelSelector")
	if pages, ok := f.lists[cluster]; ok {
		next := r.URL.Query().Get("continue")
		if f.expire(next) {
			w.WriteHeader(http.StatusGone)
			_, _ = io.WriteString(w, `{"kind":"Status","reason":"Expired","message":"the continue token is too old"}`)
			return
		}
		page, ok := pages[next]
		if !ok {
			http.Error(w, "no page for the continue token", http.StatusBadRequest)
			return
		}
		_, _ = io.WriteString(w, selectNamespaces(page, selector))
		return
	}
	switch cluster {
	case "c-1":
		_, _ = io.WriteString(w, selectNamespaces(alphaNamespaces, selector))
	case "c-2":
		_, _ = io.WriteString(w, selectNamespaces(betaNamespaces, selector))
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

// selectNamespaces returns page with only the namespaces that a selector of
// the form key=value selects. Another selector returns page as it is.
func selectNamespaces(page, selector string) string {
	key, value, ok := strings.Cut(selector, "=")
	if !ok {
		return page
	}
	var list struct {
		Kind     string            `json:"kind"`
		Metadata json.RawMessage   `json:"metadata,omitempty"`
		Items    []json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal([]byte(page), &list); err != nil {
		return page
	}
	kept := make([]json.RawMessage, 0, len(list.Items))
	for _, raw := range list.Items {
		var item namespace
		if json.Unmarshal(raw, &item) == nil && item.Metadata.Labels[key] == value {
			kept = append(kept, raw)
		}
	}
	list.Items = kept
	body, err := json.Marshal(list)
	if err != nil {
		return page
	}
	return string(body)
}

// expire reports whether a list request with the continue token next gets
// status 410, and counts that answer.
func (f *fakeRancher) expire(next string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.expired[next] == 0 {
		return false
	}
	f.expired[next]--
	return true
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
// the annotation owner. Each opt can override a field of the Config, for
// example the MeterProvider or the TracerProvider.
func newSyncer(t *testing.T, rancher *fakeRancher, tokenFile string, opts ...func(*Config)) (*Syncer, *syncBuffer) {
	t.Helper()
	target, err := url.Parse(rancher.server.URL)
	if err != nil {
		t.Fatalf("parse the server URL: %v", err)
	}
	logs := &syncBuffer{}
	cfg := Config{
		RancherURL:  target,
		TokenFile:   tokenFile,
		Labels:      []string{"cost-center", "tier"},
		Annotations: []string{"owner"},
		Interval:    time.Second,
		Logger:      slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Version:     "test",
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	syncer, err := New(cfg)
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
