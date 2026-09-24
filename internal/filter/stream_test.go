package filter

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

type readResult struct {
	line string
	err  error
}

func readLine(reader *bufio.Reader) <-chan readResult {
	out := make(chan readResult, 1)
	go func() {
		line, err := reader.ReadString('\n')
		out <- readResult{line: line, err: err}
	}()
	return out
}

func TestListWatchStreams(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	defer releaseOnce()

	const (
		added    = `{"type":"ADDED","object":{"metadata":{"name":"a"}}}` + "\n"
		modified = `{"type":"MODIFIED","object":{"metadata":{"name":"a"}}}` + "\n"
	)

	h := newHarness(t, listUpstream(steveHandler("a"), func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("the upstream response writer has no flusher")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, added)
		flusher.Flush()
		<-release
		_, _ = io.WriteString(w, modified)
		flusher.Flush()
	}))

	resp, err := h.proxy.Client().Do(h.request(t, http.MethodGet, listPath+"?watch=true", nil, callerHeader()))
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	reader := bufio.NewReader(resp.Body)
	select {
	case got := <-readLine(reader):
		if got.err != nil {
			t.Fatalf("read the first event: %v", got.err)
		}
		if got.line != added {
			t.Fatalf("first event = %q, want the ADDED event", got.line)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the first event did not arrive before the second write")
	}

	releaseOnce()

	select {
	case got := <-readLine(reader):
		if got.err != nil {
			t.Fatalf("read the second event: %v", got.err)
		}
		if got.line != modified {
			t.Errorf("second event = %q, want the MODIFIED event", got.line)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the second event did not arrive")
	}

	privileged := h.upstream.privileged(t)
	if got := privileged.query.Get("watch"); got != "true" {
		t.Errorf("privileged watch = %q, want true", got)
	}
	if got := privileged.query.Get("labelSelector"); got != "" {
		t.Errorf("privileged labelSelector = %q, want no selector", got)
	}
	if got := privileged.header.Get("Accept"); got != "application/json" {
		t.Errorf("privileged Accept = %q, want application/json", got)
	}
}

// TestWatchSendsProjectSelector checks the selector of a caller whose
// namespaces are all in its own projects. The selector of the caller merges
// into it, as it does on a list.
func TestWatchSendsProjectSelector(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstreamWithProjects(
		steveLabeledHandler(steveNamespace{name: "a", project: "p-1"}),
		projectsHandler("p-1"),
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
		},
	))

	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath+"?watch=true&labelSelector=team%3Dx", nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	privileged := h.upstream.privileged(t)
	want := "team=x,field.cattle.io/projectId in (p-1)"
	if got := privileged.query.Get("labelSelector"); got != want {
		t.Errorf("labelSelector = %q, want %q", got, want)
	}
}

// TestWatchSendsEmptySelector checks the selector of a caller with no
// namespace and no project.
func TestWatchSendsEmptySelector(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstream(steveHandler(), func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
	}))

	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath+"?watch=true&labelSelector=team%3Dx", nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	privileged := h.upstream.privileged(t)
	want := "team=x,kubernetes.io/metadata.name,!kubernetes.io/metadata.name"
	if got := privileged.query.Get("labelSelector"); got != want {
		t.Errorf("labelSelector = %q, want %q", got, want)
	}
}

// TestWatchFiltersEventsWithEmptySelector checks that the event filter runs
// on a watch with the selector that matches no namespace. Rancher sends no
// such event, and the filter is the second gate.
func TestWatchFiltersEventsWithEmptySelector(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstream(steveHandler(), func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"type":"ADDED","object":{"metadata":{"name":"a"}}}`+"\n")
	}))

	resp, body := h.do(t, h.request(t, http.MethodGet, listPath+"?watch=true", nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(body) != 0 {
		t.Errorf("body = %q, want no event", body)
	}
}

// TestWatchFiltersEventsWithProjectSelector checks that the event filter runs
// on a watch with a project selector. Rancher sends no event outside the
// selector, and the filter is the second gate.
func TestWatchFiltersEventsWithProjectSelector(t *testing.T) {
	t.Parallel()
	const allowed = `{"type":"ADDED","object":{"metadata":{"name":"a","labels":{"field.cattle.io/projectId":"p-1"}}}}` + "\n"
	h := newHarness(t, listUpstreamWithProjects(
		steveLabeledHandler(steveNamespace{name: "a", project: "p-1"}),
		projectsHandler("p-1"),
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"type":"ADDED","object":{"metadata":{"name":"z"}}}`+"\n")
			_, _ = io.WriteString(w, allowed)
		},
	))

	resp, body := h.do(t, h.request(t, http.MethodGet, listPath+"?watch=true", nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if string(body) != allowed {
		t.Errorf("body = %q, want the allowed event %q", body, allowed)
	}
}

func TestWatchDropsDisallowedNamespace(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstream(steveHandler("a"), func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"type":"ADDED","object":{"metadata":{"name":"a"}}}`+"\n")
		_, _ = io.WriteString(w, `{"type":"ADDED","object":{"metadata":{"name":"z"}}}`+"\n")
	}))

	resp, body := h.do(t, h.request(t, http.MethodGet, listPath+"?watch=true", nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	want := `{"type":"ADDED","object":{"metadata":{"name":"a"}}}` + "\n"
	if string(body) != want {
		t.Errorf("body = %q, want %q", body, want)
	}
}

func TestWatchAllowsProjectMember(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstreamWithProjects(
		steveLabeledHandler(steveNamespace{name: "a"}, steveNamespace{name: "b", project: "p-1"}),
		projectsHandler("p-1"),
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"type":"ADDED","object":{"metadata":{"name":"z","labels":{"field.cattle.io/projectId":"p-1"}}}}`+"\n")
		},
	))

	resp, body := h.do(t, h.request(t, http.MethodGet, listPath+"?watch=true", nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	want := `{"type":"ADDED","object":{"metadata":{"name":"z","labels":{"field.cattle.io/projectId":"p-1"}}}}` + "\n"
	if string(body) != want {
		t.Errorf("body = %q, want %q", body, want)
	}
}

func TestWatchPassesBookmark(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstream(steveHandler(), func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"type":"BOOKMARK","object":{"metadata":{"resourceVersion":"99"}}}`+"\n")
	}))

	resp, body := h.do(t, h.request(t, http.MethodGet, listPath+"?watch=true", nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	want := `{"type":"BOOKMARK","object":{"metadata":{"resourceVersion":"99"}}}` + "\n"
	if string(body) != want {
		t.Errorf("body = %q, want %q", body, want)
	}
}

func TestWatchAllowsTableRow(t *testing.T) {
	t.Parallel()
	event := `{"type":"ADDED","object":{"kind":"Table","apiVersion":"meta.k8s.io/v1","metadata":{"resourceVersion":"1"},` +
		`"rows":[{"cells":["a","Active","1h"],"object":{"kind":"PartialObjectMetadata","apiVersion":"meta.k8s.io/v1",` +
		`"metadata":{"name":"a"}}}]}}` + "\n"
	h := newHarness(t, listUpstream(steveHandler("a"), func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, event)
	}))

	resp, body := h.do(t, h.request(t, http.MethodGet, listPath+"?watch=true", nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if string(body) != event {
		t.Errorf("body = %q, want the unchanged table event %q", body, event)
	}
}

func TestWatchDropsTableRowOutsideProjects(t *testing.T) {
	t.Parallel()
	event := `{"type":"ADDED","object":{"kind":"Table","apiVersion":"meta.k8s.io/v1","metadata":{"resourceVersion":"1"},` +
		`"rows":[{"cells":["z","Active","1h"],"object":{"kind":"PartialObjectMetadata","apiVersion":"meta.k8s.io/v1",` +
		`"metadata":{"name":"z"}}}]}}` + "\n"
	h := newHarness(t, listUpstream(steveHandler("a"), func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, event)
	}))

	resp, body := h.do(t, h.request(t, http.MethodGet, listPath+"?watch=true", nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(body) != 0 {
		t.Errorf("body = %q, want no event", body)
	}
}

func TestWatchAllowsTableRowWithProjectLabel(t *testing.T) {
	t.Parallel()
	event := `{"type":"ADDED","object":{"kind":"Table","apiVersion":"meta.k8s.io/v1","metadata":{"resourceVersion":"1"},` +
		`"rows":[{"cells":["z","Active","1h"],"object":{"kind":"PartialObjectMetadata","apiVersion":"meta.k8s.io/v1",` +
		`"metadata":{"name":"z","labels":{"field.cattle.io/projectId":"p-1"}}}}]}}` + "\n"
	h := newHarness(t, listUpstreamWithProjects(
		steveLabeledHandler(steveNamespace{name: "a"}, steveNamespace{name: "b", project: "p-1"}),
		projectsHandler("p-1"),
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, event)
		},
	))

	resp, body := h.do(t, h.request(t, http.MethodGet, listPath+"?watch=true", nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if string(body) != event {
		t.Errorf("body = %q, want the unchanged table event %q", body, event)
	}
}

// TestWatchAllowsPartialObjectMetadata covers a metadata-only informer, which
// asks for application/json;as=PartialObjectMetadataList;v=v1;g=meta.k8s.io.
// Its event object has the kind PartialObjectMetadata, not Table, with the
// name and the labels directly under object.metadata.
func TestWatchAllowsPartialObjectMetadata(t *testing.T) {
	t.Parallel()
	event := `{"type":"ADDED","object":{"kind":"PartialObjectMetadata","apiVersion":"meta.k8s.io/v1",` +
		`"metadata":{"name":"a","labels":{"team":"x"}}}}` + "\n"
	h := newHarness(t, listUpstream(steveHandler("a"), func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, event)
	}))

	header := callerHeader()
	header.Set("Accept", "application/json;as=PartialObjectMetadataList;v=v1;g=meta.k8s.io")
	resp, body := h.do(t, h.request(t, http.MethodGet, listPath+"?watch=true", nil, header))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if string(body) != event {
		t.Errorf("body = %q, want the unchanged event %q", body, event)
	}
}

func TestWatchKeepsCallerTableAccept(t *testing.T) {
	t.Parallel()
	const kubectlTable = "application/json;as=Table;v=v1;g=meta.k8s.io," +
		"application/json;as=Table;v=v1beta1;g=meta.k8s.io,application/json"
	h := newHarness(t, listUpstream(steveHandler("a"), func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
	}))

	header := callerHeader()
	header.Set("Accept", kubectlTable)
	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath+"?watch=true", nil, header))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	privileged := h.upstream.privileged(t)
	if got := privileged.header.Get("Accept"); got != kubectlTable {
		t.Errorf("privileged Accept = %q, want %q", got, kubectlTable)
	}
}

func TestWatchReplacesCallerProtobufAccept(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstream(steveHandler("a"), func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
	}))

	header := callerHeader()
	header.Set("Accept", protobufContentType)
	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath+"?watch=true", nil, header))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	privileged := h.upstream.privileged(t)
	if got := privileged.header.Get("Accept"); got != jsonContentType {
		t.Errorf("privileged Accept = %q, want %q", got, jsonContentType)
	}
}

// TestListWatchWebsocketUpgrade checks that the event filter also reads the
// frames of a watch that Rancher answers with a protocol switch.
func TestListWatchWebsocketUpgrade(t *testing.T) {
	t.Parallel()
	read := make(chan struct{})
	defer close(read)

	allowed := eventLine("a")
	h := newHarness(t, listUpstream(steveHandler("a"), func(w http.ResponseWriter, r *http.Request) {
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Error("the upstream response writer has no hijacker")
			return
		}
		conn, buffered, err := hijacker.Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = buffered.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
		_, _ = buffered.Write(wsStream("", eventLine("z"), allowed))
		_, _ = buffered.Write(wsFrame(true, opcodeClose, nil))
		if err := buffered.Flush(); err != nil {
			t.Errorf("write the upgrade response: %v", err)
			return
		}
		<-read
	}))

	conn, err := net.Dial("tcp", strings.TrimPrefix(h.proxy.URL, "http://"))
	if err != nil {
		t.Fatalf("dial the proxy: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("set the deadline: %v", err)
	}

	req := h.request(t, http.MethodGet, listPath+"?watch=true", nil, http.Header{
		"Authorization":            []string{callerToken},
		"Connection":               []string{"Upgrade"},
		"Upgrade":                  []string{"websocket"},
		"Sec-WebSocket-Key":        []string{"dGhlIHNhbXBsZSBub25jZQ=="},
		"Sec-WebSocket-Version":    []string{"13"},
		"Sec-WebSocket-Extensions": []string{"permessage-deflate"},
	})
	if err := req.Write(conn); err != nil {
		t.Fatalf("write the request: %v", err)
	}

	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, req)
	if err != nil {
		t.Fatalf("read the response: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	if got := resp.Header.Get("Upgrade"); !strings.EqualFold(got, "websocket") {
		t.Errorf("Upgrade = %q, want websocket", got)
	}

	frame, err := readWSFrame(reader)
	if err != nil {
		t.Fatalf("read the first frame: %v", err)
	}
	if string(frame.payload) != string(allowed) {
		t.Errorf("first frame = %q, want the allowed event %q", frame.payload, allowed)
	}
	frame, err = readWSFrame(reader)
	if err != nil {
		t.Fatalf("read the second frame: %v", err)
	}
	if frame.opcode != opcodeClose {
		t.Errorf("second frame opcode = %#x, want the close frame %#x", frame.opcode, opcodeClose)
	}

	requests := h.upstream.all()
	if len(requests) != 5 {
		t.Fatalf("upstream requests = %d, want 5", len(requests))
	}
	privileged := h.upstream.privileged(t)
	if got := privileged.header.Get("Authorization"); got != serviceAuth {
		t.Errorf("privileged Authorization = %q, want %q", got, serviceAuth)
	}
	if got := privileged.query.Get("labelSelector"); got != "" {
		t.Errorf("privileged labelSelector = %q, want no selector", got)
	}
	if got := privileged.header.Get("Upgrade"); got != "websocket" {
		t.Errorf("privileged Upgrade = %q, want websocket", got)
	}
	if got := privileged.header.Get("Connection"); !strings.EqualFold(got, "Upgrade") {
		t.Errorf("privileged Connection = %q, want Upgrade", got)
	}
	if got := privileged.header.Get(extensionsHeader); got != "" {
		t.Errorf("privileged %s = %q, want no extension", extensionsHeader, got)
	}
}

// bookmarkEvent is the first event of openWatch. A BOOKMARK passes the event
// filter, so it tells the test that the stream is open.
const bookmarkEvent = `{"type":"BOOKMARK","object":{"metadata":{"resourceVersion":"1"}}}` + "\n"

// projectSet is the project id list that an upstream handler answers. A test
// changes it while a watch runs.
type projectSet struct {
	mu  sync.Mutex
	ids []string
}

func newProjectSet(ids ...string) *projectSet {
	return &projectSet{ids: ids}
}

func (p *projectSet) set(ids ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ids = ids
}

func (p *projectSet) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		ids := slices.Clone(p.ids)
		p.mu.Unlock()
		projectsHandler(ids...)(w, r)
	}
}

// shortTTL shortens the cache TTL, which is also the interval of the ticker
// that re-reads the projects of a watch.
func shortTTL(cfg *Config) {
	cfg.CacheTTL = 20 * time.Millisecond
}

// openWatch answers a watch with one BOOKMARK event, and keeps the stream
// open until release closes.
func openWatch(release <-chan struct{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, bookmarkEvent)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-release
	}
}

// startWatch opens a namespace watch through the proxy, and reads the
// BOOKMARK event of openWatch, so the stream runs when it returns.
func startWatch(t *testing.T, h *harness) *bufio.Reader {
	t.Helper()
	resp, err := h.proxy.Client().Do(h.request(t, http.MethodGet, listPath+"?watch=true", nil, callerHeader()))
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	reader := bufio.NewReader(resp.Body)
	select {
	case got := <-readLine(reader):
		if got.err != nil {
			t.Fatalf("read the bookmark: %v", got.err)
		}
		if got.line != bookmarkEvent {
			t.Fatalf("first event = %q, want the BOOKMARK event", got.line)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the bookmark did not arrive")
	}
	return reader
}

// streamEnd reads the rest of the stream. It reports a nil error for a clean
// end of the stream.
func streamEnd(body io.Reader) <-chan error {
	out := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, body)
		out <- err
	}()
	return out
}

// waitFor polls condition until it holds, or until 10 s pass.
func waitFor(condition func() bool) bool {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// TestWatchEndsOnProjectChange checks that the ticker ends a chunked stream
// once the caller gets an allowed namespace in a second project. The
// selector of the watch names the first project only, so Rancher sends no
// event for the namespace of the second project.
func TestWatchEndsOnProjectChange(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	namespaces := newNamespaceSet(steveNamespace{name: "a", project: "p-1"})
	projects := newProjectSet("p-1")
	h := newHarnessOpt(t, listUpstreamWithProjects(
		namespaces.handler(),
		projects.handler(),
		openWatch(release),
	), shortTTL)

	ended := streamEnd(startWatch(t, h))
	namespaces.set(steveNamespace{name: "a", project: "p-1"}, steveNamespace{name: "c", project: "p-2"})
	projects.set("p-1", "p-2")
	h.clock.advance(time.Minute)

	select {
	case err := <-ended:
		if err != nil {
			t.Errorf("stream error = %v, want a clean end of the stream", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the stream stayed open after the project set changed")
	}
}

// TestWatchStaysOpenWithTheSameProjects checks that the ticker re-reads the
// allowed set, and that it keeps the stream open while the projects are the
// same.
func TestWatchStaysOpenWithTheSameProjects(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	projects := newProjectSet("p-1")
	h := newHarnessOpt(t, listUpstreamWithProjects(
		steveLabeledHandler(steveNamespace{name: "a", project: "p-1"}),
		projects.handler(),
		openWatch(release),
	), shortTTL)

	ended := streamEnd(startWatch(t, h))
	before := h.upstream.countPath(projectsPath)
	h.clock.advance(time.Minute)

	if !waitFor(func() bool { return h.upstream.countPath(projectsPath) > before }) {
		t.Fatal("the ticker did not re-read the projects of the caller")
	}
	select {
	case err := <-ended:
		t.Fatalf("the stream ended with %v, want an open stream", err)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestWatchWithoutSelectorHasNoTicker checks that a watch of a caller with a
// namespace outside its own projects gets no ticker. That watch has no
// selector, so the event filter shows a new project by itself.
func TestWatchWithoutSelectorHasNoTicker(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	projects := newProjectSet()
	h := newHarnessOpt(t, listUpstreamWithProjects(
		steveHandler("a"),
		projects.handler(),
		openWatch(release),
	), shortTTL)

	ended := streamEnd(startWatch(t, h))
	projects.set("p-1")
	h.clock.advance(time.Minute)
	time.Sleep(200 * time.Millisecond)

	if got := h.upstream.countPath(projectsPath); got != 1 {
		t.Errorf("project list requests = %d, want 1", got)
	}
	select {
	case err := <-ended:
		t.Fatalf("the stream ended with %v, want an open stream", err)
	default:
	}
}

// TestUpgradedWatchEndsOnProjectChange checks that the ticker also ends an
// upgraded stream, and that the client reads a close frame before the end.
func TestUpgradedWatchEndsOnProjectChange(t *testing.T) {
	t.Parallel()
	read := make(chan struct{})
	defer close(read)

	event := eventLine("a")
	namespaces := newNamespaceSet(steveNamespace{name: "a", project: "p-1"})
	projects := newProjectSet("p-1")
	h := newHarnessOpt(t, listUpstreamWithProjects(
		namespaces.handler(),
		projects.handler(),
		func(w http.ResponseWriter, r *http.Request) {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Error("the upstream response writer has no hijacker")
				return
			}
			conn, buffered, err := hijacker.Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			defer func() { _ = conn.Close() }()
			_, _ = buffered.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
			_, _ = buffered.Write(wsMessage("", event))
			if err := buffered.Flush(); err != nil {
				t.Errorf("write the upgrade response: %v", err)
				return
			}
			<-read
		},
	), shortTTL)

	conn, err := net.Dial("tcp", strings.TrimPrefix(h.proxy.URL, "http://"))
	if err != nil {
		t.Fatalf("dial the proxy: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatalf("set the deadline: %v", err)
	}

	req := h.request(t, http.MethodGet, listPath+"?watch=true", nil, http.Header{
		"Authorization":         []string{callerToken},
		"Connection":            []string{"Upgrade"},
		"Upgrade":               []string{"websocket"},
		"Sec-WebSocket-Key":     []string{"dGhlIHNhbXBsZSBub25jZQ=="},
		"Sec-WebSocket-Version": []string{"13"},
	})
	if err := req.Write(conn); err != nil {
		t.Fatalf("write the request: %v", err)
	}

	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, req)
	if err != nil {
		t.Fatalf("read the response: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	frame, err := readWSFrame(reader)
	if err != nil {
		t.Fatalf("read the first frame: %v", err)
	}
	if string(frame.payload) != string(event) {
		t.Fatalf("first frame = %q, want the allowed event %q", frame.payload, event)
	}

	namespaces.set(steveNamespace{name: "a", project: "p-1"}, steveNamespace{name: "c", project: "p-2"})
	projects.set("p-1", "p-2")
	h.clock.advance(time.Minute)

	frame, err = readWSFrame(reader)
	if err != nil {
		t.Fatalf("read the close frame: %v", err)
	}
	if frame.opcode != opcodeClose || string(frame.payload) != string([]byte{0x03, 0xe8}) {
		t.Errorf("frame = %+v, want the close frame with status 1000", frame)
	}
	if _, err := reader.ReadByte(); !errors.Is(err, io.EOF) {
		t.Errorf("read after the close frame = %v, want io.EOF", err)
	}
}

// TestWatchEndsOnNamespaceOutsideProjects checks that the ticker ends the
// stream once the caller gets a namespace outside its own projects. The
// selector of the watch holds the projects of the caller only, and Rancher
// drops that namespace server-side, so no event reaches the event filter.
func TestWatchEndsOnNamespaceOutsideProjects(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	namespaces := newNamespaceSet(steveNamespace{name: "a", project: "p-1"})
	h := newHarnessOpt(t, listUpstreamWithProjects(
		namespaces.handler(),
		newProjectSet("p-1").handler(),
		openWatch(release),
	), shortTTL)

	ended := streamEnd(startWatch(t, h))
	namespaces.set(steveNamespace{name: "a", project: "p-1"}, steveNamespace{name: "b"})
	h.clock.advance(time.Minute)

	select {
	case err := <-ended:
		if err != nil {
			t.Errorf("stream error = %v, want a clean end of the stream", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the stream stayed open after the caller got a namespace outside its projects")
	}
}

// TestWatchStaysOpenOnNamespaceInsideProjects checks that a new namespace in a
// project of the caller keeps the stream open. The selector for the same
// projects is the same string, and Rancher sends the event for that namespace.
func TestWatchStaysOpenOnNamespaceInsideProjects(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	namespaces := newNamespaceSet(steveNamespace{name: "a", project: "p-1"})
	h := newHarnessOpt(t, listUpstreamWithProjects(
		namespaces.handler(),
		newProjectSet("p-1").handler(),
		openWatch(release),
	), shortTTL)

	ended := streamEnd(startWatch(t, h))
	before := h.upstream.countPath(stevePath)
	namespaces.set(steveNamespace{name: "a", project: "p-1"}, steveNamespace{name: "b", project: "p-1"})
	h.clock.advance(time.Minute)

	if !waitFor(func() bool { return h.upstream.countPath(stevePath) > before }) {
		t.Fatal("the ticker did not re-read the allowed set of the caller")
	}
	select {
	case err := <-ended:
		t.Fatalf("the stream ended with %v, want an open stream", err)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestWatchIsRegisteredWhileOpen checks that watch=yes opens a watch, that
// the privileged request keeps that literal query value, and that the stream
// is registered while it is open.
func TestWatchIsRegisteredWhileOpen(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	h := newHarness(t, listUpstream(steveHandler("a"), openWatch(release)))

	resp, err := h.proxy.Client().Do(h.request(t, http.MethodGet, listPath+"?watch=yes", nil, callerHeader()))
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	if got := h.svc.watches.len(); got != 1 {
		t.Errorf("watches open = %d, want 1, the stream must be registered while it is open", got)
	}

	privileged := h.upstream.privileged(t)
	if got := privileged.query.Get("watch"); got != "yes" {
		t.Errorf("privileged watch = %q, want yes", got)
	}
}

// TestWatchKeepsQueryValueAndFiltersEvents checks the two truthy forms of
// watch that carry no explicit value, watch=yes and watch=, on the namespace
// path: the privileged request keeps the literal value, an event outside the
// allowed set is dropped, and the log line has watch=true.
func TestWatchKeepsQueryValueAndFiltersEvents(t *testing.T) {
	t.Parallel()
	for _, query := range []string{"watch=yes", "watch="} {
		t.Run(query, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, listUpstream(steveHandler("a"), func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, `{"type":"ADDED","object":{"metadata":{"name":"z"}}}`+"\n")
			}))

			resp, body := h.do(t, h.request(t, http.MethodGet, listPath+"?"+query, nil, callerHeader()))
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
			if len(body) != 0 {
				t.Errorf("body = %q, want the event outside the allowed set dropped", body)
			}

			privileged := h.upstream.privileged(t)
			if !privileged.query.Has("watch") {
				t.Fatal("the privileged query has no watch parameter")
			}
			if want := strings.TrimPrefix(query, "watch="); privileged.query.Get("watch") != want {
				t.Errorf("privileged watch = %q, want %q", privileged.query.Get("watch"), want)
			}

			if !strings.Contains(h.logs.String(), "watch=true") {
				t.Errorf("logs have no watch=true line: %s", h.logs.String())
			}
		})
	}
}

// TestWatchStripsAcceptEncodingAndContentEncoding checks that the privileged
// watch request carries no Accept-Encoding, also when the caller sends one,
// and that the filtered 200 answer carries no Content-Encoding, also when
// the upstream sets one.
func TestWatchStripsAcceptEncodingAndContentEncoding(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstream(steveHandler("a"), func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Accept-Encoding"); got != "" {
			t.Errorf("privileged Accept-Encoding = %q, want no header", got)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(http.StatusOK)
	}))

	header := callerHeader()
	header.Set("Accept-Encoding", "gzip")
	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath+"?watch=true", nil, header))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Encoding"); got != "" {
		t.Errorf("filtered Content-Encoding = %q, want no header", got)
	}
}

// TestMaxWatchesCapsANamespaceWatch checks that a namespace watch above
// MaxWatches answers 503 while an earlier watch stays open, that the log
// line names the capped outcome, and that a plain list is not capped.
func TestMaxWatchesCapsANamespaceWatch(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	defer releaseOnce()

	h := newHarnessOpt(t, listUpstream(steveHandler("a"), func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !watchRequested(r.URL.Query()) {
			_, _ = io.WriteString(w, `{"kind":"NamespaceList","items":[]}`)
			return
		}
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("the upstream response writer has no flusher")
			return
		}
		w.WriteHeader(http.StatusOK)
		flusher.Flush()
		<-release
	}), func(cfg *Config) { cfg.MaxWatches = 1 })

	resp1, err := h.proxy.Client().Do(h.request(t, http.MethodGet, listPath+"?watch=true", nil, callerHeader()))
	if err != nil {
		t.Fatalf("first watch: %v", err)
	}
	t.Cleanup(func() { _ = resp1.Body.Close() })
	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("first watch status = %d, want 200", resp1.StatusCode)
	}
	if got := h.svc.watches.len(); got != 1 {
		t.Fatalf("watches open = %d, want 1", got)
	}

	resp2, body2 := h.do(t, h.request(t, http.MethodGet, listPath+"?watch=true", nil, callerHeader()))
	if resp2.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("second watch status = %d, want 503", resp2.StatusCode)
	}
	var status statusBody
	if err := json.Unmarshal(body2, &status); err != nil {
		t.Fatalf("parse the response %q: %v", body2, err)
	}
	if status.Reason != reasonUnavailable {
		t.Errorf("reason = %q, want %q", status.Reason, reasonUnavailable)
	}
	if !strings.Contains(status.Message, "1") {
		t.Errorf("message = %q, want it to name the limit 1", status.Message)
	}
	if !strings.Contains(h.logs.String(), "outcome=capped") {
		t.Errorf("logs have no outcome=capped line: %s", h.logs.String())
	}
	if !strings.Contains(h.logs.String(), "status=503") {
		t.Errorf("logs have no status=503 line: %s", h.logs.String())
	}

	resp3, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
	if resp3.StatusCode != http.StatusOK {
		t.Errorf("plain list status = %d, want 200, the cap must not affect a non-watch request", resp3.StatusCode)
	}
}

// TestMaxWatchesPerCallerCapsANamespaceWatch checks that a namespace watch of
// a caller at MaxWatchesPerCaller answers 503 with the per-caller message, and
// that drover.filter.watches.rejected names the caller limit. A second caller
// still opens a watch.
func TestMaxWatchesPerCallerCapsANamespaceWatch(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	metricReader := sdkmetric.NewManualReader()
	h := newHarnessOpt(t, listUpstream(steveHandler("a"), openWatch(release)),
		recordingMeter(metricReader), func(cfg *Config) { cfg.MaxWatchesPerCaller = 2 })

	for range 2 {
		startWatch(t, h)
	}
	resp, body := h.do(t, h.request(t, http.MethodGet, listPath+"?watch=true", nil, callerHeader()))
	wantWatchCapped(t, resp, body, "per-caller limit of 2")
	wantRejected(t, metricReader, limitCaller, 1)

	if got := watchStatus(t, h, listPath+"?watch=true", otherCallerHeader()); got != http.StatusOK {
		t.Errorf("watch status of a second caller = %d, want 200", got)
	}
}

// TestMaxWatchesHoldsAgainstAParallelBurst checks that a parallel burst of
// twice MaxWatches namespace watches opens at most MaxWatches privileged
// watches upstream, while the upstream keeps each of them open, and that
// every other watch answers 503.
func TestMaxWatchesHoldsAgainstAParallelBurst(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	const limit = 5
	metricReader := sdkmetric.NewManualReader()
	h := newHarnessOpt(t, listUpstream(steveHandler("a"), openWatch(release)),
		recordingMeter(metricReader), func(cfg *Config) {
			cfg.MaxWatches = limit
			cfg.MaxWatchesPerCaller = 2 * limit
		})

	statuses := make(chan int, 2*limit)
	var group sync.WaitGroup
	for range 2 * limit {
		group.Go(func() {
			statuses <- watchStatus(t, h, listPath+"?watch=true", callerHeader())
		})
	}
	group.Wait()
	close(statuses)

	counts := map[int]int{}
	for status := range statuses {
		counts[status]++
	}
	if counts[http.StatusOK] != limit || counts[http.StatusServiceUnavailable] != limit {
		t.Errorf("statuses = %v, want %d of 200 and %d of 503", counts, limit, limit)
	}
	privileged := 0
	for _, request := range h.upstream.all() {
		if request.path == listPath && request.header.Get("Authorization") == serviceAuth {
			privileged++
		}
	}
	if privileged > limit {
		t.Errorf("privileged watches upstream = %d, want at most %d", privileged, limit)
	}
	wantRejected(t, metricReader, limitShared, limit)
}

// TestNamespaceWatchReleasesTheSlotOnAFailure checks that a namespace watch
// that fails after the reserve frees its slot. The caller has one slot, so a
// leaked slot answers the next watch with 503.
func TestNamespaceWatchReleasesTheSlotOnAFailure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		// fail answers the privileged watch while the failure lasts. Nil
		// means that the token file is absent instead.
		fail http.HandlerFunc
		want int
	}{
		{"upstream error status", errorStatus, http.StatusInternalServerError},
		{"closed connection", closeConnection, http.StatusBadGateway},
		{"no token", nil, http.StatusBadGateway},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			release := make(chan struct{})
			defer close(release)

			var failing atomic.Bool
			failing.Store(true)
			watch := openWatch(release)
			privileged := func(w http.ResponseWriter, r *http.Request) {
				if failing.Load() && test.fail != nil {
					test.fail(w, r)
					return
				}
				watch(w, r)
			}
			tokenFile := filepath.Join(t.TempDir(), "token")
			if test.fail != nil {
				writeToken(t, tokenFile, "service")
			}
			h := newHarnessWithTokenFile(t, listUpstream(steveHandler("a"), privileged), tokenFile, func(cfg *Config) {
				cfg.MaxWatches = 1
				cfg.MaxWatchesPerCaller = 1
			})

			resp, _ := h.do(t, h.request(t, http.MethodGet, listPath+"?watch=true", nil, callerHeader()))
			if resp.StatusCode != test.want {
				t.Fatalf("status of the failed watch = %d, want %d", resp.StatusCode, test.want)
			}
			if got := h.svc.watches.reservedCount(); got != 0 {
				t.Errorf("reserved slots after the failure = %d, want 0", got)
			}

			failing.Store(false)
			writeToken(t, tokenFile, "service")
			startWatch(t, h)
		})
	}
}
