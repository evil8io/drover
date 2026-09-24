package filter

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// podEvent returns the ADDED event of the pod of one namespace, with the
// newline that separates two events of a watch stream.
func podEvent(namespace string) string {
	return `{"type":"ADDED","object":{"metadata":{"name":"pod-` + namespace + `","namespace":"` + namespace + `"}}}` + "\n"
}

// bookmarkLine is a BOOKMARK event of one upstream watch. Its resourceVersion
// belongs to that namespace alone.
const bookmarkLine = `{"type":"BOOKMARK","object":{"metadata":{"resourceVersion":"7"}}}` + "\n"

// writeWatchLines writes the events of one upstream watch, and flushes them,
// so the merge reads them before the handler returns.
func writeWatchLines(w http.ResponseWriter, lines []string) {
	for _, line := range lines {
		_, _ = io.WriteString(w, line)
	}
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

// namespaceWatches answers a namespaced watch with the events that lines
// holds for the namespace, and it keeps the stream open until release closes.
// A namespace without an entry answers 403, which is what a namespace where
// the caller may not watch gives.
func namespaceWatches(release <-chan struct{}, lines map[string][]string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		namespace, _, _ := splitNamespaced(r.URL.Path)
		events, ok := lines[namespace]
		if !ok {
			writeForbidden(w, namespacedForbidden)
			return
		}
		w.Header().Set("Content-Type", jsonContentType)
		w.WriteHeader(http.StatusOK)
		writeWatchLines(w, events)
		<-release
	}
}

// startMergedWatch opens a cluster-wide watch through the proxy, and returns
// the answer with a reader of the merged stream.
func startMergedWatch(t *testing.T, h *harness, target string) (*http.Response, *bufio.Reader) {
	t.Helper()
	resp, err := h.proxy.Client().Do(h.request(t, http.MethodGet, target, nil, callerHeader()))
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	return resp, bufio.NewReader(resp.Body)
}

// nextEvent returns the next event of the merged stream.
func nextEvent(t *testing.T, reader *bufio.Reader) string {
	t.Helper()
	select {
	case got := <-readLine(reader):
		if got.err != nil {
			t.Fatalf("read an event: %v", got.err)
		}
		return got.line
	case <-time.After(10 * time.Second):
		t.Fatal("the event did not arrive")
	}
	return ""
}

// mergedEvents reads count events, and sorts them. The upstream watches run
// at the same time, so the events of two namespaces arrive in no fixed order.
func mergedEvents(t *testing.T, reader *bufio.Reader, count int) []string {
	t.Helper()
	events := make([]string, 0, count)
	for range count {
		events = append(events, nextEvent(t, reader))
	}
	slices.Sort(events)
	return events
}

// wantNoEvent checks that the stream stays open, and that it gives no event.
// A caller reads no further event after this check, because the read of it
// runs on.
func wantNoEvent(t *testing.T, reader *bufio.Reader) {
	t.Helper()
	select {
	case got := <-readLine(reader):
		t.Fatalf("the stream gave the line %q with the error %v, want no event", got.line, got.err)
	case <-time.After(100 * time.Millisecond):
	}
}

// wantCleanEnd checks that the rest of the stream ends without an error.
func wantCleanEnd(t *testing.T, reader io.Reader, message string) {
	t.Helper()
	select {
	case err := <-streamEnd(reader):
		if err != nil {
			t.Errorf("stream error = %v, want a clean end of the stream", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal(message)
	}
}

// TestMergedWatchAboveTheCapIsForbidden checks the limit of a merged watch,
// which is below the limit of a list, and that no upstream watch opens above
// it.
func TestMergedWatchAboveTheCapIsForbidden(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	h := newHarnessOpt(t, collectionUpstream(
		steveHandler("a", "b"),
		namespaceWatches(release, map[string][]string{"a": {podEvent("a")}, "b": {podEvent("b")}}),
	), withFanout, func(cfg *Config) { cfg.FanoutMaxWatchNamespaces = 1 })

	resp, body := h.do(t, h.request(t, http.MethodGet, podsPath+"?watch=true", nil, callerHeader()))
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}

	var status statusBody
	if err := json.Unmarshal(body, &status); err != nil {
		t.Fatalf("parse the response %q: %v", body, err)
	}
	if status.Reason != reasonForbidden {
		t.Errorf("reason = %q, want %q", status.Reason, reasonForbidden)
	}
	if status.Code != http.StatusForbidden {
		t.Errorf("code = %d, want 403", status.Code)
	}
	numbers := regexp.MustCompile(`[0-9]+`).FindAllString(status.Message, -1)
	if !slices.Contains(numbers, "2") || !slices.Contains(numbers, "1") {
		t.Errorf("message = %q, want the allowed count 2 and the limit 1", status.Message)
	}
	if got := len(namespacedRecords(h.upstream)); got != 0 {
		t.Errorf("namespaced requests = %d, want 0", got)
	}
}

// TestMergedWatchWithoutAnAllowedNamespaceStreamsNoEvent checks that a caller
// that may see no namespace gets an open stream, as the list path gives it a
// collection without an element.
func TestMergedWatchWithoutAnAllowedNamespaceStreamsNoEvent(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	h := newHarnessOpt(t, collectionUpstreamWithDiscovery(
		steveHandler(), namespaceWatches(release, nil), coreDiscovery(),
	), withFanout)

	_, reader := startMergedWatch(t, h, podsPath+"?watch=true")
	if got := len(namespacedRecords(h.upstream)); got != 0 {
		t.Errorf("namespaced requests = %d, want 0", got)
	}
	wantNoEvent(t, reader)
}

// TestMergedWatchWithoutAnAllowedNamespaceKeepsNativeForAClusterScopedKind
// checks that a kind without a namespace scope keeps the native answer, which
// discovery reports.
func TestMergedWatchWithoutAnAllowedNamespaceKeepsNativeForAClusterScopedKind(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	h := newHarnessOpt(t, collectionUpstreamWithDiscovery(
		steveHandler(), namespaceWatches(release, nil), coreDiscovery(),
	), withFanout)

	resp, body := h.do(t, h.request(t, http.MethodGet, nodesPath+"?watch=true", nil, callerHeader()))
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}
	if string(body) != nativeForbidden {
		t.Errorf("body = %q, want the native answer", body)
	}
	if got := len(namespacedRecords(h.upstream)); got != 0 {
		t.Errorf("namespaced requests = %d, want 0", got)
	}
}

// TestMergedWatchMergesEveryAllowedNamespace checks that the client gets the
// events of every allowed namespace, and that each upstream watch goes to the
// namespaced path with the credentials of the caller.
func TestMergedWatchMergesEveryAllowedNamespace(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	h := newHarnessOpt(t, collectionUpstream(
		steveHandler("b", "a"),
		namespaceWatches(release, map[string][]string{"a": {podEvent("a")}, "b": {podEvent("b")}}),
	), withFanout)

	_, reader := startMergedWatch(t, h, podsPath+"?watch=true")
	want := []string{podEvent("a"), podEvent("b")}
	if got := mergedEvents(t, reader, 2); !slices.Equal(got, want) {
		t.Errorf("events = %q, want %q", got, want)
	}

	wantPaths := []string{
		"/k8s/clusters/c-1/api/v1/namespaces/a/pods",
		"/k8s/clusters/c-1/api/v1/namespaces/b/pods",
	}
	if got := namespacedPaths(h.upstream); !slices.Equal(got, wantPaths) {
		t.Errorf("namespaced paths = %v, want %v", got, wantPaths)
	}
	for _, request := range namespacedRecords(h.upstream) {
		if got := request.header.Get("Authorization"); got != callerToken {
			t.Errorf("%s Authorization = %q, want %q", request.path, got, callerToken)
		}
		if got := request.query.Get("watch"); got != "true" {
			t.Errorf("%s watch = %q, want true", request.path, got)
		}
	}
}

// TestMergedWatchOpensEveryWatchAtTheSameTime checks that the upstream
// watches run at the same time. A merge that opens them one by one gives the
// events of the last namespace only after the watch before it ends.
func TestMergedWatchOpensEveryWatchAtTheSameTime(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	const namespaces = 3
	lines := map[string][]string{}
	names := make([]string, 0, namespaces)
	want := make([]string, 0, namespaces)
	for _, name := range []string{"a", "b", "c"} {
		names = append(names, name)
		lines[name] = []string{podEvent(name)}
		want = append(want, podEvent(name))
	}

	gate := newWave(namespaces)
	watches := namespaceWatches(release, lines)
	h := newHarnessOpt(t, collectionUpstream(
		steveHandler(names...),
		func(w http.ResponseWriter, r *http.Request) {
			gate.enter()
			defer gate.leave()
			watches(w, r)
		},
	), withFanout)

	_, reader := startMergedWatch(t, h, podsPath+"?watch=true")
	if got := mergedEvents(t, reader, namespaces); !slices.Equal(got, want) {
		t.Errorf("events = %q, want %q", got, want)
	}
	if got := gate.highestInFlight(); got != namespaces {
		t.Errorf("upstream watches at the same time = %d, want %d", got, namespaces)
	}
}

// TestMergedWatchSkipsForbiddenNamespace checks that a namespace that answers
// 403 drops out of the merge, and that the other namespace still streams.
func TestMergedWatchSkipsForbiddenNamespace(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	h := newHarnessOpt(t, collectionUpstream(
		steveHandler("a", "b"),
		namespaceWatches(release, map[string][]string{"b": {podEvent("b")}}),
	), withFanout)

	_, reader := startMergedWatch(t, h, podsPath+"?watch=true")
	if got := nextEvent(t, reader); got != podEvent("b") {
		t.Errorf("event = %q, want %q", got, podEvent("b"))
	}
	if got := len(namespacedRecords(h.upstream)); got != 2 {
		t.Errorf("namespaced requests = %d, want 2", got)
	}
	wantNoEvent(t, reader)
}

// TestMergedWatchWithANotFoundNamespaceKeepsNative checks the answer for a
// kind that has no namespace scope. Its namespaced path answers 404 in every
// namespace.
func TestMergedWatchWithANotFoundNamespaceKeepsNative(t *testing.T) {
	t.Parallel()
	h := newHarnessOpt(t, collectionUpstream(
		steveHandler("a", "b"), notFoundHandler,
	), withFanout)

	resp, body := h.do(t, h.request(t, http.MethodGet, podsPath+"?watch=true", nil, callerHeader()))
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}
	if string(body) != nativeForbidden {
		t.Errorf("body = %q, want the native answer", body)
	}
}

// TestMergedWatchWithEveryNamespaceForbiddenKeepsNative checks the answer for
// an allowed set where no namespace answers 200. The caller then keeps the
// native 403, also when discovery reports a namespaced kind.
func TestMergedWatchWithEveryNamespaceForbiddenKeepsNative(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	h := newHarnessOpt(t, collectionUpstreamWithDiscovery(
		steveHandler("a", "b"), namespaceWatches(release, nil), coreDiscovery(),
	), withFanout)

	resp, body := h.do(t, h.request(t, http.MethodGet, podsPath+"?watch=true", nil, callerHeader()))
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}
	if string(body) != nativeForbidden {
		t.Errorf("body = %q, want the native answer", body)
	}
	if got := len(namespacedRecords(h.upstream)); got != 2 {
		t.Errorf("namespaced requests = %d, want 2", got)
	}
}

// TestMergedWatchKeepsEveryEventOfOneNamespace checks that the events of one
// upstream watch reach the client unchanged, and in their order.
func TestMergedWatchKeepsEveryEventOfOneNamespace(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	const (
		added    = `{"type":"ADDED","object":{"metadata":{"name":"pod-a","namespace":"a"}}}` + "\n"
		modified = `{"type":"MODIFIED","object":{"metadata":{"name":"pod-a","namespace":"a"}}}` + "\n"
		deleted  = `{"type":"DELETED","object":{"metadata":{"name":"pod-a","namespace":"a"}}}` + "\n"
	)
	h := newHarnessOpt(t, collectionUpstream(
		steveHandler("a"),
		namespaceWatches(release, map[string][]string{"a": {added, modified, deleted}}),
	), withFanout)

	_, reader := startMergedWatch(t, h, podsPath+"?watch=true")
	for _, want := range []string{added, modified, deleted} {
		if got := nextEvent(t, reader); got != want {
			t.Errorf("event = %q, want %q", got, want)
		}
	}
}

// TestMergedWatchDropsBookmark checks that no BOOKMARK reaches the client.
// The resourceVersion of a bookmark belongs to one namespace, so it is no
// resume point of the merge.
func TestMergedWatchDropsBookmark(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	h := newHarnessOpt(t, collectionUpstream(
		steveHandler("a"),
		namespaceWatches(release, map[string][]string{"a": {bookmarkLine, podEvent("a")}}),
	), withFanout)

	_, reader := startMergedWatch(t, h, podsPath+"?watch=true")
	if got := nextEvent(t, reader); got != podEvent("a") {
		t.Errorf("event = %q, want the ADDED event %q", got, podEvent("a"))
	}
	wantNoEvent(t, reader)
}

// TestMergedWatchKeepsTheCallerQuery checks the query of an upstream watch.
// It keeps the window that the caller asks for, and it drops the parameters
// that belong to one namespace.
func TestMergedWatchKeepsTheCallerQuery(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	h := newHarnessOpt(t, collectionUpstream(
		steveHandler("a", "b"),
		namespaceWatches(release, map[string][]string{"a": {podEvent("a")}, "b": {podEvent("b")}}),
	), withFanout)

	target := podsPath + "?watch=true&resourceVersion=42&timeoutSeconds=300&allowWatchBookmarks=true" +
		"&limit=500&continue=tok&labelSelector=team%3Dx"
	_, reader := startMergedWatch(t, h, target)
	if got := len(mergedEvents(t, reader, 2)); got != 2 {
		t.Fatalf("events = %d, want 2", got)
	}

	records := namespacedRecords(h.upstream)
	if len(records) != 2 {
		t.Fatalf("namespaced requests = %d, want 2", len(records))
	}
	for _, request := range records {
		if got := request.query.Get("resourceVersion"); got != "42" {
			t.Errorf("%s resourceVersion = %q, want 42", request.path, got)
		}
		if got := request.query.Get("timeoutSeconds"); got != "300" {
			t.Errorf("%s timeoutSeconds = %q, want 300", request.path, got)
		}
		if got := request.query.Get("labelSelector"); got != "team=x" {
			t.Errorf("%s labelSelector = %q, want team=x", request.path, got)
		}
		if request.query.Has("limit") || request.query.Has("continue") || request.query.Has("allowWatchBookmarks") {
			t.Errorf("%s query = %v, want no limit, no continue and no allowWatchBookmarks", request.path, request.query)
		}
	}
}

// TestMergedWatchEndsWhenAnUpstreamWatchEnds checks that the end of one
// upstream watch ends the merged stream. The client then re-lists, and it
// reads no stale data.
func TestMergedWatchEndsWhenAnUpstreamWatchEnds(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	// The watch of namespace b ends on this signal, after the client read
	// every event. The end of the merge drops an event that is still on its
	// way, so an earlier end makes the count of the events uncertain.
	endB := make(chan struct{})
	endBOnce := sync.OnceFunc(func() { close(endB) })
	defer endBOnce()

	h := newHarnessOpt(t, collectionUpstream(
		steveHandler("a", "b"),
		func(w http.ResponseWriter, r *http.Request) {
			namespace, _, _ := splitNamespaced(r.URL.Path)
			w.Header().Set("Content-Type", jsonContentType)
			w.WriteHeader(http.StatusOK)
			writeWatchLines(w, []string{podEvent(namespace)})
			if namespace == "b" {
				<-endB
				return
			}
			<-release
		},
	), withFanout)

	_, reader := startMergedWatch(t, h, podsPath+"?watch=true")
	want := []string{podEvent("a"), podEvent("b")}
	if got := mergedEvents(t, reader, 2); !slices.Equal(got, want) {
		t.Errorf("events = %q, want %q", got, want)
	}

	endBOnce()
	wantCleanEnd(t, reader, "the merged stream stayed open after an upstream watch ended")
}

// TestMergedWatchEndsOnAllowedSetChange checks that the ticker ends the
// stream once the caller may see a namespace that the merge does not cover.
func TestMergedWatchEndsOnAllowedSetChange(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	namespaces := newNamespaceSet(steveNamespace{name: "a"})
	h := newHarnessOpt(t, collectionUpstream(
		namespaces.handler(),
		namespaceWatches(release, map[string][]string{"a": {podEvent("a")}, "b": {podEvent("b")}}),
	), withFanout, shortTTL)

	_, reader := startMergedWatch(t, h, podsPath+"?watch=true")
	if got := nextEvent(t, reader); got != podEvent("a") {
		t.Fatalf("event = %q, want %q", got, podEvent("a"))
	}

	namespaces.set(steveNamespace{name: "a"}, steveNamespace{name: "b"})
	h.clock.advance(time.Minute)
	wantCleanEnd(t, reader, "the stream stayed open after the allowed set changed")
}

// TestMergedWatchStaysOpenWithTheSameAllowedSet checks that the ticker
// re-reads the allowed set, and that it keeps the stream open while that set
// is the same.
func TestMergedWatchStaysOpenWithTheSameAllowedSet(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	namespaces := newNamespaceSet(steveNamespace{name: "a"})
	h := newHarnessOpt(t, collectionUpstream(
		namespaces.handler(),
		namespaceWatches(release, map[string][]string{"a": {podEvent("a")}}),
	), withFanout, shortTTL)

	_, reader := startMergedWatch(t, h, podsPath+"?watch=true")
	if got := nextEvent(t, reader); got != podEvent("a") {
		t.Fatalf("event = %q, want %q", got, podEvent("a"))
	}

	before := h.upstream.countPath(stevePath)
	ended := streamEnd(reader)
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

// openWatchCount returns the value of drover.filter.watches.open.
func openWatchCount(t *testing.T, reader *sdkmetric.ManualReader) int64 {
	t.Helper()
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	var total int64
	for _, point := range findSum(t, data, "drover.filter.watches.open").DataPoints {
		total += point.Value
	}
	return total
}

// TestDrainEndsMergedWatch checks that a drain ends the merged stream with a
// plain end of the stream, and that the open-watch metric returns to zero.
func TestDrainEndsMergedWatch(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	metricReader := sdkmetric.NewManualReader()
	h := newHarnessOpt(t, collectionUpstream(
		steveHandler("a"),
		namespaceWatches(release, map[string][]string{"a": {podEvent("a")}}),
	), withFanout, func(cfg *Config) {
		cfg.MeterProvider = sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
	})

	_, reader := startMergedWatch(t, h, podsPath+"?watch=true")
	if got := nextEvent(t, reader); got != podEvent("a") {
		t.Fatalf("event = %q, want %q", got, podEvent("a"))
	}
	if got := openWatchCount(t, metricReader); got != 1 {
		t.Errorf("drover.filter.watches.open = %d, want 1", got)
	}

	if got := h.svc.StartDrain(); got != 1 {
		t.Errorf("StartDrain = %d, want 1", got)
	}
	wantCleanEnd(t, reader, "the merged stream stayed open after the drain")

	if !waitFor(func() bool { return openWatchCount(t, metricReader) == 0 }) {
		t.Errorf("drover.filter.watches.open = %d, want 0 after the drain", openWatchCount(t, metricReader))
	}
}

// TestMergedWatchIsCappedByMaxWatches checks that the MaxWatches bound also
// caps a merged cluster-wide watch, because it shares the same registry as a
// namespace watch.
func TestMergedWatchIsCappedByMaxWatches(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	h := newHarnessOpt(t, collectionUpstream(
		steveHandler("a"),
		namespaceWatches(release, map[string][]string{"a": {podEvent("a")}}),
	), withFanout, func(cfg *Config) { cfg.MaxWatches = 1 })

	startMergedWatch(t, h, podsPath+"?watch=true")
	if got := h.svc.watches.len(); got != 1 {
		t.Fatalf("watches open = %d, want 1", got)
	}

	resp, _ := h.do(t, h.request(t, http.MethodGet, podsPath+"?watch=true", nil, callerHeader()))
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
}

// TestMaxWatchesPerCallerCapsAMergedWatch checks that a merged watch of a
// caller at MaxWatchesPerCaller answers 503 with the per-caller message, and
// that drover.filter.watches.rejected names the caller limit. A second caller
// still opens a merged watch.
func TestMaxWatchesPerCallerCapsAMergedWatch(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	metricReader := sdkmetric.NewManualReader()
	h := newHarnessOpt(t, collectionUpstream(
		steveHandler("a"),
		namespaceWatches(release, map[string][]string{"a": {podEvent("a")}}),
	), withFanout, recordingMeter(metricReader), func(cfg *Config) { cfg.MaxWatchesPerCaller = 1 })

	startMergedWatch(t, h, podsPath+"?watch=true")
	resp, body := h.do(t, h.request(t, http.MethodGet, podsPath+"?watch=true", nil, callerHeader()))
	wantWatchCapped(t, resp, body, "per-caller limit of 1")
	wantRejected(t, metricReader, limitCaller, 1)

	if got := watchStatus(t, h, podsPath+"?watch=true", otherCallerHeader()); got != http.StatusOK {
		t.Errorf("merged watch status of a second caller = %d, want 200", got)
	}
}

// TestMergedWatchReleasesTheSlotOnAFailure checks that a merged watch whose
// upstream watches all fail after the reserve frees its slot, and that the
// caller keeps the native answer. The caller has one slot, so a leaked slot
// answers the next watch with 503.
func TestMergedWatchReleasesTheSlotOnAFailure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		fail http.HandlerFunc
	}{
		{"upstream error status", errorStatus},
		{"closed connection", closeConnection},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			release := make(chan struct{})
			defer close(release)

			var failing atomic.Bool
			failing.Store(true)
			watches := namespaceWatches(release, map[string][]string{"a": {podEvent("a")}})
			h := newHarnessOpt(t, collectionUpstream(steveHandler("a"), func(w http.ResponseWriter, r *http.Request) {
				if failing.Load() {
					test.fail(w, r)
					return
				}
				watches(w, r)
			}), withFanout, func(cfg *Config) {
				cfg.MaxWatches = 1
				cfg.MaxWatchesPerCaller = 1
			})

			resp, body := h.do(t, h.request(t, http.MethodGet, podsPath+"?watch=true", nil, callerHeader()))
			if resp.StatusCode != http.StatusForbidden || string(body) != nativeForbidden {
				t.Fatalf("failed merged watch = %d %q, want the native answer", resp.StatusCode, body)
			}
			if got := h.svc.watches.reservedCount(); got != 0 {
				t.Errorf("reserved slots after the failure = %d, want 0", got)
			}

			failing.Store(false)
			_, reader := startMergedWatch(t, h, podsPath+"?watch=true")
			if got := nextEvent(t, reader); got != podEvent("a") {
				t.Errorf("event = %q, want %q", got, podEvent("a"))
			}
		})
	}
}

// endBookmark is the BOOKMARK event that ends the initial events of one
// upstream watch-list.
func endBookmark(version string) string {
	return `{"type":"BOOKMARK","object":{"kind":"Pod","apiVersion":"v1","metadata":{"resourceVersion":"` + version +
		`","annotations":{"k8s.io/initial-events-end":"true"}}}}` + "\n"
}

// decodeBookmark reads the fields of a merged end bookmark that a test checks.
func decodeBookmark(t *testing.T, line string) (kind, apiVersion, version string, annotations map[string]string) {
	t.Helper()
	var event struct {
		Type   string `json:"type"`
		Object struct {
			Kind       string `json:"kind"`
			APIVersion string `json:"apiVersion"`
			Metadata   struct {
				ResourceVersion string            `json:"resourceVersion"`
				Annotations     map[string]string `json:"annotations"`
			} `json:"metadata"`
		} `json:"object"`
	}
	if err := json.Unmarshal([]byte(line), &event); err != nil {
		t.Fatalf("decode %q: %v", line, err)
	}
	if event.Type != watchBookmark {
		t.Fatalf("event %q has the type %q, want BOOKMARK", line, event.Type)
	}
	return event.Object.Kind, event.Object.APIVersion, event.Object.Metadata.ResourceVersion, event.Object.Metadata.Annotations
}

const watchListQuery = "?watch=true&sendInitialEvents=true&resourceVersionMatch=NotOlderThan&allowWatchBookmarks=true"

// TestMergedWatchListEndsTheInitialEventsOnce checks that a watch-list gets
// the initial events of every namespace, then one end bookmark with the lowest
// resourceVersion, and that a plain bookmark still stays out.
func TestMergedWatchListEndsTheInitialEventsOnce(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	h := newHarnessOpt(t, collectionUpstream(
		steveHandler("a", "b"),
		namespaceWatches(release, map[string][]string{
			"a": {podEvent("a"), bookmarkLine, endBookmark("10")},
			"b": {podEvent("b"), endBookmark("7")},
		}),
	), withFanout)

	_, reader := startMergedWatch(t, h, podsPath+watchListQuery)
	added := []string{nextEvent(t, reader), nextEvent(t, reader)}
	slices.Sort(added)
	if want := []string{podEvent("a"), podEvent("b")}; !slices.Equal(added, want) {
		t.Errorf("initial events = %q, want %q", added, want)
	}
	kind, apiVersion, version, annotations := decodeBookmark(t, nextEvent(t, reader))
	if kind != "Pod" || apiVersion != "v1" || version != "7" || annotations[initialEventsEndAnnotation] != "true" {
		t.Errorf("end bookmark = %s %s %q %v, want Pod v1 7 with the end annotation", kind, apiVersion, version, annotations)
	}
	wantNoEvent(t, reader)

	for _, record := range namespacedRecords(h.upstream) {
		if record.query.Get("allowWatchBookmarks") != "true" || record.query.Get("sendInitialEvents") != "true" {
			t.Errorf("upstream query of %s = %v, want allowWatchBookmarks and sendInitialEvents", record.path, record.query)
		}
	}
}

// TestMergedWatchListWithoutAnAllowedNamespaceEndsAtOnce checks that a caller
// with no namespace gets the end bookmark from discovery, so a watch-list
// client does not wait.
func TestMergedWatchListWithoutAnAllowedNamespaceEndsAtOnce(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	h := newHarnessOpt(t, collectionUpstreamWithDiscovery(
		steveHandler(), namespaceWatches(release, nil), coreDiscovery(),
	), withFanout)

	_, reader := startMergedWatch(t, h, podsPath+watchListQuery)
	kind, apiVersion, version, annotations := decodeBookmark(t, nextEvent(t, reader))
	if kind != "Pod" || apiVersion != "v1" || version != "" || annotations[initialEventsEndAnnotation] != "true" {
		t.Errorf("end bookmark = %s %s %q %v, want Pod v1 with no version and the end annotation", kind, apiVersion, version, annotations)
	}
	wantNoEvent(t, reader)
}

// TestMergedWatchDropsAnEndBookmarkWithoutAWatchList checks that the end
// bookmark of an upstream stays out of a plain watch.
func TestMergedWatchDropsAnEndBookmarkWithoutAWatchList(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	defer close(release)

	h := newHarnessOpt(t, collectionUpstream(
		steveHandler("a"),
		namespaceWatches(release, map[string][]string{"a": {endBookmark("10"), podEvent("a")}}),
	), withFanout)

	_, reader := startMergedWatch(t, h, podsPath+"?watch=true")
	if got := nextEvent(t, reader); got != podEvent("a") {
		t.Errorf("event = %q, want the ADDED event", got)
	}
	wantNoEvent(t, reader)
}
