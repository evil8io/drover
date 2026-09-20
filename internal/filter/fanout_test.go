package filter

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	podsPath        = "/k8s/clusters/c-1/api/v1/pods"
	deploymentsPath = "/k8s/clusters/c-1/apis/apps/v1/deployments"
	nodesPath       = "/k8s/clusters/c-1/api/v1/nodes"

	// nativeForbidden is the answer of Rancher for a cluster-wide list that
	// the caller may not do at the cluster scope.
	nativeForbidden = `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden",` +
		`"message":"the cluster scope denies the caller","code":403}`

	// namespacedForbidden separates the answer of one namespace from the
	// native answer, so a test sees which one the caller got.
	namespacedForbidden = `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden",` +
		`"message":"the namespace denies the caller","code":403}`

	tableAccept = "application/json;as=Table;v=v1;g=meta.k8s.io"
)

// withFanout turns the fan-out on, for newHarnessOpt.
func withFanout(cfg *Config) { cfg.Fanout = true }

// splitNamespaced returns the namespace and the resource of a namespaced
// collection path, for example
// /k8s/clusters/c-1/apis/apps/v1/namespaces/a/deployments.
func splitNamespaced(path string) (namespace, resource string, ok bool) {
	parts := strings.Split(path, "/")
	for i, part := range parts {
		if part == "namespaces" && i+2 == len(parts)-1 {
			return parts[i+1], parts[i+2], true
		}
	}
	return "", "", false
}

// collectionUpstream routes the requests of a cluster-wide collection GET.
// The Steve path, the project path and the identity path fill the allowed
// set. A namespaced collection path goes to namespaced. Every other path is
// the native request, which Rancher denies.
func collectionUpstream(steve, namespaced http.HandlerFunc) http.HandlerFunc {
	projects := projectsHandler()
	identity := selfSubjectReviewHandler(callerUsername)
	return func(w http.ResponseWriter, r *http.Request) {
		_, _, isNamespaced := splitNamespaced(r.URL.Path)
		switch {
		case r.URL.Path == stevePath:
			steve(w, r)
		case r.URL.Path == projectsPath:
			projects(w, r)
		case r.URL.Path == selfSubjectReviewPath:
			identity(w, r)
		case isNamespaced:
			namespaced(w, r)
		default:
			writeForbidden(w, nativeForbidden)
		}
	}
}

// namespaceLists answers a namespaced collection with the body that bodies
// holds for the namespace. A namespace without a body answers 403, which is
// what a namespace gives where the caller may not list.
func namespaceLists(bodies map[string]string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		namespace, _, _ := splitNamespaced(r.URL.Path)
		body, ok := bodies[namespace]
		if !ok {
			writeForbidden(w, namespacedForbidden)
			return
		}
		w.Header().Set("Content-Type", jsonContentType)
		_, _ = io.WriteString(w, body)
	}
}

func writeForbidden(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", jsonContentType)
	w.WriteHeader(http.StatusForbidden)
	_, _ = io.WriteString(w, body)
}

// collectionJSON is a List of kind with one element in the namespace.
func collectionJSON(kind, apiVersion, namespace, name, resourceVersion string) string {
	return fmt.Sprintf(`{"kind":%q,"apiVersion":%q,"metadata":{"resourceVersion":%q},`+
		`"items":[{"metadata":{"name":%q,"namespace":%q}}]}`,
		kind, apiVersion, resourceVersion, name, namespace)
}

// tableJSON is a Table with two columns and one row.
func tableJSON(cell, resourceVersion string) string {
	return fmt.Sprintf(`{"kind":"Table","apiVersion":"meta.k8s.io/v1",`+
		`"metadata":{"resourceVersion":%q},`+
		`"columnDefinitions":[{"name":"Name","type":"string"},{"name":"Age","type":"string"}],`+
		`"rows":[{"cells":[%q,"1d"]}]}`, resourceVersion, cell)
}

// namespacedRecords returns the recorded namespaced collection requests,
// sorted by path. The fan-out runs the requests at the same time, so the
// recorded order is not the dispatch order.
func namespacedRecords(up *upstream) []recorded {
	var records []recorded
	for _, request := range up.all() {
		if _, _, ok := splitNamespaced(request.path); ok {
			records = append(records, request)
		}
	}
	slices.SortFunc(records, func(a, b recorded) int { return strings.Compare(a.path, b.path) })
	return records
}

func namespacedPaths(up *upstream) []string {
	records := namespacedRecords(up)
	paths := make([]string, 0, len(records))
	for _, request := range records {
		paths = append(paths, request.path)
	}
	return paths
}

// mergedList is the part of a merged List that the tests read.
type mergedList struct {
	Kind       string `json:"kind"`
	APIVersion string `json:"apiVersion"`
	Metadata   struct {
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
	Items []struct {
		Metadata struct {
			Name      string `json:"name"`
			Namespace string `json:"namespace"`
		} `json:"metadata"`
	} `json:"items"`
}

func parseList(t *testing.T, body []byte) mergedList {
	t.Helper()
	var list mergedList
	if err := json.Unmarshal(body, &list); err != nil {
		t.Fatalf("parse the merged body %q: %v", body, err)
	}
	return list
}

func (l mergedList) names() []string {
	names := make([]string, 0, len(l.Items))
	for _, item := range l.Items {
		names = append(names, item.Metadata.Name)
	}
	return names
}

func (l mergedList) namespaces() []string {
	namespaces := make([]string, 0, len(l.Items))
	for _, item := range l.Items {
		namespaces = append(namespaces, item.Metadata.Namespace)
	}
	return namespaces
}

// mergedTable is the part of a merged Table that the tests read.
type mergedTable struct {
	Kind              string `json:"kind"`
	APIVersion        string `json:"apiVersion"`
	ColumnDefinitions []struct {
		Name string `json:"name"`
	} `json:"columnDefinitions"`
	Rows []struct {
		Cells []any `json:"cells"`
	} `json:"rows"`
}

func TestCollectionWithoutFanoutPassesThrough(t *testing.T) {
	t.Parallel()
	h := newHarness(t, collectionUpstream(
		steveHandler("a", "b"),
		namespaceLists(map[string]string{
			"a": collectionJSON("PodList", "v1", "a", "pod-a", "5"),
			"b": collectionJSON("PodList", "v1", "b", "pod-b", "12"),
		}),
	))

	resp, body := h.do(t, h.request(t, http.MethodGet, podsPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}
	if string(body) != nativeForbidden {
		t.Errorf("body = %q, want the native answer", body)
	}

	requests := h.upstream.all()
	if len(requests) != 1 {
		t.Fatalf("upstream requests = %d, want 1", len(requests))
	}
	if requests[0].path != podsPath {
		t.Errorf("upstream path = %q, want %q", requests[0].path, podsPath)
	}
}

func TestCollectionPathsThatPassThrough(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		method string
		path   string
	}{
		{"core discovery", http.MethodGet, "/k8s/clusters/c-1/api/v1"},
		{"group discovery", http.MethodGet, "/k8s/clusters/c-1/apis"},
		{"group version discovery", http.MethodGet, "/k8s/clusters/c-1/apis/apps/v1"},
		{"namespaced collection", http.MethodGet, "/k8s/clusters/c-1/api/v1/namespaces/a/pods"},
		{"single object", http.MethodGet, "/k8s/clusters/c-1/api/v1/namespaces/a/pods/p"},
		{"post", http.MethodPost, podsPath},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			h := newHarnessOpt(t, func(w http.ResponseWriter, r *http.Request) {
				writeForbidden(w, nativeForbidden)
			}, withFanout)

			var body io.Reader
			if test.method == http.MethodPost {
				body = strings.NewReader(`{}`)
			}
			resp, answer := h.do(t, h.request(t, test.method, test.path, body, callerHeader()))
			if resp.StatusCode != http.StatusForbidden {
				t.Errorf("status = %d, want 403", resp.StatusCode)
			}
			if string(answer) != nativeForbidden {
				t.Errorf("body = %q, want the native answer", answer)
			}

			requests := h.upstream.all()
			if len(requests) != 1 {
				t.Fatalf("upstream requests = %d, want 1", len(requests))
			}
			if requests[0].method != test.method || requests[0].path != test.path {
				t.Errorf("upstream request = %s %q, want %s %q",
					requests[0].method, requests[0].path, test.method, test.path)
			}
		})
	}
}

func TestCollectionMergesCoreList(t *testing.T) {
	t.Parallel()
	h := newHarnessOpt(t, collectionUpstream(
		steveHandler("b", "a"),
		namespaceLists(map[string]string{
			"a": collectionJSON("PodList", "v1", "a", "pod-a", "5"),
			"b": collectionJSON("PodList", "v1", "b", "pod-b", "12"),
		}),
	), withFanout)

	resp, body := h.do(t, h.request(t, http.MethodGet, podsPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %q", resp.StatusCode, body)
	}

	list := parseList(t, body)
	if list.Kind != "PodList" {
		t.Errorf("kind = %q, want PodList", list.Kind)
	}
	if list.APIVersion != "v1" {
		t.Errorf("apiVersion = %q, want v1", list.APIVersion)
	}
	if want := []string{"pod-a", "pod-b"}; !slices.Equal(list.names(), want) {
		t.Errorf("items = %v, want %v", list.names(), want)
	}
	if list.Metadata.ResourceVersion != "12" {
		t.Errorf("resourceVersion = %q, want 12", list.Metadata.ResourceVersion)
	}

	// The resourceVersion of the merge is the highest of the answers, which
	// the stream knows at the end only, so the metadata stands last.
	merged := string(body)
	if last, meta := strings.LastIndex(merged, `"pod-b"`), strings.LastIndex(merged, `"metadata"`); meta < last {
		t.Errorf("the metadata stands before the last element: %q", merged)
	}

	want := []string{
		"/k8s/clusters/c-1/api/v1/namespaces/a/pods",
		"/k8s/clusters/c-1/api/v1/namespaces/b/pods",
	}
	if got := namespacedPaths(h.upstream); !slices.Equal(got, want) {
		t.Errorf("namespaced paths = %v, want %v", got, want)
	}
}

func TestCollectionMergesGroupList(t *testing.T) {
	t.Parallel()
	h := newHarnessOpt(t, collectionUpstream(
		steveHandler("a", "b"),
		namespaceLists(map[string]string{
			"a": collectionJSON("DeploymentList", "apps/v1", "a", "deploy-a", "9"),
			"b": collectionJSON("DeploymentList", "apps/v1", "b", "deploy-b", "4"),
		}),
	), withFanout)

	resp, body := h.do(t, h.request(t, http.MethodGet, deploymentsPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %q", resp.StatusCode, body)
	}

	list := parseList(t, body)
	if list.Kind != "DeploymentList" || list.APIVersion != "apps/v1" {
		t.Errorf("kind = %q and apiVersion = %q, want DeploymentList and apps/v1", list.Kind, list.APIVersion)
	}
	if want := []string{"deploy-a", "deploy-b"}; !slices.Equal(list.names(), want) {
		t.Errorf("items = %v, want %v", list.names(), want)
	}
	if list.Metadata.ResourceVersion != "9" {
		t.Errorf("resourceVersion = %q, want 9", list.Metadata.ResourceVersion)
	}

	want := []string{
		"/k8s/clusters/c-1/apis/apps/v1/namespaces/a/deployments",
		"/k8s/clusters/c-1/apis/apps/v1/namespaces/b/deployments",
	}
	if got := namespacedPaths(h.upstream); !slices.Equal(got, want) {
		t.Errorf("namespaced paths = %v, want %v", got, want)
	}
}

func TestCollectionMergesTable(t *testing.T) {
	t.Parallel()
	h := newHarnessOpt(t, collectionUpstream(
		steveHandler("a", "b"),
		namespaceLists(map[string]string{
			"a": tableJSON("pod-a", "3"),
			"b": tableJSON("pod-b", "8"),
		}),
	), withFanout)

	header := callerHeader()
	header.Set("Accept", tableAccept)
	resp, body := h.do(t, h.request(t, http.MethodGet, podsPath, nil, header))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %q", resp.StatusCode, body)
	}

	var table mergedTable
	if err := json.Unmarshal(body, &table); err != nil {
		t.Fatalf("parse the merged body %q: %v", body, err)
	}
	if table.Kind != "Table" || table.APIVersion != "meta.k8s.io/v1" {
		t.Errorf("kind = %q and apiVersion = %q, want Table and meta.k8s.io/v1", table.Kind, table.APIVersion)
	}
	if len(table.ColumnDefinitions) != 2 {
		t.Errorf("columnDefinitions = %d, want the 2 of the first answer", len(table.ColumnDefinitions))
	}
	if len(table.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(table.Rows))
	}
	for i, want := range []string{"pod-a", "pod-b"} {
		if len(table.Rows[i].Cells) == 0 || table.Rows[i].Cells[0] != want {
			t.Errorf("row %d = %v, want the first cell %q", i, table.Rows[i].Cells, want)
		}
	}
}

func TestCollectionSkipsForbiddenNamespace(t *testing.T) {
	t.Parallel()
	h := newHarnessOpt(t, collectionUpstream(
		steveHandler("a", "b"),
		namespaceLists(map[string]string{
			"b": collectionJSON("PodList", "v1", "b", "pod-b", "12"),
		}),
	), withFanout)

	resp, body := h.do(t, h.request(t, http.MethodGet, podsPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %q", resp.StatusCode, body)
	}

	list := parseList(t, body)
	if want := []string{"pod-b"}; !slices.Equal(list.names(), want) {
		t.Errorf("items = %v, want %v", list.names(), want)
	}
	if list.Metadata.ResourceVersion != "12" {
		t.Errorf("resourceVersion = %q, want 12", list.Metadata.ResourceVersion)
	}
	if got := len(namespacedRecords(h.upstream)); got != 2 {
		t.Errorf("namespaced requests = %d, want 2", got)
	}
}

func TestCollectionWithEveryNamespaceForbiddenKeepsNativeWhenDiscoveryFails(t *testing.T) {
	t.Parallel()
	h := newHarnessOpt(t, collectionUpstream(
		steveHandler("a", "b"),
		namespaceLists(nil),
	), withFanout)

	resp, body := h.do(t, h.request(t, http.MethodGet, nodesPath, nil, callerHeader()))
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

func TestCollectionWithoutAnAllowedNamespaceMakesNoNamespacedRequest(t *testing.T) {
	t.Parallel()
	h := newHarnessOpt(t, collectionUpstream(
		steveHandler(),
		namespaceLists(nil),
	), withFanout)

	resp, body := h.do(t, h.request(t, http.MethodGet, podsPath, nil, callerHeader()))
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

func TestCollectionAboveTheCapIsForbidden(t *testing.T) {
	t.Parallel()
	h := newHarnessOpt(t, collectionUpstream(
		steveHandler("a", "b"),
		namespaceLists(map[string]string{
			"a": collectionJSON("PodList", "v1", "a", "pod-a", "5"),
			"b": collectionJSON("PodList", "v1", "b", "pod-b", "12"),
		}),
	), withFanout, func(cfg *Config) { cfg.FanoutMaxNamespaces = 1 })

	resp, body := h.do(t, h.request(t, http.MethodGet, podsPath, nil, callerHeader()))
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

func TestCollectionWatchReturnsNative(t *testing.T) {
	t.Parallel()
	h := newHarnessOpt(t, collectionUpstream(
		steveHandler("a", "b"),
		namespaceLists(map[string]string{
			"a": collectionJSON("PodList", "v1", "a", "pod-a", "5"),
		}),
	), withFanout)

	resp, body := h.do(t, h.request(t, http.MethodGet, podsPath+"?watch=true", nil, callerHeader()))
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}
	if string(body) != nativeForbidden {
		t.Errorf("body = %q, want the native answer", body)
	}
	if h.upstream.count() != 1 {
		t.Errorf("upstream requests = %d, want the native request only", h.upstream.count())
	}
}

func TestCollectionNamespacedRequestKeepsCallerCredentials(t *testing.T) {
	t.Parallel()
	h := newHarnessOpt(t, collectionUpstream(
		steveHandler("a", "b"),
		namespaceLists(map[string]string{
			"a": collectionJSON("PodList", "v1", "a", "pod-a", "5"),
			"b": collectionJSON("PodList", "v1", "b", "pod-b", "12"),
		}),
	), withFanout)

	header := callerHeader()
	header.Set("Accept", "application/vnd.kubernetes.protobuf,application/json")
	target := podsPath + "?limit=500&continue=tok&labelSelector=team%3Dx"
	resp, body := h.do(t, h.request(t, http.MethodGet, target, nil, header))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %q", resp.StatusCode, body)
	}

	records := namespacedRecords(h.upstream)
	if len(records) != 2 {
		t.Fatalf("namespaced requests = %d, want 2", len(records))
	}
	for _, request := range records {
		if request.header.Get("Authorization") != callerToken {
			t.Errorf("%s Authorization = %q, want %q", request.path, request.header.Get("Authorization"), callerToken)
		}
		if request.query.Has("limit") || request.query.Has("continue") {
			t.Errorf("%s query = %v, want no limit and no continue", request.path, request.query)
		}
		if got := request.query.Get("labelSelector"); got != "team=x" {
			t.Errorf("%s labelSelector = %q, want team=x", request.path, got)
		}
		if got := request.header.Get("Accept"); got != jsonContentType {
			t.Errorf("%s Accept = %q, want %q", request.path, got, jsonContentType)
		}
	}
}

func TestCollectionMergesEmptyLists(t *testing.T) {
	t.Parallel()
	empty := `{"kind":"PodList","apiVersion":"v1","metadata":{"resourceVersion":"7"},"items":[]}`
	h := newHarnessOpt(t, collectionUpstream(
		steveHandler("a", "b"),
		namespaceLists(map[string]string{"a": empty, "b": empty}),
	), withFanout)

	resp, body := h.do(t, h.request(t, http.MethodGet, podsPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %q", resp.StatusCode, body)
	}

	var object map[string]any
	if err := json.Unmarshal(body, &object); err != nil {
		t.Fatalf("parse the merged body %q: %v", body, err)
	}
	items, ok := object["items"].([]any)
	if !ok {
		t.Fatalf("items = %v, want an array: %q", object["items"], body)
	}
	if len(items) != 0 {
		t.Errorf("items = %v, want no element", items)
	}
	if list := parseList(t, body); list.Metadata.ResourceVersion != "7" {
		t.Errorf("resourceVersion = %q, want 7", list.Metadata.ResourceVersion)
	}
}

func TestCollectionImpersonationPassesThrough(t *testing.T) {
	t.Parallel()
	h := newHarnessOpt(t, collectionUpstream(
		steveHandler("a", "b"),
		namespaceLists(map[string]string{
			"a": collectionJSON("PodList", "v1", "a", "pod-a", "5"),
		}),
	), withFanout)

	header := callerHeader()
	header.Set("Impersonate-User", "alice")
	resp, body := h.do(t, h.request(t, http.MethodGet, podsPath, nil, header))
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}
	if string(body) != nativeForbidden {
		t.Errorf("body = %q, want the native answer", body)
	}
	if h.upstream.count() != 1 {
		t.Errorf("upstream requests = %d, want the native request only", h.upstream.count())
	}
}

// wave holds the first n namespaced requests until all n of them run, and it
// records the highest count of requests that run at the same time. It
// releases a held request after a deadline, so a fan-out that runs the
// requests one by one fails the test instead of blocking it.
type wave struct {
	mu       sync.Mutex
	arrived  int
	inFlight int
	highest  int
	n        int
	release  chan struct{}
}

func newWave(n int) *wave {
	return &wave{n: n, release: make(chan struct{})}
}

func (g *wave) enter() {
	g.mu.Lock()
	g.arrived++
	g.inFlight++
	g.highest = max(g.highest, g.inFlight)
	arrived := g.arrived
	g.mu.Unlock()

	if arrived > g.n {
		return
	}
	if arrived == g.n {
		close(g.release)
	}
	select {
	case <-g.release:
	case <-time.After(2 * time.Second):
	}
}

func (g *wave) leave() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.inFlight--
}

func (g *wave) highestInFlight() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.highest
}

func TestCollectionFansOutConcurrently(t *testing.T) {
	t.Parallel()
	const concurrency = 3

	names := make([]string, 0, 12)
	bodies := make(map[string]string, 12)
	wantNames := make([]string, 0, 12)
	wantPaths := make([]string, 0, 12)
	for i := 1; i <= 12; i++ {
		namespace := fmt.Sprintf("ns-%02d", i)
		pod := "pod-" + namespace
		names = append(names, namespace)
		bodies[namespace] = collectionJSON("PodList", "v1", namespace, pod, fmt.Sprintf("%d", 100+i))
		wantNames = append(wantNames, pod)
		wantPaths = append(wantPaths, "/k8s/clusters/c-1/api/v1/namespaces/"+namespace+"/pods")
	}

	gate := newWave(concurrency)
	lists := namespaceLists(bodies)
	h := newHarnessOpt(t, collectionUpstream(
		steveHandler(names...),
		func(w http.ResponseWriter, r *http.Request) {
			gate.enter()
			defer gate.leave()
			lists(w, r)
		},
	), withFanout, func(cfg *Config) { cfg.FanoutConcurrency = concurrency })

	resp, body := h.do(t, h.request(t, http.MethodGet, podsPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %q", resp.StatusCode, body)
	}

	if got := namespacedPaths(h.upstream); !slices.Equal(got, wantPaths) {
		t.Errorf("namespaced paths = %v, want each namespace once: %v", got, wantPaths)
	}
	list := parseList(t, body)
	if !slices.Equal(list.names(), wantNames) {
		t.Errorf("items = %v, want %v", list.names(), wantNames)
	}
	if !slices.Equal(list.namespaces(), names) {
		t.Errorf("item namespaces = %v, want %v", list.namespaces(), names)
	}
	if list.Metadata.ResourceVersion != "112" {
		t.Errorf("resourceVersion = %q, want 112", list.Metadata.ResourceVersion)
	}
	if got := gate.highestInFlight(); got != concurrency {
		t.Errorf("namespaced requests at the same time = %d, want %d", got, concurrency)
	}
}

func TestMaxResourceVersion(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		a    string
		b    string
		want string
	}{
		{"left higher", "12", "5", "12"},
		{"right higher", "5", "12", "12"},
		{"equal", "7", "7", "7"},
		{"wide", "18446744073709551615", "2", "18446744073709551615"},
		{"empty left", "", "7", "7"},
		{"empty right", "7", "", "7"},
		{"both empty", "", "", ""},
		{"non-numeric left", "abc", "7", "7"},
		{"non-numeric right", "7", "abc", "7"},
		{"both non-numeric", "abc", "def", "abc"},
		{"non-numeric and empty", "", "abc", "abc"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := maxResourceVersion(test.a, test.b); got != test.want {
				t.Errorf("maxResourceVersion(%q, %q) = %q, want %q", test.a, test.b, got, test.want)
			}
		})
	}
}

func TestReviewGrantsCollectionList(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		fanout     bool
		attributes string
		want       bool
	}{
		{"cluster-wide list with the fan-out", true, `"verb":"list","resource":"pods"`, true},
		{"cluster-wide list without the fan-out", false, `"verb":"list","resource":"pods"`, false},
		{"namespaced list with the fan-out", true, `"verb":"list","resource":"pods","namespace":"a"`, false},
		{"namespaced list without the fan-out", false, `"verb":"list","resource":"pods","namespace":"a"`, false},
		{"get with the fan-out", true, `"verb":"get","resource":"pods"`, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			h := newHarnessOpt(t, reviewUpstreamWithNames(deniedAnswer, "prod"),
				func(cfg *Config) { cfg.Fanout = test.fanout })

			request := fmt.Sprintf(reviewTemplate, test.attributes)
			resp, body := h.postReview(t, []byte(request), nil)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}

			var object map[string]any
			if err := json.Unmarshal(body, &object); err != nil {
				t.Fatalf("parse the response %q: %v", body, err)
			}
			status, ok := object["status"].(map[string]any)
			if !ok {
				t.Fatalf("the response has no status object: %q", body)
			}
			if status["allowed"] != test.want {
				t.Errorf("allowed = %v, want %v", status["allowed"], test.want)
			}
		})
	}
}

// notFoundHandler answers every namespaced request with 404, which is what a
// namespaced path of a kind without a namespace scope gives.
func notFoundHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", jsonContentType)
	w.WriteHeader(http.StatusNotFound)
	_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`)
}

func TestCollectionStopsAfterANotFoundNamespace(t *testing.T) {
	t.Parallel()
	names := make([]string, 0, 12)
	for i := range 12 {
		names = append(names, fmt.Sprintf("ns-%02d", i))
	}
	concurrency := 2
	h := newHarnessOpt(t, collectionUpstream(steveHandler(names...), notFoundHandler),
		withFanout, func(cfg *Config) { cfg.FanoutConcurrency = concurrency })

	resp, body := h.do(t, h.request(t, http.MethodGet, nodesPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}
	if string(body) != nativeForbidden {
		t.Errorf("body = %q, want the native answer", body)
	}
	// One request can still start after the first 404 sets the stop, because
	// the dispatcher reads the stop before it takes a slot, not after.
	got := len(namespacedRecords(h.upstream))
	if got < 1 || got > concurrency+1 {
		t.Errorf("namespaced requests = %d, want 1 to %d of %d namespaces", got, concurrency+1, len(names))
	}
}

const (
	coreDiscoveryPath = "/k8s/clusters/c-1/api/v1"
	appsDiscoveryPath = "/k8s/clusters/c-1/apis/apps/v1"
)

// discoveryHandler answers the discovery document of the api path.
func discoveryHandler(groupVersion string, resources ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		entries := make([]string, 0, len(resources))
		for _, resource := range resources {
			parts := strings.Split(resource, ":")
			entries = append(entries, fmt.Sprintf(`{"name":%q,"kind":%q,"namespaced":%s}`, parts[0], parts[1], parts[2]))
		}
		w.Header().Set("Content-Type", jsonContentType)
		_, _ = io.WriteString(w, fmt.Sprintf(
			`{"kind":"APIResourceList","apiVersion":"v1","groupVersion":%q,"resources":[%s]}`,
			groupVersion, strings.Join(entries, ",")))
	}
}

// collectionUpstreamWithDiscovery is collectionUpstream with a discovery
// document per api path.
func collectionUpstreamWithDiscovery(steve, namespaced http.HandlerFunc, discovery map[string]http.HandlerFunc) http.HandlerFunc {
	base := collectionUpstream(steve, namespaced)
	return func(w http.ResponseWriter, r *http.Request) {
		if handler, ok := discovery[r.URL.Path]; ok {
			handler(w, r)
			return
		}
		base(w, r)
	}
}

func coreDiscovery() map[string]http.HandlerFunc {
	return map[string]http.HandlerFunc{
		coreDiscoveryPath: discoveryHandler("v1", "pods:Pod:true", "nodes:Node:false"),
		appsDiscoveryPath: discoveryHandler("apps/v1", "deployments:Deployment:true"),
	}
}

func TestCollectionWithoutAnAllowedNamespaceAnswersAnEmptyList(t *testing.T) {
	t.Parallel()
	h := newHarnessOpt(t, collectionUpstreamWithDiscovery(
		steveHandler(), namespaceLists(nil), coreDiscovery(),
	), withFanout)

	resp, body := h.do(t, h.request(t, http.MethodGet, podsPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", resp.StatusCode, body)
	}
	list := parseList(t, body)
	if list.Kind != "PodList" || list.APIVersion != "v1" {
		t.Errorf("kind = %q, apiVersion = %q, want PodList and v1", list.Kind, list.APIVersion)
	}
	if len(list.Items) != 0 {
		t.Errorf("items = %d, want 0", len(list.Items))
	}
	if !strings.Contains(string(body), `"items":[]`) {
		t.Errorf("body = %s, want an empty array, not null", body)
	}
	if got := len(namespacedRecords(h.upstream)); got != 0 {
		t.Errorf("namespaced requests = %d, want 0", got)
	}
}

func TestCollectionWithoutAnAllowedNamespaceAnswersAnEmptyGroupList(t *testing.T) {
	t.Parallel()
	h := newHarnessOpt(t, collectionUpstreamWithDiscovery(
		steveHandler(), namespaceLists(nil), coreDiscovery(),
	), withFanout)

	resp, body := h.do(t, h.request(t, http.MethodGet, deploymentsPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", resp.StatusCode, body)
	}
	list := parseList(t, body)
	if list.Kind != "DeploymentList" || list.APIVersion != "apps/v1" {
		t.Errorf("kind = %q, apiVersion = %q, want DeploymentList and apps/v1", list.Kind, list.APIVersion)
	}
}

func TestCollectionWithoutAnAllowedNamespaceKeepsNativeForAClusterScopedKind(t *testing.T) {
	t.Parallel()
	h := newHarnessOpt(t, collectionUpstreamWithDiscovery(
		steveHandler(), namespaceLists(nil), coreDiscovery(),
	), withFanout)

	resp, body := h.do(t, h.request(t, http.MethodGet, nodesPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}
	if string(body) != nativeForbidden {
		t.Errorf("body = %q, want the native answer", body)
	}
}

func TestCollectionWithoutAnAllowedNamespaceKeepsNativeWhenDiscoveryFails(t *testing.T) {
	t.Parallel()
	h := newHarnessOpt(t, collectionUpstream(steveHandler(), namespaceLists(nil)), withFanout)

	resp, body := h.do(t, h.request(t, http.MethodGet, podsPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}
	if string(body) != nativeForbidden {
		t.Errorf("body = %q, want the native answer", body)
	}
}

func TestCollectionWithEveryNamespaceForbiddenAnswersAnEmptyList(t *testing.T) {
	t.Parallel()
	h := newHarnessOpt(t, collectionUpstreamWithDiscovery(
		steveHandler("a", "b"), namespaceLists(nil), coreDiscovery(),
	), withFanout)

	resp, body := h.do(t, h.request(t, http.MethodGet, podsPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", resp.StatusCode, body)
	}
	list := parseList(t, body)
	if list.Kind != "PodList" || len(list.Items) != 0 {
		t.Errorf("kind = %q with %d items, want PodList with 0", list.Kind, len(list.Items))
	}
	if got := len(namespacedRecords(h.upstream)); got != 2 {
		t.Errorf("namespaced requests = %d, want 2", got)
	}
}

func TestCollectionWithANotFoundNamespaceKeepsNative(t *testing.T) {
	t.Parallel()
	h := newHarnessOpt(t, collectionUpstreamWithDiscovery(
		steveHandler("a", "b"), notFoundHandler, coreDiscovery(),
	), withFanout)

	resp, body := h.do(t, h.request(t, http.MethodGet, podsPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}
	if string(body) != nativeForbidden {
		t.Errorf("body = %q, want the native answer", body)
	}
}
