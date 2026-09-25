package filter

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestListNative200(t *testing.T) {
	t.Parallel()
	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == stevePath {
			t.Error("the upstream got an allowed set request")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"kind":"NamespaceList","items":[{"metadata":{"name":"a"}}]}`)
	})

	resp, body := h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if want := `{"kind":"NamespaceList","items":[{"metadata":{"name":"a"}}]}`; string(body) != want {
		t.Errorf("body = %q, want %q", body, want)
	}
	if h.upstream.count() != 1 {
		t.Errorf("upstream requests = %d, want 1", h.upstream.count())
	}
}

func TestListFiltersAfter403(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstream(steveHandler("b", "a"), namespaceListHandler))

	header := callerHeader()
	header.Set("Cookie", "R_SESS=x")
	resp, body := h.do(t, h.request(t, http.MethodGet, listPath+"?limit=500", nil, header))

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if resp.Header.Get("X-Source") != "privileged" {
		t.Errorf("X-Source = %q, want privileged", resp.Header.Get("X-Source"))
	}
	if want := `{"kind":"NamespaceList","items":[]}`; string(body) != want {
		t.Errorf("body = %q, want %q", body, want)
	}

	requests := h.upstream.all()
	if len(requests) != 5 {
		t.Fatalf("upstream requests = %d, want 5", len(requests))
	}

	native := requests[0]
	if native.path != listPath || native.header.Get("Authorization") != callerToken {
		t.Errorf("native request = %s %q with %q", native.method, native.path, native.header.Get("Authorization"))
	}
	if native.query.Get("labelSelector") != "" {
		t.Errorf("native labelSelector = %q, want no selector", native.query.Get("labelSelector"))
	}

	steve := requests[1]
	if steve.method != http.MethodGet || steve.path != stevePath {
		t.Errorf("allowed set request = %s %q, want GET %q", steve.method, steve.path, stevePath)
	}
	if steve.header.Get("Authorization") != callerToken {
		t.Errorf("allowed set Authorization = %q, want %q", steve.header.Get("Authorization"), callerToken)
	}
	if steve.header.Get("Accept") != "application/json" {
		t.Errorf("allowed set Accept = %q, want application/json", steve.header.Get("Accept"))
	}

	projects := requests[2]
	if projects.method != http.MethodGet || projects.path != projectsPath {
		t.Errorf("project list request = %s %q, want GET %q", projects.method, projects.path, projectsPath)
	}
	if projects.header.Get("Authorization") != callerToken {
		t.Errorf("project list Authorization = %q, want %q", projects.header.Get("Authorization"), callerToken)
	}

	identity := requests[3]
	if identity.method != http.MethodPost || identity.path != selfSubjectReviewPath {
		t.Errorf("caller identity request = %s %q, want POST %q", identity.method, identity.path, selfSubjectReviewPath)
	}
	if identity.header.Get("Authorization") != callerToken {
		t.Errorf("caller identity Authorization = %q, want %q", identity.header.Get("Authorization"), callerToken)
	}

	privileged := requests[4]
	if privileged.path != listPath {
		t.Errorf("privileged path = %q, want %q", privileged.path, listPath)
	}
	if privileged.header.Get("Authorization") != serviceAuth {
		t.Errorf("privileged Authorization = %q, want %q", privileged.header.Get("Authorization"), serviceAuth)
	}
	if _, ok := privileged.header["Cookie"]; ok {
		t.Error("the privileged request has a Cookie header")
	}
	if got, want := privileged.query.Get("labelSelector"), "kubernetes.io/metadata.name in (a,b)"; got != want {
		t.Errorf("labelSelector = %q, want %q", got, want)
	}
	if got := privileged.query.Get("limit"); got != "500" {
		t.Errorf("limit = %q, want 500", got)
	}
}

func TestListMergesCallerSelector(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstream(steveHandler("a", "b"), namespaceListHandler))

	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath+"?labelSelector=team%3Dx", nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	privileged := h.upstream.privileged(t)
	want := "team=x,kubernetes.io/metadata.name in (a,b)"
	if got := privileged.query.Get("labelSelector"); got != want {
		t.Errorf("labelSelector = %q, want %q", got, want)
	}
}

func TestListEmptySet(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstream(steveHandler(), namespaceListHandler))

	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	privileged := h.upstream.privileged(t)
	want := "kubernetes.io/metadata.name,!kubernetes.io/metadata.name"
	if got := privileged.query.Get("labelSelector"); got != want {
		t.Errorf("labelSelector = %q, want %q", got, want)
	}
}

func TestListProjectSelector(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstreamWithProjects(
		steveLabeledHandler(steveNamespace{name: "a", project: "p-1"}, steveNamespace{name: "b", project: "p-1"}),
		projectsHandler("p-1"),
		namespaceListHandler,
	))

	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	privileged := h.upstream.privileged(t)
	want := "field.cattle.io/projectId in (p-1)"
	if got := privileged.query.Get("labelSelector"); got != want {
		t.Errorf("labelSelector = %q, want %q", got, want)
	}
}

func TestListMergesCallerSelectorIntoProjectSelector(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstreamWithProjects(
		steveLabeledHandler(steveNamespace{name: "a", project: "p-1"}),
		projectsHandler("p-1"),
		namespaceListHandler,
	))

	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath+"?labelSelector=team%3Dx", nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	privileged := h.upstream.privileged(t)
	want := "team=x,field.cattle.io/projectId in (p-1)"
	if got := privileged.query.Get("labelSelector"); got != want {
		t.Errorf("labelSelector = %q, want %q", got, want)
	}
}

func TestListNameSelectorOnMismatchedProject(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstreamWithProjects(
		steveLabeledHandler(steveNamespace{name: "a", project: "p-1"}, steveNamespace{name: "z", project: "p-2"}),
		projectsHandler("p-1"),
		namespaceListHandler,
	))

	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	privileged := h.upstream.privileged(t)
	want := "kubernetes.io/metadata.name in (a,z)"
	if got := privileged.query.Get("labelSelector"); got != want {
		t.Errorf("labelSelector = %q, want %q", got, want)
	}
}

func TestListNameSelectorOnMissingProjectLabel(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstreamWithProjects(
		steveLabeledHandler(steveNamespace{name: "a", project: "p-1"}, steveNamespace{name: "z"}),
		projectsHandler("p-1"),
		namespaceListHandler,
	))

	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	privileged := h.upstream.privileged(t)
	want := "kubernetes.io/metadata.name in (a,z)"
	if got := privileged.query.Get("labelSelector"); got != want {
		t.Errorf("labelSelector = %q, want %q", got, want)
	}
}

// TestListProjectSelectorExcludesVisibleProjectWithoutNamespace checks that
// the list selector drops a project that /v3/projects shows, when the caller
// has no namespace of that project. Rancher grants project visibility and
// namespace access as separate rights, so p-b must not reach the selector.
func TestListProjectSelectorExcludesVisibleProjectWithoutNamespace(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstreamWithProjects(
		steveLabeledHandler(steveNamespace{name: "a", project: "p-a"}),
		projectsHandler("p-a", "p-b"),
		namespaceListHandler,
	))

	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	privileged := h.upstream.privileged(t)
	want := "field.cattle.io/projectId in (p-a)"
	if got := privileged.query.Get("labelSelector"); got != want {
		t.Errorf("labelSelector = %q, want %q", got, want)
	}
}

// TestWatchDropsEventOfVisibleProjectWithoutNamespace is
// TestListProjectSelectorExcludesVisibleProjectWithoutNamespace for a watch.
// It also checks that the event filter drops an event of p-b, the second
// gate for a namespace that the upstream selector already excludes.
func TestWatchDropsEventOfVisibleProjectWithoutNamespace(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstreamWithProjects(
		steveLabeledHandler(steveNamespace{name: "a", project: "p-a"}),
		projectsHandler("p-a", "p-b"),
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"type":"ADDED","object":{"metadata":{"name":"z","labels":{"field.cattle.io/projectId":"p-b"}}}}`+"\n")
		},
	))

	resp, body := h.do(t, h.request(t, http.MethodGet, listPath+"?watch=true", nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(body) != 0 {
		t.Errorf("body = %q, want no event", body)
	}

	privileged := h.upstream.privileged(t)
	want := "field.cattle.io/projectId in (p-a)"
	if got := privileged.query.Get("labelSelector"); got != want {
		t.Errorf("labelSelector = %q, want %q", got, want)
	}
}

// TestListMatchesNothingWithNoNamespaceAndVisibleProjects checks that a
// caller with no Steve namespace gets the selector that matches none, even
// when /v3/projects shows p-a and p-b. A visible project with no allowed
// namespace grants no access.
func TestListMatchesNothingWithNoNamespaceAndVisibleProjects(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstreamWithProjects(steveHandler(), projectsHandler("p-a", "p-b"), namespaceListHandler))

	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	privileged := h.upstream.privileged(t)
	want := "kubernetes.io/metadata.name,!kubernetes.io/metadata.name"
	if got := privileged.query.Get("labelSelector"); got != want {
		t.Errorf("labelSelector = %q, want %q", got, want)
	}
}

// TestWatchMatchesNothingWithNoNamespaceAndVisibleProjects is
// TestListMatchesNothingWithNoNamespaceAndVisibleProjects for a watch. See
// watchSelector: a caller with no name and no project gets the selector that
// matches none, and an empty allowedSet.projects keeps that case true.
func TestWatchMatchesNothingWithNoNamespaceAndVisibleProjects(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstreamWithProjects(steveHandler(), projectsHandler("p-a", "p-b"), func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
	}))

	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath+"?watch=true", nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	privileged := h.upstream.privileged(t)
	want := "kubernetes.io/metadata.name,!kubernetes.io/metadata.name"
	if got := privileged.query.Get("labelSelector"); got != want {
		t.Errorf("labelSelector = %q, want %q", got, want)
	}
}

// TestListProjectSelectorIncludesEveryProjectWithNamespace checks the
// positive case: the selector still names every visible project that holds
// an allowed namespace of the caller.
func TestListProjectSelectorIncludesEveryProjectWithNamespace(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstreamWithProjects(
		steveLabeledHandler(steveNamespace{name: "a", project: "p-a"}, steveNamespace{name: "b", project: "p-b"}),
		projectsHandler("p-a", "p-b"),
		namespaceListHandler,
	))

	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	privileged := h.upstream.privileged(t)
	want := "field.cattle.io/projectId in (p-a,p-b)"
	if got := privileged.query.Get("labelSelector"); got != want {
		t.Errorf("labelSelector = %q, want %q", got, want)
	}
}

func TestListImpersonationPassesThrough(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstream(steveHandler("a"), namespaceListHandler))

	header := callerHeader()
	header.Set("Impersonate-User", "someone")
	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, header))

	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}
	if h.upstream.count() != 1 {
		t.Errorf("upstream requests = %d, want 1", h.upstream.count())
	}
}

func TestListSteveDeniesCallerGetsNative403(t *testing.T) {
	t.Parallel()
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()
			h := newHarness(t, listUpstream(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				_, _ = io.WriteString(w, `{"type":"error","code":"Unauthorized"}`)
			}, namespaceListHandler))

			resp, body := h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
			if resp.StatusCode != http.StatusForbidden {
				t.Errorf("status = %d, want 403", resp.StatusCode)
			}
			if want := "forbidden\n"; string(body) != want {
				t.Errorf("body = %q, want the native answer %q", body, want)
			}

			requests := h.upstream.all()
			if len(requests) != 2 {
				t.Fatalf("upstream requests = %d, want 2", len(requests))
			}
			for _, request := range requests {
				if request.header.Get("Authorization") == serviceAuth {
					t.Errorf("request %q uses the service token", request.path)
				}
			}
		})
	}
}

func TestListSteveError(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstream(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}, namespaceListHandler))

	resp, body := h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}

	var status struct {
		Kind    string `json:"kind"`
		Reason  string `json:"reason"`
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &status); err != nil {
		t.Fatalf("parse status body %q: %v", body, err)
	}
	if status.Kind != "Status" {
		t.Errorf("kind = %q, want Status", status.Kind)
	}
	if status.Code != http.StatusBadGateway {
		t.Errorf("code = %d, want 502", status.Code)
	}
	if status.Reason != reasonInternalError {
		t.Errorf("reason = %q, want %q", status.Reason, reasonInternalError)
	}
	if status.Message == "" {
		t.Error("the status body has no message")
	}
}

func TestListStevePagination(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstream(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("continue") == "t1" {
			_, _ = io.WriteString(w, steveCollectionJSON("", "c"))
			return
		}
		_, _ = io.WriteString(w, steveCollectionJSON("t1", "a", "b"))
	}, namespaceListHandler))

	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	requests := h.upstream.all()
	if len(requests) != 6 {
		t.Fatalf("upstream requests = %d, want 6", len(requests))
	}
	if got := requests[2].query.Get("continue"); got != "t1" {
		t.Errorf("second allowed set continue = %q, want t1", got)
	}
	want := "kubernetes.io/metadata.name in (a,b,c)"
	if got := requests[5].query.Get("labelSelector"); got != want {
		t.Errorf("labelSelector = %q, want %q", got, want)
	}
}

func TestListSteveRequestExcludesManagedFields(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstream(steveHandler("a"), namespaceListHandler))

	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	requests := h.upstream.all()
	steve := requests[1]
	if got := steve.query.Get("exclude"); got != "metadata.managedFields" {
		t.Errorf("allowed set exclude = %q, want %q", got, "metadata.managedFields")
	}
	projects := requests[2]
	if _, ok := projects.query["exclude"]; ok {
		t.Error("the project list request has an exclude parameter")
	}
}

func TestListSteveBodyTooLarge(t *testing.T) {
	t.Parallel()
	huge := fmt.Sprintf(`{"type":"collection","data":[{"id":"a","metadata":{"name":"%s"}}]}`, strings.Repeat("x", maxSteveBody))
	h := newHarness(t, listUpstream(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, huge)
	}, namespaceListHandler))

	resp, body := h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}

	var status struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &status); err != nil {
		t.Fatalf("parse status body %q: %v", body, err)
	}
	want := fmt.Sprintf("allowed set response is larger than %d bytes", maxSteveBody)
	if !strings.Contains(status.Message, want) {
		t.Errorf("message = %q, want to contain %q", status.Message, want)
	}
}

func TestListProjectsBodyTooLarge(t *testing.T) {
	t.Parallel()
	huge := fmt.Sprintf(`{"type":"collection","data":[{"id":"local:%s"}]}`, strings.Repeat("x", maxSteveBody))
	h := newHarness(t, listUpstreamWithProjects(steveHandler("a"), func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, huge)
	}, namespaceListHandler))

	resp, body := h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}

	var status struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &status); err != nil {
		t.Fatalf("parse status body %q: %v", body, err)
	}
	want := fmt.Sprintf("project list response is larger than %d bytes", maxSteveBody)
	if !strings.Contains(status.Message, want) {
		t.Errorf("message = %q, want to contain %q", status.Message, want)
	}
}

func TestListTooManyAllowedNames(t *testing.T) {
	t.Parallel()
	names := make([]string, maxAllowedNames+1)
	for i := range names {
		names[i] = fmt.Sprintf("ns-%d", i)
	}
	h := newHarness(t, listUpstream(steveHandler(names...), namespaceListHandler))

	resp, body := h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}

	var status struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &status); err != nil {
		t.Fatalf("parse status body %q: %v", body, err)
	}
	want := fmt.Sprintf("allowed set has more than %d names", maxAllowedNames)
	if !strings.Contains(status.Message, want) {
		t.Errorf("message = %q, want to contain %q", status.Message, want)
	}
}

func TestListSteveMalformedBody(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstream(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"data": tomato}`)
	}, namespaceListHandler))

	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", resp.StatusCode)
	}
}

func TestListCachesAllowedSet(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstream(steveHandler("a"), namespaceListHandler))

	for i := range 2 {
		resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200", i, resp.StatusCode)
		}
	}
	if got := h.upstream.countPath(stevePath); got != 1 {
		t.Fatalf("allowed set requests = %d, want 1", got)
	}

	h.clock.advance(defaultCacheTTL + time.Second)
	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := h.upstream.countPath(stevePath); got != 2 {
		t.Errorf("allowed set requests = %d, want 2", got)
	}
}

// TestListCachesCallerName checks that the caller identity lookup runs once
// per cache TTL, cached with the rest of the allowed set.
func TestListCachesCallerName(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstreamFull(steveHandler("a"), projectsHandler(),
		selfSubjectReviewHandler("u-alice"), namespaceListHandler))

	for i := range 2 {
		resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200", i, resp.StatusCode)
		}
	}
	if got := h.upstream.countPath(selfSubjectReviewPath); got != 1 {
		t.Fatalf("caller identity requests = %d, want 1", got)
	}

	h.clock.advance(defaultCacheTTL + time.Second)
	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := h.upstream.countPath(selfSubjectReviewPath); got != 2 {
		t.Errorf("caller identity requests = %d, want 2", got)
	}
}

func TestListCoalescesAllowedSetRequests(t *testing.T) {
	t.Parallel()
	const callers = 5
	native := make(chan struct{}, callers)
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	defer releaseOnce()

	h := newHarness(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == stevePath:
			<-release
			steveHandler("a")(w, r)
		case r.URL.Path == projectsPath:
			projectsHandler()(w, r)
		case r.URL.Path == selfSubjectReviewPath:
			selfSubjectReviewHandler(callerUsername)(w, r)
		case r.Header.Get("Authorization") == serviceAuth:
			namespaceListHandler(w, r)
		default:
			native <- struct{}{}
			http.Error(w, "forbidden", http.StatusForbidden)
		}
	})

	done := make(chan error, callers)
	for range callers {
		request := h.request(t, http.MethodGet, listPath, nil, callerHeader())
		go func() {
			resp, err := h.proxy.Client().Do(request)
			if err != nil {
				done <- err
				return
			}
			_, err = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if err == nil && resp.StatusCode != http.StatusOK {
				err = fmt.Errorf("status = %d, want 200", resp.StatusCode)
			}
			done <- err
		}()
	}

	for range callers {
		<-native
	}
	releaseOnce()

	for range callers {
		if err := <-done; err != nil {
			t.Errorf("list request: %v", err)
		}
	}
	if got := h.upstream.countPath(stevePath); got != 1 {
		t.Errorf("allowed set requests = %d, want 1", got)
	}
}

func TestListCookieCaller(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstream(steveHandler("a"), namespaceListHandler))

	header := http.Header{"Cookie": []string{"R_SESS=x"}}
	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, header))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	requests := h.upstream.all()
	if got := requests[1].header.Get("Cookie"); got != "R_SESS=x" {
		t.Errorf("allowed set Cookie = %q, want %q", got, "R_SESS=x")
	}
	if got := requests[1].header.Get("Authorization"); got != "" {
		t.Errorf("allowed set Authorization = %q, want no header", got)
	}
	if got := requests[4].header.Get("Authorization"); got != serviceAuth {
		t.Errorf("privileged Authorization = %q, want %q", got, serviceAuth)
	}
	if _, ok := requests[4].header["Cookie"]; ok {
		t.Error("the privileged request has a Cookie header")
	}
}

func TestListRereadsTokenFile(t *testing.T) {
	t.Parallel()
	h := newHarness(t, listUpstream(steveHandler("a"), func(w http.ResponseWriter, r *http.Request) {
		namespaceListHandler(w, r)
	}))

	resp, _ := h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first status = %d, want 200", resp.StatusCode)
	}
	writeToken(t, h.tokenFile, "rotated")

	resp, _ = h.do(t, h.request(t, http.MethodGet, listPath, nil, callerHeader()))
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("second status = %d, want 403", resp.StatusCode)
	}

	requests := h.upstream.all()
	last := requests[len(requests)-1]
	if got := last.header.Get("Authorization"); got != "Bearer rotated" {
		t.Errorf("privileged Authorization = %q, want %q", got, "Bearer rotated")
	}
}

func TestMergeSelector(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		caller string
		names  []string
		want   string
	}{
		{
			name:  "one name",
			names: []string{"a"},
			want:  "kubernetes.io/metadata.name in (a)",
		},
		{
			name:  "three names",
			names: []string{"a", "b", "c"},
			want:  "kubernetes.io/metadata.name in (a,b,c)",
		},
		{
			name: "empty set",
			want: "kubernetes.io/metadata.name,!kubernetes.io/metadata.name",
		},
		{
			name:   "caller selector",
			caller: "team=x",
			names:  []string{"a", "b"},
			want:   "team=x,kubernetes.io/metadata.name in (a,b)",
		},
		{
			name:   "caller selector and empty set",
			caller: "team=x,env in (dev)",
			want:   "team=x,env in (dev),kubernetes.io/metadata.name,!kubernetes.io/metadata.name",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := mergeSelector(test.caller, test.names); got != test.want {
				t.Errorf("mergeSelector(%q, %v) = %q, want %q", test.caller, test.names, got, test.want)
			}
		})
	}
}

func TestWatchSelector(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		caller   string
		set      allowedSet
		want     string
		selected bool
	}{
		{
			name:     "no namespace and no project",
			set:      allowedSet{},
			want:     "kubernetes.io/metadata.name,!kubernetes.io/metadata.name",
			selected: true,
		},
		{
			name:     "no namespace and no project, with a caller selector",
			caller:   "team=x",
			set:      allowedSet{},
			want:     "team=x,kubernetes.io/metadata.name,!kubernetes.io/metadata.name",
			selected: true,
		},
		{
			name:     "every namespace in a project of the caller",
			set:      allowedSet{names: []string{"a", "b"}, projects: []string{"p-1", "p-2"}},
			want:     "field.cattle.io/projectId in (p-1,p-2)",
			selected: true,
		},
		{
			name:     "every namespace in a project of the caller, with a caller selector",
			caller:   "team=x",
			set:      allowedSet{names: []string{"a", "b"}, projects: []string{"p-1"}},
			want:     "team=x,field.cattle.io/projectId in (p-1)",
			selected: true,
		},
		{
			name: "a namespace outside the projects of the caller",
			set:  allowedSet{names: []string{"a", "b"}, projects: []string{"p-1"}, extras: []string{"b"}},
		},
		{
			name:   "a namespace outside the projects of the caller, with a caller selector",
			caller: "team=x",
			set:    allowedSet{names: []string{"a", "b"}, projects: []string{"p-1"}, extras: []string{"b"}},
		},
		{
			name: "a namespace and no project",
			set:  allowedSet{names: []string{"a"}, extras: []string{"a"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, selected := watchSelector(test.caller, test.set)
			if selected != test.selected {
				t.Fatalf("watchSelector(%q, %+v) selected = %v, want %v", test.caller, test.set, selected, test.selected)
			}
			if got != test.want {
				t.Errorf("watchSelector(%q, %+v) = %q, want %q", test.caller, test.set, got, test.want)
			}
		})
	}
}

func TestFilterJSONAccept(t *testing.T) {
	t.Parallel()
	const kubectlTable = "application/json;as=Table;v=v1;g=meta.k8s.io," +
		"application/json;as=Table;v=v1beta1;g=meta.k8s.io,application/json"
	tests := []struct {
		name   string
		accept string
		want   string
	}{
		{
			name:   "empty",
			accept: "",
			want:   jsonContentType,
		},
		{
			name:   "any type",
			accept: "*/*",
			want:   jsonContentType,
		},
		{
			name:   "protobuf only",
			accept: protobufContentType,
			want:   jsonContentType,
		},
		{
			name:   "protobuf and json",
			accept: protobufContentType + ",application/json",
			want:   jsonContentType,
		},
		{
			name:   "cbor and json",
			accept: "application/cbor-seq,application/json",
			want:   jsonContentType,
		},
		{
			name:   "kubectl table",
			accept: kubectlTable,
			want:   kubectlTable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := filterJSONAccept(test.accept); got != test.want {
				t.Errorf("filterJSONAccept(%q) = %q, want %q", test.accept, got, test.want)
			}
		})
	}
}

func TestWatchRequested(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{name: "absent", query: "", want: false},
		{name: "present empty", query: "watch=", want: true},
		{name: "yes", query: "watch=yes", want: true},
		{name: "true", query: "watch=true", want: true},
		{name: "one", query: "watch=1", want: true},
		{name: "zero", query: "watch=0", want: false},
		{name: "false", query: "watch=false", want: false},
		{name: "false uppercase", query: "watch=FALSE", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			query, err := url.ParseQuery(test.query)
			if err != nil {
				t.Fatalf("parse query %q: %v", test.query, err)
			}
			if got := watchRequested(query); got != test.want {
				t.Errorf("watchRequested(%q) = %v, want %v", test.query, got, test.want)
			}
		})
	}
}
