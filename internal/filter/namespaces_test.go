package filter

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
	if len(requests) != 4 {
		t.Fatalf("upstream requests = %d, want 4", len(requests))
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

	privileged := requests[3]
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

	privileged := h.upstream.all()[3]
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

	privileged := h.upstream.all()[3]
	want := "kubernetes.io/metadata.name,!kubernetes.io/metadata.name"
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

func TestListSteveDeniesCaller(t *testing.T) {
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
			if resp.StatusCode != status {
				t.Errorf("status = %d, want %d", resp.StatusCode, status)
			}
			if want := `{"type":"error","code":"Unauthorized"}`; string(body) != want {
				t.Errorf("body = %q, want %q", body, want)
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
	if len(requests) != 5 {
		t.Fatalf("upstream requests = %d, want 5", len(requests))
	}
	if got := requests[2].query.Get("continue"); got != "t1" {
		t.Errorf("second allowed set continue = %q, want t1", got)
	}
	want := "kubernetes.io/metadata.name in (a,b,c)"
	if got := requests[4].query.Get("labelSelector"); got != want {
		t.Errorf("labelSelector = %q, want %q", got, want)
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
	if got := requests[3].header.Get("Authorization"); got != serviceAuth {
		t.Errorf("privileged Authorization = %q, want %q", got, serviceAuth)
	}
	if _, ok := requests[3].header["Cookie"]; ok {
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
