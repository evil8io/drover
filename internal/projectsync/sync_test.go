package projectsync

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const (
	alphaTwoPath  = "/k8s/clusters/c-1/api/v1/namespaces/alpha-two"
	betaOnePath   = "/k8s/clusters/c-2/api/v1/namespaces/beta-one"
	alphaListPath = "/k8s/clusters/c-1/api/v1/namespaces"

	alphaTwoBody = `{"metadata":{"labels":{"cost-center":"cc-1"},"annotations":{"owner":"alpha@example.com"}}}`
	betaOneBody  = `{"metadata":{"labels":{"cost-center":"cc-2"}}}`
)

// reconcileOnce runs one reconcile against a fake Rancher with a token file.
func reconcileOnce(t *testing.T, options ...func(*fakeRancher)) (*fakeRancher, *syncBuffer) {
	t.Helper()
	rancher := newFakeRancher(t, options...)
	syncer, logs := newSyncer(t, rancher, tokenFile(t, serviceToken+"\n"))
	syncer.reconcile(context.Background())
	return rancher, logs
}

func patchedPaths(rancher *fakeRancher) []string {
	var paths []string
	for _, request := range rancher.method(http.MethodPatch) {
		paths = append(paths, request.path)
	}
	return slices.Sorted(slices.Values(paths))
}

func requestsOfPath(rancher *fakeRancher, path string) []recorded {
	var out []recorded
	for _, request := range rancher.all() {
		if request.path == path {
			out = append(out, request)
		}
	}
	return out
}

func TestReconcileSetsAMissingKeyAndOverwritesADifferentValue(t *testing.T) {
	t.Parallel()
	rancher, logs := reconcileOnce(t)

	if got := patchedPaths(rancher); !slices.Equal(got, []string{alphaTwoPath, betaOnePath}) {
		t.Fatalf("patched paths = %v, want [%s %s]", got, alphaTwoPath, betaOnePath)
	}
	bodies := map[string]string{alphaTwoPath: alphaTwoBody, betaOnePath: betaOneBody}
	for _, patch := range rancher.method(http.MethodPatch) {
		if patch.body != bodies[patch.path] {
			t.Errorf("patch of %s = %s, want %s", patch.path, patch.body, bodies[patch.path])
		}
		if got := patch.header.Get("Content-Type"); got != mergePatchType {
			t.Errorf("content type of %s = %q, want %q", patch.path, got, mergePatchType)
		}
		if got := patch.header.Get("Authorization"); got != "Bearer "+serviceToken {
			t.Errorf("authorization of %s = %q, want the service token", patch.path, got)
		}
		if got := patch.header.Get("User-Agent"); got != userAgent {
			t.Errorf("user agent of %s = %q, want %q", patch.path, got, userAgent)
		}
	}

	lists := requestsOfPath(rancher, alphaListPath)
	if len(lists) != 1 {
		t.Fatalf("namespace list requests of c-1 = %d, want 1", len(lists))
	}
	if got := lists[0].query.Get("labelSelector"); got != projectLabel {
		t.Errorf("label selector = %q, want %q", got, projectLabel)
	}
	if !strings.Contains(logs.String(), `msg="namespace unchanged" cluster=c-1 namespace=alpha-one`) {
		t.Errorf("no unchanged line for alpha-one:\n%s", logs.String())
	}
	if !strings.Contains(logs.String(), "msg=reconcile clusters=2 projects=2 namespaces=5 patched=2 errors=0") {
		t.Errorf("no summary line:\n%s", logs.String())
	}
}

func TestReconcileKeepsAKeyThatTheProjectDoesNotHave(t *testing.T) {
	t.Parallel()
	rancher, _ := reconcileOnce(t)

	// The project has no tier label, and namespace alpha-two has tier gold.
	for _, patch := range rancher.method(http.MethodPatch) {
		if strings.Contains(patch.body, "tier") {
			t.Errorf("patch of %s has the tier key: %s", patch.path, patch.body)
		}
	}
}

func TestReconcileIgnoresAKeyOutsideTheAllowList(t *testing.T) {
	t.Parallel()
	rancher, _ := reconcileOnce(t)

	for _, patch := range rancher.method(http.MethodPatch) {
		for _, key := range []string{"team", "note"} {
			if strings.Contains(patch.body, key) {
				t.Errorf("patch of %s has the key %s: %s", patch.path, key, patch.body)
			}
		}
	}
}

func TestReconcileIgnoresANamespaceWithoutTheProjectLabel(t *testing.T) {
	t.Parallel()
	rancher, logs := reconcileOnce(t)

	for _, patch := range rancher.method(http.MethodPatch) {
		if strings.HasSuffix(patch.path, "/alpha-three") || strings.HasSuffix(patch.path, "/alpha-orphan") {
			t.Errorf("patch of %s, want no patch", patch.path)
		}
	}
	if !strings.Contains(logs.String(), `msg="namespace skipped" cluster=c-1 namespace=alpha-three`) {
		t.Errorf("no skipped line for alpha-three:\n%s", logs.String())
	}
	if !strings.Contains(logs.String(), `msg="namespace skipped" cluster=c-1 namespace=alpha-orphan project=p-gone`) {
		t.Errorf("no skipped line for alpha-orphan:\n%s", logs.String())
	}
}

func TestReconcileSkipsAForbiddenCluster(t *testing.T) {
	t.Parallel()
	rancher, logs := reconcileOnce(t, forbid("c-1"))

	if got := patchedPaths(rancher); !slices.Equal(got, []string{betaOnePath}) {
		t.Fatalf("patched paths = %v, want [%s]", got, betaOnePath)
	}
	if !strings.Contains(logs.String(), `msg="the service user has no binding on the cluster" cluster=c-1`) {
		t.Errorf("no warning for cluster c-1:\n%s", logs.String())
	}
	if !strings.Contains(logs.String(), "msg=reconcile clusters=2 projects=2 namespaces=1 patched=1 errors=1") {
		t.Errorf("no summary line with one error:\n%s", logs.String())
	}
}

func TestReconcileWaitsForTheTokenFile(t *testing.T) {
	t.Parallel()
	rancher := newFakeRancher(t)
	path := filepath.Join(t.TempDir(), "token")
	syncer, logs := newSyncer(t, rancher, path)

	syncer.reconcile(context.Background())
	syncer.reconcile(context.Background())

	if got := len(rancher.all()); got != 0 {
		t.Fatalf("requests without a token file = %d, want 0", got)
	}
	if got := strings.Count(logs.String(), `msg="the token file is not available yet"`); got != 1 {
		t.Errorf("warnings about the token file = %d, want 1", got)
	}

	writeToken(t, path, serviceToken+"\n")
	syncer.reconcile(context.Background())

	if got := patchedPaths(rancher); !slices.Equal(got, []string{alphaTwoPath, betaOnePath}) {
		t.Errorf("patched paths = %v, want [%s %s]", got, alphaTwoPath, betaOnePath)
	}
	if !strings.Contains(logs.String(), `msg="the token file has a token"`) {
		t.Errorf("no line about the token file:\n%s", logs.String())
	}
}

func TestReconcileFollowsThePagination(t *testing.T) {
	t.Parallel()
	rancher, _ := reconcileOnce(t, paginated())

	lists := requestsOfPath(rancher, projectsPath)
	if len(lists) != 2 {
		t.Fatalf("project list requests = %d, want 2", len(lists))
	}
	if got := lists[1].query.Get("marker"); got != "2" {
		t.Errorf("marker of the second page = %q, want 2", got)
	}
	if got := patchedPaths(rancher); !slices.Equal(got, []string{alphaTwoPath, betaOnePath}) {
		t.Errorf("patched paths = %v, want [%s %s]", got, alphaTwoPath, betaOnePath)
	}
}

func TestHandlerAnswersHealthz(t *testing.T) {
	t.Parallel()
	rancher := newFakeRancher(t)
	syncer, _ := newSyncer(t, rancher, tokenFile(t, serviceToken))

	server := httptest.NewServer(syncer.Handler())
	t.Cleanup(server.Close)

	resp, err := server.Client().Get(server.URL + "/healthz")
	if err != nil {
		t.Fatalf("get healthz: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read the answer: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if string(body) != "ok" {
		t.Errorf("body = %q, want ok", body)
	}
}
