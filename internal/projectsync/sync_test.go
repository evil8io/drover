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
	"time"
)

const (
	alphaTwoPath   = "/k8s/clusters/c-1/api/v1/namespaces/alpha-two"
	alphaMovedPath = "/k8s/clusters/c-1/api/v1/namespaces/alpha-moved"
	betaOnePath    = "/k8s/clusters/c-2/api/v1/namespaces/beta-one"
	alphaListPath  = "/k8s/clusters/c-1/api/v1/namespaces"

	alphaTwoBody = `{"metadata":{"labels":{"cost-center":"cc-1"},` +
		`"annotations":{"drover-managed-annotations":"owner","drover-managed-labels":"cost-center",` +
		`"owner":"alpha@example.com"}}}`
	alphaMovedBody = `{"metadata":{"labels":{"tier":null},"annotations":{"drover-managed-labels":"cost-center"}}}`
	betaOneBody    = `{"metadata":{"labels":{"cost-center":"cc-2"},` +
		`"annotations":{"drover-managed-labels":"cost-center"}}}`
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

	want := []string{alphaMovedPath, alphaTwoPath, betaOnePath}
	if got := patchedPaths(rancher); !slices.Equal(got, want) {
		t.Fatalf("patched paths = %v, want %v", got, want)
	}
	bodies := map[string]string{alphaTwoPath: alphaTwoBody, alphaMovedPath: alphaMovedBody, betaOnePath: betaOneBody}
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
	if !strings.Contains(logs.String(), "msg=reconcile clusters=2 projects=2 namespaces=6 patched=3 errors=0") {
		t.Errorf("no summary line:\n%s", logs.String())
	}
}

func TestReconcileSetsTheProjectDisplayName(t *testing.T) {
	t.Parallel()
	rancher := newFakeRancher(t)
	syncer, _ := newSyncer(t, rancher, tokenFile(t, serviceToken+"\n"), func(cfg *Config) {
		cfg.Labels = nil
		cfg.Annotations = nil
		cfg.NameLabel = "example.com/project-name"
		cfg.NameAnnotation = "example.com/project-display-name"
	})
	syncer.reconcile(context.Background())

	patches := requestsOfPath(rancher, alphaTwoPath)
	if len(patches) != 1 {
		t.Fatalf("patch requests of %s = %d, want 1", alphaTwoPath, len(patches))
	}
	want := `{"metadata":{"labels":{"example.com/project-name":"Alpha"},` +
		`"annotations":{"drover-managed-annotations":"example.com/project-display-name",` +
		`"drover-managed-labels":"example.com/project-name",` +
		`"example.com/project-display-name":"Alpha"}}}`
	if got := patches[0].body; got != want {
		t.Errorf("patch of %s = %s, want %s", alphaTwoPath, got, want)
	}
}

func TestReconcileKeepsAKeyOfTheTenant(t *testing.T) {
	t.Parallel()
	rancher, _ := reconcileOnce(t)

	// The project has no tier label. Namespace alpha-two has tier gold, and its
	// managed annotation does not name tier, so the tier key belongs to the
	// tenant.
	for _, patch := range requestsOfPath(rancher, alphaTwoPath) {
		if strings.Contains(patch.body, "tier") {
			t.Errorf("patch of %s has the tier key: %s", patch.path, patch.body)
		}
	}
}

func TestReconcileRemovesAManagedKeyThatTheProjectDropped(t *testing.T) {
	t.Parallel()
	rancher, _ := reconcileOnce(t)

	// Namespace alpha-moved owns cost-center and tier. The project has no tier,
	// so the patch removes tier and records the smaller owned set.
	patches := requestsOfPath(rancher, alphaMovedPath)
	if len(patches) != 1 {
		t.Fatalf("patch requests of %s = %d, want 1", alphaMovedPath, len(patches))
	}
	if got := patches[0].body; got != alphaMovedBody {
		t.Errorf("patch of %s = %s, want %s", alphaMovedPath, got, alphaMovedBody)
	}
}

func TestDesiredRemovesOnlyAKeyOfTheAllowList(t *testing.T) {
	t.Parallel()

	// The tenant wrote a record that names keys the service never writes. Only
	// tier, a key of the allow list, leaves the namespace. The project sets no
	// key, so both records go too.
	var target namespace
	target.Metadata.Name = "alpha-forged"
	target.Metadata.Labels = map[string]string{
		"field.cattle.io/projectId":          "p-alpha",
		"pod-security.kubernetes.io/enforce": "restricted",
		"tier":                               "gold",
	}
	target.Metadata.Annotations = map[string]string{
		managedLabelsKey:      "field.cattle.io/projectId,pod-security.kubernetes.io/enforce,tier",
		managedAnnotationsKey: "kubectl.kubernetes.io/last-applied-configuration",
		"kubectl.kubernetes.io/last-applied-configuration": "{}",
	}
	source := project{ID: "c-1:p-alpha", Name: "Alpha"}

	change := desired(source, target, []string{"cost-center", "tier"}, []string{"owner"}, "", "", "")

	if !slices.Equal(change.removeLabels, []string{"tier"}) {
		t.Errorf("removeLabels = %v, want [tier]", change.removeLabels)
	}
	if want := []string{managedAnnotationsKey, managedLabelsKey}; !slices.Equal(change.removeAnnotations, want) {
		t.Errorf("removeAnnotations = %v, want %v", change.removeAnnotations, want)
	}
}

func TestReconcileLeavesANamespaceThatIsInTheWantedState(t *testing.T) {
	t.Parallel()
	rancher, _ := reconcileOnce(t)

	// Namespace alpha-one has every key of the project, and its managed
	// annotations name the same keys.
	if got := requestsOfPath(rancher, "/k8s/clusters/c-1/api/v1/namespaces/alpha-one"); len(got) != 0 {
		t.Errorf("patch requests of alpha-one = %d, want 0", len(got))
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

// TestReconcileSyncsTheClustersInParallel checks that the run syncs the two
// clusters at the same time. The namespace list of each cluster blocks until
// the request timeout, so a serial run needs two timeouts.
func TestReconcileSyncsTheClustersInParallel(t *testing.T) {
	t.Parallel()
	const timeout = 300 * time.Millisecond
	rancher := newFakeRancher(t, hanging("c-1", "c-2"))
	syncer, logs := newSyncer(t, rancher, tokenFile(t, serviceToken), func(cfg *Config) {
		cfg.Interval = timeout
	})

	start := time.Now()
	syncer.reconcile(context.Background())
	elapsed := time.Since(start)

	if elapsed < timeout {
		t.Fatalf("the run took %s, want at least one timeout of %s", elapsed, timeout)
	}
	if elapsed >= 2*timeout {
		t.Errorf("the run took %s, want less than the two timeouts of a serial run", elapsed)
	}
	if !strings.Contains(logs.String(), "msg=reconcile clusters=2 projects=2 namespaces=0 patched=0 errors=2") {
		t.Errorf("no summary line with two errors:\n%s", logs.String())
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

	if got := patchedPaths(rancher); !slices.Equal(got, []string{alphaMovedPath, alphaTwoPath, betaOnePath}) {
		t.Errorf("patched paths = %v, want [%s %s %s]", got, alphaMovedPath, alphaTwoPath, betaOnePath)
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
	if got := patchedPaths(rancher); !slices.Equal(got, []string{alphaMovedPath, alphaTwoPath, betaOnePath}) {
		t.Errorf("patched paths = %v, want [%s %s %s]", got, alphaMovedPath, alphaTwoPath, betaOnePath)
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
