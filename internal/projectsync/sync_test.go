package projectsync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
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
	if got := lists[0].query.Get("limit"); got != "500" {
		t.Errorf("limit of the first page = %q, want 500", got)
	}
	if got := lists[1].query.Get("marker"); got != "2" {
		t.Errorf("marker of the second page = %q, want 2", got)
	}
	if got := patchedPaths(rancher); !slices.Equal(got, []string{alphaMovedPath, alphaTwoPath, betaOnePath}) {
		t.Errorf("patched paths = %v, want [%s %s %s]", got, alphaMovedPath, alphaTwoPath, betaOnePath)
	}
}

// twoPages serves the namespaces of cluster c-1 in two pages. The first page
// has its metadata after the items.
func twoPages() func(*fakeRancher) {
	return listing("c-1", map[string]string{
		"": `{"kind":"NamespaceList","items":[` + alphaOneItem + `,` + alphaTwoItem +
			`],"metadata":{"continue":"page-2"}}`,
		"page-2": `{"kind":"NamespaceList","metadata":{"resourceVersion":"20"},"items":[` +
			alphaThreeItem + `,` + alphaOrphanItem + `,` + alphaMovedItem + `]}`,
	})
}

// continueTokens returns the continue token of every request, in order.
func continueTokens(requests []recorded) []string {
	tokens := make([]string, 0, len(requests))
	for _, request := range requests {
		tokens = append(tokens, request.query.Get("continue"))
	}
	return tokens
}

func TestNamespaceListKeepsOnlyTheKeysThatTheSyncReads(t *testing.T) {
	t.Parallel()
	const (
		count          = 100
		nameLabel      = "example.com/project-name"
		nameAnnotation = "example.com/project-display-name"
	)

	// Each namespace has about 250 KB of annotations that the sync does not
	// read, and all of them are on one page.
	bulk := strings.Repeat("x", 250_000)
	var page strings.Builder
	page.WriteString(`{"kind":"NamespaceList","items":[`)
	for i := range count {
		if i > 0 {
			page.WriteString(",")
		}
		fmt.Fprintf(&page, `{"metadata":{"name":"bulk-%03d","resourceVersion":"%d",`+
			`"labels":{"field.cattle.io/projectId":"p-alpha","cost-center":"old","tier":"gold","app":"web",%q:"Alpha"},`+
			`"annotations":{"owner":"alpha@example.com",%q:"Alpha","note":"outside the allow list","bulk":%q,`+
			`"drover-managed-labels":"cost-center,%s","drover-managed-annotations":"%s,owner"}}}`,
			i, 100+i, nameLabel, nameAnnotation, bulk, nameLabel, nameAnnotation)
	}
	page.WriteString(`],"metadata":{"resourceVersion":"200"}}`)

	rancher := newFakeRancher(t, listing("c-1", map[string]string{"": page.String()}))
	syncer, _ := newSyncer(t, rancher, tokenFile(t, serviceToken), func(cfg *Config) {
		cfg.NameLabel = nameLabel
		cfg.NameAnnotation = nameAnnotation
		cfg.Interval = 30 * time.Second
	})
	ctx := context.Background()

	items, err := syncer.namespaces(ctx, serviceToken, "c-1", projectLabel)
	if err != nil {
		t.Fatalf("list the namespaces of c-1: %v", err)
	}
	if len(items) != count {
		t.Fatalf("namespaces = %d, want %d", len(items), count)
	}
	wantLabels := map[string]string{
		projectLabel:  "p-alpha",
		"cost-center": "old",
		"tier":        "gold",
		nameLabel:     "Alpha",
	}
	wantAnnotations := map[string]string{
		"owner":               "alpha@example.com",
		nameAnnotation:        "Alpha",
		managedLabelsKey:      "cost-center," + nameLabel,
		managedAnnotationsKey: nameAnnotation + ",owner",
	}
	for i, item := range items {
		if want := fmt.Sprintf("bulk-%03d", i); item.Metadata.Name != want {
			t.Errorf("name of namespace %d = %q, want %q", i, item.Metadata.Name, want)
		}
		if want := strconv.Itoa(100 + i); item.Metadata.ResourceVersion != want {
			t.Errorf("resource version of %s = %q, want %q", item.Metadata.Name, item.Metadata.ResourceVersion, want)
		}
		if !maps.Equal(item.Metadata.Labels, wantLabels) {
			t.Errorf("labels of %s = %v, want %v", item.Metadata.Name, item.Metadata.Labels, wantLabels)
		}
		if !maps.Equal(item.Metadata.Annotations, wantAnnotations) {
			t.Errorf("annotation keys of %s = %v, want %v",
				item.Metadata.Name, keysOf(item.Metadata.Annotations), keysOf(wantAnnotations))
		}
	}

	syncer.reconcile(ctx)

	const wantBody = `{"metadata":{"labels":{"cost-center":"cc-1"}}}`
	var patches int
	for _, patch := range rancher.method(http.MethodPatch) {
		if !strings.HasPrefix(patch.path, alphaListPath+"/bulk-") {
			continue
		}
		patches++
		if patch.body != wantBody {
			t.Errorf("patch of %s = %s, want %s", patch.path, patch.body, wantBody)
		}
	}
	if patches != count {
		t.Errorf("patches of the bulk namespaces = %d, want %d", patches, count)
	}
}

func TestReconcileRewritesARecordAboveTheBound(t *testing.T) {
	t.Parallel()
	record := "cost-center," + strings.Repeat("x", maxRecordValue)
	page := `{"kind":"NamespaceList","items":[{"metadata":{"name":"alpha-filled","resourceVersion":"40",` +
		`"labels":{"field.cattle.io/projectId":"p-alpha","cost-center":"cc-1"},` +
		`"annotations":{"owner":"alpha@example.com","drover-managed-annotations":"owner",` +
		`"drover-managed-labels":"` + record + `"}}}]}`
	rancher := newFakeRancher(t, listing("c-1", map[string]string{"": page}))
	syncer, _ := newSyncer(t, rancher, tokenFile(t, serviceToken))
	ctx := context.Background()

	items, err := syncer.namespaces(ctx, serviceToken, "c-1", projectLabel)
	if err != nil {
		t.Fatalf("list the namespaces of c-1: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("namespaces = %d, want 1", len(items))
	}
	if _, ok := items[0].Metadata.Annotations[managedLabelsKey]; ok {
		t.Errorf("the prune keeps %s above %d bytes", managedLabelsKey, maxRecordValue)
	}
	if got := items[0].Metadata.Annotations[managedAnnotationsKey]; got != "owner" {
		t.Errorf("%s = %q, want owner", managedAnnotationsKey, got)
	}

	syncer.reconcile(ctx)

	patches := requestsOfPath(rancher, alphaListPath+"/alpha-filled")
	if len(patches) != 1 {
		t.Fatalf("patch requests of alpha-filled = %d, want 1", len(patches))
	}
	const want = `{"metadata":{"annotations":{"drover-managed-labels":"cost-center"}}}`
	if got := patches[0].body; got != want {
		t.Errorf("patch of alpha-filled = %s, want %s", got, want)
	}
}

func TestNamespaceListFollowsTheContinueToken(t *testing.T) {
	t.Parallel()
	rancher, logs := reconcileOnce(t, twoPages())

	lists := requestsOfPath(rancher, alphaListPath)
	if got := continueTokens(lists); !slices.Equal(got, []string{"", "page-2"}) {
		t.Fatalf("continue tokens of the list requests = %q, want [\"\" page-2]", got)
	}
	for _, list := range lists {
		if got := list.query.Get("limit"); got != "500" {
			t.Errorf("limit = %q, want 500", got)
		}
		if got := list.query.Get("labelSelector"); got != projectLabel {
			t.Errorf("label selector = %q, want %q", got, projectLabel)
		}
	}
	if got := patchedPaths(rancher); !slices.Equal(got, []string{alphaMovedPath, alphaTwoPath, betaOnePath}) {
		t.Errorf("patched paths = %v, want [%s %s %s]", got, alphaMovedPath, alphaTwoPath, betaOnePath)
	}
	if !strings.Contains(logs.String(), "msg=reconcile clusters=2 projects=2 namespaces=6 patched=3 errors=0") {
		t.Errorf("no summary line with six namespaces:\n%s", logs.String())
	}
}

func TestNamespaceListStartsAgainAfterAnExpiredContinueToken(t *testing.T) {
	t.Parallel()
	rancher, logs := reconcileOnce(t, twoPages(), expiring("page-2", 1))

	want := []string{"", "page-2", "", "page-2"}
	if got := continueTokens(requestsOfPath(rancher, alphaListPath)); !slices.Equal(got, want) {
		t.Fatalf("continue tokens of the list requests = %q, want %q", got, want)
	}
	if got := patchedPaths(rancher); !slices.Equal(got, []string{alphaMovedPath, alphaTwoPath, betaOnePath}) {
		t.Errorf("patched paths = %v, want [%s %s %s]", got, alphaMovedPath, alphaTwoPath, betaOnePath)
	}
	// The first page of the expired list does not count again.
	if !strings.Contains(logs.String(), "msg=reconcile clusters=2 projects=2 namespaces=6 patched=3 errors=0") {
		t.Errorf("no summary line with six namespaces:\n%s", logs.String())
	}
}

func TestNamespaceListFailsAfterASecondExpiredContinueToken(t *testing.T) {
	t.Parallel()
	rancher := newFakeRancher(t, twoPages(), expiring("page-2", 2))
	syncer, _ := newSyncer(t, rancher, tokenFile(t, serviceToken))

	_, err := syncer.namespaces(context.Background(), serviceToken, "c-1", projectLabel)
	var status statusError
	if !errors.As(err, &status) || status.status != http.StatusGone {
		t.Fatalf("error = %v, want status 410", err)
	}
	want := []string{"", "page-2", "", "page-2"}
	if got := continueTokens(requestsOfPath(rancher, alphaListPath)); !slices.Equal(got, want) {
		t.Errorf("continue tokens of the list requests = %q, want %q", got, want)
	}
}

func TestReconcileFailsTheClusterOfAPageAboveTheCap(t *testing.T) {
	t.Parallel()
	const pageCap = 4096
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	large := `{"kind":"NamespaceList","items":[{"metadata":{"name":"alpha-large",` +
		`"labels":{"field.cattle.io/projectId":"p-alpha"},"annotations":{"bulk":"` +
		strings.Repeat("x", 2*pageCap) + `"}}}]}`
	rancher := newFakeRancher(t, listing("c-1", map[string]string{"": large}))
	syncer, logs := newSyncer(t, rancher, tokenFile(t, serviceToken), func(cfg *Config) {
		cfg.MeterProvider = provider
	})
	syncer.pageCap = pageCap
	syncer.reconcile(context.Background())

	want := `msg="the namespace list request failed" cluster=c-1 ` +
		`error="decode the namespace list of cluster c-1: the page is larger than 4096 bytes"`
	if !strings.Contains(logs.String(), want) {
		t.Errorf("no failure line with the cap:\n%s", logs.String())
	}
	if got := patchedPaths(rancher); !slices.Equal(got, []string{betaOnePath}) {
		t.Errorf("patched paths = %v, want [%s]", got, betaOnePath)
	}
	if !strings.Contains(logs.String(), "msg=reconcile clusters=2 projects=2 namespaces=1 patched=1 errors=1") {
		t.Errorf("no summary line with one error:\n%s", logs.String())
	}

	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	errs := findSum(t, data, "drover.sync.errors")
	if len(errs.DataPoints) != 1 || errs.DataPoints[0].Value != 1 {
		t.Errorf("drover.sync.errors points = %v, want one point with value 1", errs.DataPoints)
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
