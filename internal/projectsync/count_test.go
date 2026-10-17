package projectsync

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	alphaProjectPath = "/k8s/clusters/local/apis/management.cattle.io/v3/namespaces/c-1/projects/p-alpha"

	// gammaProject is a second project of cluster c-1. The fixed list of c-1
	// has no namespace of it.
	gammaProject = `{"id":"c-1:p-gamma","clusterId":"c-1","name":"Gamma","labels":{},"annotations":{}}`
	// countedGammaProject is gammaProject with its namespace count.
	countedGammaProject = `{"id":"c-1:p-gamma","clusterId":"c-1","name":"Gamma","labels":{},` +
		`"annotations":{"managed.drover.evil8.io/namespace-count":"0"}}`
	// uncountedBetaProject is betaProject without its namespace count.
	uncountedBetaProject = `{"id":"c-2:p-beta","clusterId":"c-2","name":"Beta",` +
		`"labels":{"cost-center":"cc-2"},"annotations":{}}`

	// alphaNewItem is a new namespace of p-alpha.
	alphaNewItem = `{"metadata":{"name":"alpha-new","resourceVersion":"31",` +
		`"labels":{"field.cattle.io/projectId":"p-alpha"},` +
		`"annotations":{"field.cattle.io/projectId":"c-1:p-alpha"}}}`
	// alphaOneDeleting is alpha-one with a deletionTimestamp.
	alphaOneDeleting = `{"metadata":{"name":"alpha-one","resourceVersion":"41","deletionTimestamp":"2024-01-01T00:00:00Z",` +
		`"labels":{"field.cattle.io/projectId":"p-alpha"},` +
		`"annotations":{"field.cattle.io/projectId":"c-1:p-alpha"}}}`
)

// isCountPatch reports whether request is a patch of the namespace count
// annotation of a project.
func isCountPatch(request recorded) bool {
	return request.method == http.MethodPatch &&
		strings.HasPrefix(request.path, clusterPath(rancherCluster)+managementProjects) &&
		strings.Contains(request.body, namespaceCountKey)
}

// countWrites returns the namespace count patches of requests, as
// "<cluster>/<project>=<count>", in the order of the requests.
func countWrites(t *testing.T, requests []recorded) []string {
	t.Helper()
	var out []string
	for _, request := range requests {
		if !isCountPatch(request) {
			continue
		}
		var body struct {
			Metadata struct {
				Annotations map[string]string `json:"annotations"`
			} `json:"metadata"`
		}
		if err := json.Unmarshal([]byte(request.body), &body); err != nil {
			t.Fatalf("decode the count patch %s: %v", request.body, err)
		}
		if len(body.Metadata.Annotations) != 1 {
			t.Errorf("count patch of %s = %s, want one annotation", request.path, request.body)
		}
		if got := request.header.Get("Content-Type"); got != mergePatchType {
			t.Errorf("content type of %s = %q, want %q", request.path, got, mergePatchType)
		}
		target := strings.Replace(strings.TrimPrefix(request.path, clusterPath(rancherCluster)+managementProjects), "/projects/", "/", 1)
		out = append(out, target+"="+body.Metadata.Annotations[namespaceCountKey])
	}
	return out
}

func eventOf(eventType, item string) string {
	return `{"type":"` + eventType + `","object":` + item + `}`
}

func TestReconcileWritesTheNamespaceCountOfEachProject(t *testing.T) {
	t.Parallel()
	rancher, logs := reconcileOnce(t, servingProjects(alphaProject, uncountedBetaProject, gammaProject))

	// p-alpha has its count already. alpha-orphan counts for p-gone, which
	// the snapshot does not have.
	got := countWrites(t, rancher.all())
	slices.Sort(got)
	if want := []string{"c-1/p-gamma=0", "c-2/p-beta=1"}; !slices.Equal(got, want) {
		t.Fatalf("count writes = %v, want %v", got, want)
	}
	for _, line := range []string{
		`msg="the namespace count is written" cluster=c-1 project=p-gamma count=0`,
		`msg="the namespace count is written" cluster=c-2 project=p-beta count=1`,
		"errors=0",
	} {
		if !strings.Contains(logs.String(), line) {
			t.Errorf("no line %s:\n%s", line, logs.String())
		}
	}
}

func TestReconcileStoresTheWrittenCountInTheSnapshot(t *testing.T) {
	t.Parallel()
	rancher := newFakeRancher(t, servingProjects(alphaProject, betaProject, gammaProject))
	syncer, _ := newSyncer(t, rancher, tokenFile(t, serviceToken))
	syncer.reconcile(context.Background())

	if got := syncer.projectsOf("c-1")["p-gamma"].namespaceCount; got != "0" {
		t.Errorf("namespace count of p-gamma in the snapshot = %q, want 0", got)
	}
	if got := syncer.projectsOf("c-1")["p-alpha"].Labels["cost-center"]; got != "cc-1" {
		t.Errorf("cost-center of p-alpha in the snapshot = %q, want cc-1", got)
	}
}

func TestReconcileCountsAFailedCountPatchOnly(t *testing.T) {
	t.Parallel()
	_, logs := reconcileOnce(t,
		servingProjects(alphaProject, uncountedBetaProject, gammaProject),
		failPatch("p-gamma", http.StatusInternalServerError, `{"kind":"Status","reason":"InternalError"}`),
		failPatch("p-beta", http.StatusNotFound, `{"kind":"Status","reason":"NotFound"}`))

	for _, line := range []string{
		`level=WARN msg="the namespace count patch failed" cluster=c-1 project=p-gamma`,
		`msg="the namespace count patch is skipped" cluster=c-2 project=p-beta`,
		"errors=1",
	} {
		if !strings.Contains(logs.String(), line) {
			t.Errorf("no line %s:\n%s", line, logs.String())
		}
	}
	if strings.Contains(logs.String(), `msg="the namespace count is written"`) {
		t.Errorf("a written line for a failed patch:\n%s", logs.String())
	}
}

func TestWatchWritesTheCountOfANamespaceEvent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		options []func(*fakeRancher)
		want    []string
	}{
		{
			name:    "a new namespace",
			options: []func(*fakeRancher){watching("c-1", eventOf(watchAdded, alphaNewItem))},
			want:    []string{"c-1/p-alpha=4"},
		},
		{
			name:    "a deleted namespace",
			options: []func(*fakeRancher){watching("c-1", eventOf(watchDeleted, alphaOneDeleting))},
			want:    []string{"c-1/p-alpha=2"},
		},
		{
			name: "a namespace that moves to another project",
			options: []func(*fakeRancher){
				servingProjects(alphaProject, betaProject, countedGammaProject),
				watching("c-1", eventOf(watchModified, strings.ReplaceAll(alphaTwoItem, "p-alpha", "p-gamma"))),
			},
			want: []string{"c-1/p-alpha=2", "c-1/p-gamma=1"},
		},
		{
			name:    "a namespace in Terminating",
			options: []func(*fakeRancher){watching("c-1", eventOf(watchModified, alphaOneDeleting))},
		},
		{
			name: "a namespace that leaves the selection in Terminating",
			options: []func(*fakeRancher){
				watching("c-1", eventOf(watchDeleted, alphaOneItem)),
				refreshed("alpha-one", alphaOneDeleting),
			},
		},
		{
			name:    "a namespace that leaves the selection and is gone",
			options: []func(*fakeRancher){watching("c-1", eventOf(watchDeleted, alphaOneItem))},
			want:    []string{"c-1/p-alpha=2"},
		},
		{
			name: "a namespace that leaves the selection and the read fails",
			options: []func(*fakeRancher){
				watching("c-1", eventOf(watchDeleted, alphaOneItem)),
				failRefresh("alpha-one", http.StatusInternalServerError),
			},
		},
		{
			name: "a namespace of a project outside the snapshot",
			options: []func(*fakeRancher){watching("c-1",
				eventOf(watchAdded, strings.ReplaceAll(alphaNewItem, "p-alpha", "p-gone")))},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rancher := newFakeRancher(t, tt.options...)
			syncer, _ := newSyncer(t, rancher, tokenFile(t, serviceToken))
			syncer.reconcile(context.Background())
			before := len(rancher.all())

			if _, err := watchOnce(t, syncer, "c-1"); err != nil {
				t.Fatalf("stream the namespace watch: %v", err)
			}
			if got := countWrites(t, rancher.all()[before:]); !slices.Equal(got, tt.want) {
				t.Errorf("count writes = %v, want %v", got, tt.want)
			}
			for _, write := range tt.want {
				target, value, _ := strings.Cut(write, "=")
				cluster, name, _ := strings.Cut(target, "/")
				if got := syncer.projectsOf(cluster)[name].namespaceCount; got != value {
					t.Errorf("namespace count of %s in the snapshot = %q, want %s", name, got, value)
				}
			}
		})
	}
}

// TestWatchCountPatchTakesATokenOfTheClusterLimiter checks that the count
// patch of the worker waits for the limiter. At one token per second, the
// namespace patch takes the full bucket, so the count patch comes about one
// second after it.
func TestWatchCountPatchTakesATokenOfTheClusterLimiter(t *testing.T) {
	t.Parallel()
	rancher := newFakeRancher(t, watching("c-1", eventOf(watchAdded, alphaNewItem)))
	syncer, _ := newSyncer(t, rancher, tokenFile(t, serviceToken), func(cfg *Config) {
		cfg.PatchRate = 1
	})
	syncer.reconcile(context.Background())

	if _, err := watchOnce(t, syncer, "c-1"); err != nil {
		t.Fatalf("stream the namespace watch: %v", err)
	}
	var namespacePatch, countPatch time.Time
	for _, request := range rancher.method(http.MethodPatch) {
		switch request.path {
		case alphaNewPath:
			namespacePatch = request.at
		case alphaProjectPath:
			countPatch = request.at
		}
	}
	if namespacePatch.IsZero() || countPatch.IsZero() {
		t.Fatalf("namespace patch at %v and count patch at %v, want both", namespacePatch, countPatch)
	}
	if gap := countPatch.Sub(namespacePatch); gap < 500*time.Millisecond {
		t.Errorf("the count patch came %s after the namespace patch, want at least 500 ms", gap)
	}
}

// TestProjectWatchStartsNoWorkForANewNamespaceCount checks that a Project
// event that changes only the count stores the count, and starts no work. The
// sets count alpha-new, and the snapshot has the older count of a project
// list. A later namespace event then compares against the stored count.
func TestProjectWatchStartsNoWorkForANewNamespaceCount(t *testing.T) {
	t.Parallel()
	frame := alphaFrame(t, watchModified, func(object *projectObject) {
		object.Metadata.Annotations[namespaceCountKey] = "4"
	})
	p := newProjectTest(t, []func(*fakeRancher){watchingProjects(frame)})
	p.syncer.counts.move("c-1", "alpha-new", "p-alpha")

	if _, err := p.stream(t, ""); err != nil {
		t.Fatalf("stream the project watch: %v", err)
	}
	if got := p.calls(); len(got) != 0 {
		t.Errorf("requests = %v, want none", got)
	}
	if !strings.Contains(p.logs.String(), `msg="project unchanged" cluster=c-1 project=c-1:p-alpha`) {
		t.Errorf("no unchanged line:\n%s", p.logs.String())
	}
	if got := p.syncer.projectsOf("c-1")["p-alpha"].namespaceCount; got != "4" {
		t.Errorf("namespace count of p-alpha in the snapshot = %q, want 4", got)
	}

	var item namespace
	if err := json.Unmarshal([]byte(alphaNewItem), &item); err != nil {
		t.Fatalf("decode alpha-new: %v", err)
	}
	p.watches.get("c-1").patches.put(patchItem{target: p.syncer.pruneNamespace(item), origin: originWatch})
	p.wait(t)

	if got := countWrites(t, p.calls()); len(got) != 0 {
		t.Errorf("count writes = %v, want none", got)
	}
	if got := p.patched(); !slices.Equal(got, []string{alphaNewPath}) {
		t.Errorf("patched paths = %v, want [%s]", got, alphaNewPath)
	}
}

// TestReconcileWritesTheCountOfEveryKindOfProject checks the System project,
// the Default project, a tenant project, and the account project. The run
// that creates the account project does not have it in the snapshot, so the
// next run writes its count.
func TestReconcileWritesTheCountOfEveryKindOfProject(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-system", func(p *storedProject) { p.labels = map[string]string{systemProjectLabel: "true"} })
	fake.addProject("p-default", func(p *storedProject) { p.labels = map[string]string{defaultProjectLabel: "true"} })
	fake.addProject("p-alpha", nil)
	fake.addNamespace(tenantNamespace("cattle-system", "p-system"))
	fake.addNamespace(tenantNamespace("default", "p-default"))
	fake.addNamespace(tenantNamespace("ns-a", "p-alpha"))
	fake.addNamespace(tenantNamespace("ns-b", "p-alpha"))
	syncer, _ := newAccountsSyncer(t, fake)

	var runs [][]string
	for range 3 {
		fake.resetRequests()
		syncer.reconcile(context.Background())
		got := countWrites(t, fake.all())
		slices.Sort(got)
		runs = append(runs, got)
	}

	want := [][]string{
		{"c-1/p-alpha=2", "c-1/p-default=1", "c-1/p-system=1"},
		{"c-1/p-acct=1"},
		nil,
	}
	for i := range want {
		if !slices.Equal(runs[i], want[i]) {
			t.Errorf("count writes of run %d = %v, want %v", i+1, runs[i], want[i])
		}
	}
}

func TestNamespaceSetsChangeNothingBeforeTheFirstList(t *testing.T) {
	t.Parallel()
	var sets namespaceSets
	if got := sets.move("c-1", "alpha-new", "p-alpha"); got != nil {
		t.Errorf("changed projects before the first list = %v, want none", got)
	}
	if _, ok := sets.count("c-1", "p-alpha"); ok {
		t.Error("a count before the first list, want none")
	}

	sets.replace("c-1", nil)
	if got := sets.move("c-1", "alpha-new", "p-alpha"); !slices.Equal(got, []string{"p-alpha"}) {
		t.Errorf("changed projects after the first list = %v, want [p-alpha]", got)
	}
	sets.retain([]string{"c-2"})
	if _, ok := sets.count("c-1", "p-alpha"); ok {
		t.Error("a count of a cluster outside the run, want none")
	}
}

func TestPruneProjectKeepsNoCountAboveTheBound(t *testing.T) {
	t.Parallel()
	rancher := newFakeRancher(t)
	syncer, _ := newSyncer(t, rancher, tokenFile(t, serviceToken))
	for value, want := range map[string]string{
		"12":                       "12",
		strings.Repeat("9", 20):    strings.Repeat("9", 20),
		strings.Repeat("9", 21):    "",
		strings.Repeat("x", 1<<16): "",
	} {
		item := syncer.pruneProject(project{Annotations: map[string]string{namespaceCountKey: value}})
		if item.namespaceCount != want {
			t.Errorf("namespace count of a %d-byte value = %q, want %q", len(value), item.namespaceCount, want)
		}
		if _, ok := item.Annotations[namespaceCountKey]; ok {
			t.Error("the pruned annotations have the namespace count")
		}
	}
}

// gammaEvent returns the project of a Project event of p-gamma in c-1 with
// the namespace count value.
func gammaEvent(t *testing.T, syncer *Syncer, value string) watchedProject {
	t.Helper()
	raw := `{"metadata":{"name":"p-gamma","namespace":"c-1","resourceVersion":"5","labels":{},` +
		`"annotations":{"managed.drover.evil8.io/namespace-count":"` + value + `"}},` +
		`"spec":{"clusterName":"c-1","displayName":"Gamma"}}`
	item, err := syncer.decodeProject(json.RawMessage(raw))
	if err != nil {
		t.Fatalf("decode the project event: %v", err)
	}
	return item
}

// gammaNamespace returns the namespace name of p-gamma in c-1, as the sync
// keeps it.
func gammaNamespace(t *testing.T, syncer *Syncer, name string) namespace {
	t.Helper()
	raw := `{"metadata":{"name":"` + name + `","resourceVersion":"1",` +
		`"labels":{"field.cattle.io/projectId":"p-gamma"},` +
		`"annotations":{"field.cattle.io/projectId":"c-1:p-gamma"}}}`
	var item namespace
	if err := json.Unmarshal([]byte(raw), &item); err != nil {
		t.Fatalf("decode the namespace: %v", err)
	}
	return syncer.pruneNamespace(item)
}

// newGammaSyncer returns a syncer whose snapshot has only p-gamma of c-1,
// with the namespace count value.
func newGammaSyncer(t *testing.T, value string, options ...func(*fakeRancher)) (*Syncer, *fakeRancher) {
	t.Helper()
	rancher := newFakeRancher(t, options...)
	syncer, _ := newSyncer(t, rancher, tokenFile(t, serviceToken))
	syncer.setClusters(map[string]map[string]project{"c-1": {"p-gamma": gammaEvent(t, syncer, value).project}})
	return syncer, rancher
}

// checkGamma checks the count writes, the count of the sets, and the
// snapshot value of p-gamma.
func checkGamma(t *testing.T, syncer *Syncer, rancher *fakeRancher, writes []string, count int) {
	t.Helper()
	if got := countWrites(t, rancher.all()); !slices.Equal(got, writes) {
		t.Errorf("count writes = %v, want %v", got, writes)
	}
	if got, _ := syncer.counts.count("c-1", "p-gamma"); got != count {
		t.Errorf("count of the sets = %d, want %d", got, count)
	}
	if got, want := syncer.projectsOf("c-1")["p-gamma"].namespaceCount, strconv.Itoa(count); got != want {
		t.Errorf("namespace count in the snapshot = %q, want %q", got, want)
	}
}

// TestCountKeepsAMoveOfTheWorkerDuringTheNamespaceList checks a namespace
// that the worker counts while the namespace list of the run is in progress,
// and that the list does not have.
func TestCountKeepsAMoveOfTheWorkerDuringTheNamespaceList(t *testing.T) {
	t.Parallel()
	for _, earlier := range []bool{true, false} {
		t.Run("sets of an earlier list "+strconv.FormatBool(earlier), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			syncer, rancher := newGammaSyncer(t, "0")
			if earlier {
				syncer.counts.replace("c-1", nil)
			}

			syncer.counts.beginList("c-1")
			syncer.applyCount(ctx, "c-1", patchItem{target: gammaNamespace(t, syncer, "gamma-new"), origin: originWatch}, false)
			syncer.counts.replace("c-1", nil)
			var run counters
			syncer.writeCounts(ctx, serviceToken, "c-1", syncer.projectsOf("c-1"), &run)

			checkGamma(t, syncer, rancher, []string{"c-1/p-gamma=1"}, 1)
		})
	}
}

// TestCountWritesOfTheRunAndTheWorkerTakeTurns checks a worker move while the
// count patch of the run is in progress. The fake holds the first patch until
// the worker moved gamma-new.
func TestCountWritesOfTheRunAndTheWorkerTakeTurns(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	arrived, release := make(chan struct{}), make(chan struct{})
	var first sync.Once
	syncer, rancher := newGammaSyncer(t, "1", onPatch(func(string) {
		first.Do(func() {
			close(arrived)
			<-release
		})
	}))
	// The list of the run no longer has the namespace of the count 1.
	syncer.counts.replace("c-1", nil)

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		var run counters
		syncer.writeCounts(ctx, serviceToken, "c-1", syncer.projectsOf("c-1"), &run)
	}()
	<-arrived
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		syncer.applyCount(ctx, "c-1", patchItem{target: gammaNamespace(t, syncer, "gamma-new"), origin: originWatch}, false)
	}()
	deadline := time.Now().Add(10 * time.Second)
	for count, _ := syncer.counts.count("c-1", "p-gamma"); count != 1; count, _ = syncer.counts.count("c-1", "p-gamma") {
		if time.Now().After(deadline) {
			t.Fatal("the worker did not move gamma-new after 10 s")
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	<-runDone
	<-workerDone

	checkGamma(t, syncer, rancher, []string{"c-1/p-gamma=0", "c-1/p-gamma=1"}, 1)
}

// TestProjectWatchCorrectsTheCountOfAStaleEvent checks a project watch that
// lags behind the worker. The event of an earlier write arrives after a
// later write, and the event of the later write arrives last.
func TestProjectWatchCorrectsTheCountOfAStaleEvent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	syncer, rancher := newGammaSyncer(t, "1")
	syncer.counts.replace("c-1", []namespace{gammaNamespace(t, syncer, "gamma-old")})

	syncer.applyCount(ctx, "c-1", patchItem{target: gammaNamespace(t, syncer, "gamma-old"), origin: originWatch, deleted: true}, false)
	syncer.applyProject(ctx, nil, watchModified, gammaEvent(t, syncer, "1"))
	syncer.applyCount(ctx, "c-1", patchItem{target: gammaNamespace(t, syncer, "gamma-new"), origin: originWatch}, false)
	syncer.applyProject(ctx, nil, watchModified, gammaEvent(t, syncer, "0"))
	checkGamma(t, syncer, rancher, []string{"c-1/p-gamma=0", "c-1/p-gamma=0", "c-1/p-gamma=1", "c-1/p-gamma=1"}, 1)

	// The event of the last write has the count of the sets.
	syncer.applyProject(ctx, nil, watchModified, gammaEvent(t, syncer, "1"))
	checkGamma(t, syncer, rancher, []string{"c-1/p-gamma=0", "c-1/p-gamma=0", "c-1/p-gamma=1", "c-1/p-gamma=1"}, 1)
}

// TestCountTakesNoItemOfTheLister checks that a lister item does not move a
// namespace. The lister can list gamma-one before a change that the worker
// handled already.
func TestCountTakesNoItemOfTheLister(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	syncer, rancher := newGammaSyncer(t, "0")
	syncer.counts.replace("c-1", nil)
	item := patchItem{target: gammaNamespace(t, syncer, "gamma-one"), origin: originProject}

	syncer.applyCount(ctx, "c-1", item, false)
	checkGamma(t, syncer, rancher, nil, 0)

	item.origin = originWatch
	syncer.applyCount(ctx, "c-1", item, false)
	checkGamma(t, syncer, rancher, []string{"c-1/p-gamma=1"}, 1)
}
