package projectsync

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	alphaOnePath = "/k8s/clusters/c-1/api/v1/namespaces/alpha-one"

	projectBookmarkEvent = `{"type":"BOOKMARK","object":{"kind":"Project",` +
		`"apiVersion":"management.cattle.io/v3","metadata":{"resourceVersion":"50"}}}`
	expiredBody = `{"kind":"Status","reason":"Expired","message":"too old resource version: 5 (9)"}`
)

// alphaFrame returns a project watch frame of eventType with the Project
// object of p-alpha. The object has the state that the project list of the
// fake gives, plus keys that Rancher writes itself. edit changes the object
// first, when it is not nil.
func alphaFrame(t *testing.T, eventType string, edit func(*projectObject)) string {
	t.Helper()
	var object projectObject
	object.Metadata.Name = "p-alpha"
	object.Metadata.Namespace = "c-1"
	object.Metadata.ResourceVersion = "40"
	object.Metadata.Labels = map[string]string{
		"cost-center":       "cc-1",
		"team":              "team-a",
		"cattle.io/creator": "norman",
	}
	object.Metadata.Annotations = map[string]string{
		"owner": "alpha@example.com",
		"note":  "outside the allow list",
		"lifecycle.cattle.io/create.mgmt-project-rbac-remove": "true",
		"authz.management.cattle.io/creator-role-bindings":    "{}",
	}
	object.Spec.ClusterName = "c-1"
	object.Spec.DisplayName = "Alpha"
	if edit != nil {
		edit(&object)
	}
	frame, err := json.Marshal(struct {
		Type   string        `json:"type"`
		Object projectObject `json:"object"`
	}{Type: eventType, Object: object})
	if err != nil {
		t.Fatalf("encode the project frame: %v", err)
	}
	return string(frame)
}

// projectTest has a syncer after one reconcile run, with a worker and a
// lister per cluster, and no namespace watch.
type projectTest struct {
	rancher *fakeRancher
	syncer  *Syncer
	logs    *syncBuffer
	watches *watchSet
	// before is the count of the requests before the project stream.
	before int
}

func newProjectTest(t *testing.T, options []func(*fakeRancher), configs ...func(*Config)) *projectTest {
	t.Helper()
	rancher := newFakeRancher(t, options...)
	syncer, logs := newSyncer(t, rancher, tokenFile(t, serviceToken), configs...)
	syncer.reconcile(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	watches := syncer.newWatchSet()
	for _, cluster := range []string{"c-1", "c-2"} {
		watch := newClusterWatch(syncer.patchRate)
		watch.cancel = cancel
		watches.watchers[cluster] = watch
		watches.wg.Add(2)
		go func() {
			defer watches.wg.Done()
			syncer.patchWorker(ctx, cluster, watch)
		}()
		go func() {
			defer watches.wg.Done()
			syncer.projectLister(ctx, cluster, watch)
		}()
	}
	t.Cleanup(watches.stop)

	return &projectTest{rancher: rancher, syncer: syncer, logs: logs, watches: watches, before: len(rancher.all())}
}

// stream reads one project watch stream from resourceVersion until it ends,
// and waits until the queues are empty.
func (p *projectTest) stream(t *testing.T, resourceVersion string) (string, error) {
	t.Helper()
	version, err := p.syncer.streamProjects(context.Background(), resourceVersion, p.watches)
	p.wait(t)
	return version, err
}

// wait waits until the project queue and then the patch queue of every
// cluster is idle. The lister fills the patch queue before it ends its
// project, so an idle project queue has put every namespace.
func (p *projectTest) wait(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for _, cluster := range []string{"c-1", "c-2"} {
		watch := p.watches.get(cluster)
		for !watch.projects.idle() || !watch.patches.idle() {
			if time.Now().After(deadline) {
				t.Fatalf("the queues of %s have work after 10 s", cluster)
			}
			time.Sleep(time.Millisecond)
		}
	}
}

// calls returns the requests after the reconcile run, without the watch
// requests.
func (p *projectTest) calls() []recorded {
	var out []recorded
	for _, request := range p.rancher.all()[p.before:] {
		if request.query.Get("watch") != "true" {
			out = append(out, request)
		}
	}
	return out
}

// patched returns the paths of the patch requests after the reconcile run,
// sorted.
func (p *projectTest) patched() []string {
	var paths []string
	for _, request := range p.calls() {
		if request.method == http.MethodPatch {
			paths = append(paths, request.path)
		}
	}
	slices.Sort(paths)
	return paths
}

// lastBody returns the body of the last patch of path after the reconcile run.
func (p *projectTest) lastBody(t *testing.T, path string) string {
	t.Helper()
	var body string
	for _, request := range p.calls() {
		if request.method == http.MethodPatch && request.path == path {
			body = request.body
		}
	}
	if body == "" {
		t.Fatalf("no patch of %s", path)
	}
	return body
}

func TestProjectWatchPatchesTheNamespacesOfAChangedProject(t *testing.T) {
	t.Parallel()
	frame := alphaFrame(t, watchModified, func(object *projectObject) {
		object.Metadata.Labels["cost-center"] = "cc-9"
	})
	p := newProjectTest(t, []func(*fakeRancher){watchingProjects(frame)})

	version, err := p.stream(t, "")
	if err != nil {
		t.Fatalf("stream the project watch: %v", err)
	}
	if version != "40" {
		t.Errorf("resource version = %q, want 40", version)
	}

	var lists []recorded
	for _, request := range p.calls() {
		if request.method == http.MethodGet {
			lists = append(lists, request)
		}
	}
	if len(lists) != 1 || lists[0].path != alphaListPath {
		t.Fatalf("list requests = %v, want one list of %s", lists, alphaListPath)
	}
	if got, want := lists[0].query.Get("labelSelector"), "field.cattle.io/projectId=p-alpha"; got != want {
		t.Errorf("label selector = %q, want %q", got, want)
	}

	want := []string{alphaMovedPath, alphaOnePath, alphaTwoPath}
	if got := p.patched(); !slices.Equal(got, want) {
		t.Fatalf("patched paths = %v, want %v", got, want)
	}
	if got, want := p.lastBody(t, alphaOnePath), `{"metadata":{"labels":{"cost-center":"cc-9"}}}`; got != want {
		t.Errorf("patch of %s = %s, want %s", alphaOnePath, got, want)
	}
	for _, name := range []string{"alpha-moved", "alpha-one", "alpha-two"} {
		line := `msg="namespace patched" cluster=c-1 namespace=` + name + ` project=p-alpha origin=project`
		if !strings.Contains(p.logs.String(), line) {
			t.Errorf("no patched line with origin project for %s:\n%s", name, p.logs.String())
		}
	}
	line := `msg="project changed" cluster=c-1 project=c-1:p-alpha labels=[cost-center] annotations=[] name_changed=false`
	if !strings.Contains(p.logs.String(), line) {
		t.Errorf("no change line:\n%s", p.logs.String())
	}
	if got := p.syncer.projectsOf("c-1")["p-alpha"].Labels["cost-center"]; got != "cc-9" {
		t.Errorf("cost-center of p-alpha in the snapshot = %q, want cc-9", got)
	}
}

func TestProjectWatchSendsNoRequestForAnEqualProject(t *testing.T) {
	t.Parallel()
	p := newProjectTest(t, []func(*fakeRancher){watchingProjects(
		alphaFrame(t, watchAdded, nil),
		alphaFrame(t, watchModified, nil),
	)})

	if _, err := p.stream(t, ""); err != nil {
		t.Fatalf("stream the project watch: %v", err)
	}
	if got := p.calls(); len(got) != 0 {
		t.Errorf("requests = %v, want none", got)
	}
	if strings.Contains(p.logs.String(), `msg="project changed"`) {
		t.Errorf("a change line for an equal project:\n%s", p.logs.String())
	}
}

func TestProjectWatchIgnoresAKeyOutsideTheAllowList(t *testing.T) {
	t.Parallel()
	frame := alphaFrame(t, watchModified, func(object *projectObject) {
		object.Metadata.Labels["team"] = "team-b"
		object.Metadata.Labels["cattle.io/creator"] = "someone-else"
		object.Metadata.Annotations["note"] = "a new note"
		object.Metadata.Annotations["lifecycle.cattle.io/create.mgmt-project-rbac-remove"] = "false"
	})
	p := newProjectTest(t, []func(*fakeRancher){watchingProjects(frame)})

	if _, err := p.stream(t, ""); err != nil {
		t.Fatalf("stream the project watch: %v", err)
	}
	if got := p.calls(); len(got) != 0 {
		t.Errorf("requests = %v, want none", got)
	}
}

// TestProjectWatchSetsANewDisplayName checks that the worker clears its name
// cache. The worker patches a namespace of p-alpha first, so its cache has
// the old display name when the project changes.
func TestProjectWatchSetsANewDisplayName(t *testing.T) {
	t.Parallel()
	const (
		nameLabel      = "example.com/project-name"
		nameAnnotation = "example.com/project-display-name"
	)
	frame := alphaFrame(t, watchModified, func(object *projectObject) {
		object.Spec.DisplayName = "Alpha Two"
	})
	p := newProjectTest(t, []func(*fakeRancher){watchingProjects(frame)}, func(cfg *Config) {
		cfg.NameLabel = nameLabel
		cfg.NameAnnotation = nameAnnotation
	})

	var target namespace
	target.Metadata.Name = "alpha-two"
	target.Metadata.Labels = map[string]string{projectLabel: "p-alpha"}
	target.Metadata.Annotations = map[string]string{projectAnnotation: "c-1:p-alpha"}
	p.watches.get("c-1").patches.put(patchItem{target: target, origin: originWatch})
	p.wait(t)
	if got := p.lastBody(t, alphaTwoPath); !strings.Contains(got, `"`+nameLabel+`":"Alpha"`) {
		t.Fatalf("first patch of %s = %s, want the name label Alpha", alphaTwoPath, got)
	}

	if _, err := p.stream(t, ""); err != nil {
		t.Fatalf("stream the project watch: %v", err)
	}
	got := p.lastBody(t, alphaTwoPath)
	for _, want := range []string{`"` + nameLabel + `":"Alpha-Two"`, `"` + nameAnnotation + `":"Alpha Two"`} {
		if !strings.Contains(got, want) {
			t.Errorf("last patch of %s = %s, want %s", alphaTwoPath, got, want)
		}
	}
	if !strings.Contains(p.logs.String(), `msg="project changed" cluster=c-1 project=c-1:p-alpha labels=[] annotations=[] name_changed=true`) {
		t.Errorf("no change line with a new name:\n%s", p.logs.String())
	}
}

func TestProjectWatchSkipsAClusterOutsideTheSnapshot(t *testing.T) {
	t.Parallel()
	frame := alphaFrame(t, watchModified, func(object *projectObject) {
		object.Metadata.Namespace = "c-9"
		object.Spec.ClusterName = "c-9"
		object.Metadata.Labels["cost-center"] = "cc-9"
	})
	p := newProjectTest(t, []func(*fakeRancher){watchingProjects(frame)})

	if _, err := p.stream(t, ""); err != nil {
		t.Fatalf("stream the project watch: %v", err)
	}
	if got := p.calls(); len(got) != 0 {
		t.Errorf("requests = %v, want none", got)
	}
	if p.syncer.projectsOf("c-9") != nil {
		t.Error("the snapshot has cluster c-9")
	}
	if !strings.Contains(p.logs.String(), `msg="project skipped, the cluster is not in the snapshot" cluster=c-9`) {
		t.Errorf("no skipped line for c-9:\n%s", p.logs.String())
	}
}

func TestProjectWatchRemovesADeletedProject(t *testing.T) {
	t.Parallel()
	p := newProjectTest(t, []func(*fakeRancher){
		watchingProjects(alphaFrame(t, watchDeleted, nil)),
		watching("c-1", modifiedEvent),
	})

	if _, err := p.stream(t, ""); err != nil {
		t.Fatalf("stream the project watch: %v", err)
	}
	if got := p.calls(); len(got) != 0 {
		t.Errorf("requests = %v, want none", got)
	}
	if _, ok := p.syncer.projectsOf("c-1")["p-alpha"]; ok {
		t.Fatal("the snapshot still has p-alpha")
	}

	watch := p.watches.get("c-1")
	if _, err := p.syncer.streamNamespaces(context.Background(), "c-1", "", watch.patches); err != nil {
		t.Fatalf("stream the namespace watch: %v", err)
	}
	p.wait(t)
	if got := requestsOfPath(p.rancher, alphaNewPath); len(got) != 0 {
		t.Errorf("patch requests of %s = %d, want 0", alphaNewPath, len(got))
	}
	if !strings.Contains(p.logs.String(), `msg="namespace skipped" cluster=c-1 namespace=alpha-new project=p-alpha`) {
		t.Errorf("no skipped line for alpha-new:\n%s", p.logs.String())
	}
}

// TestProjectWatchSkipsANamespaceThatIsNotTheClusterName uses a Project in a
// cluster of the snapshot, so that the mismatch is a warning.
func TestProjectWatchSkipsANamespaceThatIsNotTheClusterName(t *testing.T) {
	t.Parallel()
	frame := alphaFrame(t, watchModified, func(object *projectObject) {
		object.Spec.ClusterName = "c-2"
		object.Metadata.Labels["cost-center"] = "cc-9"
	})
	p := newProjectTest(t, []func(*fakeRancher){watchingProjects(frame)})

	if _, err := p.stream(t, ""); err != nil {
		t.Fatalf("stream the project watch: %v", err)
	}
	if got := p.calls(); len(got) != 0 {
		t.Errorf("requests = %v, want none", got)
	}
	if got := p.syncer.projectsOf("c-1")["p-alpha"].Labels["cost-center"]; got != "cc-1" {
		t.Errorf("cost-center of p-alpha in the snapshot = %q, want cc-1", got)
	}
	want := `level=WARN msg="the project namespace is not its cluster name" project=c-1:p-alpha cluster_name=c-2`
	if !strings.Contains(p.logs.String(), want) {
		t.Errorf("no warning for the mismatch:\n%s", p.logs.String())
	}
}

// TestProjectWatchGivesNoWarningForAMismatchOutsideTheSnapshot checks that a
// malformed Project of a cluster that the service does not manage logs no
// warning, also when the stream replays it.
func TestProjectWatchGivesNoWarningForAMismatchOutsideTheSnapshot(t *testing.T) {
	t.Parallel()
	frame := alphaFrame(t, watchModified, func(object *projectObject) {
		object.Metadata.Namespace = "c-9"
		object.Spec.ClusterName = "c-8"
	})
	p := newProjectTest(t, []func(*fakeRancher){watchingProjects(frame, frame)})

	if _, err := p.stream(t, ""); err != nil {
		t.Fatalf("stream the project watch: %v", err)
	}
	if got := p.calls(); len(got) != 0 {
		t.Errorf("requests = %v, want none", got)
	}
	if strings.Contains(p.logs.String(), "level=WARN") {
		t.Errorf("a warning for a Project outside the snapshot:\n%s", p.logs.String())
	}
	want := `level=DEBUG msg="project skipped, the cluster is not in the snapshot" cluster=c-9 project=c-9:p-alpha`
	if got := strings.Count(p.logs.String(), want); got != 2 {
		t.Errorf("skipped lines for c-9:p-alpha = %d, want 2:\n%s", got, p.logs.String())
	}
}

func TestProjectWatchSkipsANameThatIsNotALabelValue(t *testing.T) {
	t.Parallel()
	frame := alphaFrame(t, watchModified, func(object *projectObject) {
		object.Metadata.Name = "p-alpha-"
		object.Metadata.Labels["cost-center"] = "cc-9"
	})
	p := newProjectTest(t, []func(*fakeRancher){watchingProjects(frame)})

	if _, err := p.stream(t, ""); err != nil {
		t.Fatalf("stream the project watch: %v", err)
	}
	if got := p.calls(); len(got) != 0 {
		t.Errorf("requests = %v, want none", got)
	}
	if _, ok := p.syncer.projectsOf("c-1")["p-alpha-"]; ok {
		t.Error("the snapshot has p-alpha-")
	}
	if !strings.Contains(p.logs.String(), `msg="project skipped, the name is not a label value" project=c-1:p-alpha-`) {
		t.Errorf("no skipped line for p-alpha-:\n%s", p.logs.String())
	}
}

func TestProjectWatchSkipsANameAbove63Characters(t *testing.T) {
	t.Parallel()
	name := "p-" + strings.Repeat("a", 62)
	frame := alphaFrame(t, watchModified, func(object *projectObject) {
		object.Metadata.Name = name
	})
	p := newProjectTest(t, []func(*fakeRancher){watchingProjects(frame)})

	if _, err := p.stream(t, ""); err != nil {
		t.Fatalf("stream the project watch: %v", err)
	}
	if got := p.calls(); len(got) != 0 {
		t.Errorf("requests = %v, want none", got)
	}
	if _, ok := p.syncer.projectsOf("c-1")[name]; ok {
		t.Errorf("the snapshot has the name of %d characters", len(name))
	}
	want := `level=DEBUG msg="project skipped, the name is not a label value" project=c-1:` + name
	if !strings.Contains(p.logs.String(), want) {
		t.Errorf("no skipped line for the name of %d characters:\n%s", len(name), p.logs.String())
	}
}

// TestProjectListTakesATokenOfTheClusterLimiter checks that the lister and
// the worker share one limiter. At one token per second, the list takes the
// full bucket, so the second of the two patches comes about two seconds after
// the list. A lister without a token, or a worker with a limiter of its own,
// gives about one second.
func TestProjectListTakesATokenOfTheClusterLimiter(t *testing.T) {
	t.Parallel()
	frame := alphaFrame(t, watchModified, func(object *projectObject) {
		object.Metadata.Labels["cost-center"] = "cc-9"
	})
	page := `{"kind":"NamespaceList","items":[` + alphaOneItem + `,` + alphaTwoItem + `]}`
	p := newProjectTest(t, []func(*fakeRancher){
		watchingProjects(frame),
		listing("c-1", map[string]string{"": page}),
	}, func(cfg *Config) {
		cfg.PatchRate = 1
	})

	if _, err := p.stream(t, ""); err != nil {
		t.Fatalf("stream the project watch: %v", err)
	}

	var list, last time.Time
	for _, request := range p.calls() {
		switch request.method {
		case http.MethodGet:
			list = request.at
		case http.MethodPatch:
			last = request.at
		}
	}
	if got := p.patched(); !slices.Equal(got, []string{alphaOnePath, alphaTwoPath}) {
		t.Fatalf("patched paths = %v, want [%s %s]", got, alphaOnePath, alphaTwoPath)
	}
	if list.IsZero() {
		t.Fatal("no list request of p-alpha")
	}
	if gap := last.Sub(list); gap < 1500*time.Millisecond {
		t.Errorf("the last patch came %s after the list, want at least 1.5 s", gap)
	}
}

func TestProjectWatchWarnsAboutAForbiddenList(t *testing.T) {
	t.Parallel()
	frame := alphaFrame(t, watchModified, func(object *projectObject) {
		object.Metadata.Labels["cost-center"] = "cc-9"
	})
	p := newProjectTest(t, []func(*fakeRancher){watchingProjects(frame), forbid("c-1")})

	if _, err := p.stream(t, ""); err != nil {
		t.Fatalf("stream the project watch: %v", err)
	}
	if got := p.patched(); len(got) != 0 {
		t.Errorf("patched paths = %v, want none", got)
	}
	want := `msg="the service user has no binding on the cluster" cluster=c-1 project=p-alpha`
	if got := strings.Count(p.logs.String(), want); got != 1 {
		t.Errorf("warnings for the list of p-alpha = %d, want 1:\n%s", got, p.logs.String())
	}
}

func TestProjectWatchRequestCarriesTheWatchParameters(t *testing.T) {
	t.Parallel()
	p := newProjectTest(t, []func(*fakeRancher){
		watchingProjects(projectBookmarkEvent),
		watchingProjects(),
	})

	version, err := p.stream(t, "")
	if err != nil {
		t.Fatalf("stream the project watch: %v", err)
	}
	if version != "50" {
		t.Fatalf("resource version = %q, want 50", version)
	}
	if _, err := p.stream(t, version); err != nil {
		t.Fatalf("stream the project watch again: %v", err)
	}

	requests := projectWatchRequests(p.rancher)
	if len(requests) != 2 {
		t.Fatalf("project watch requests = %d, want 2", len(requests))
	}
	for i, request := range requests {
		for key, want := range map[string]string{
			"watch":               "true",
			"allowWatchBookmarks": "true",
			"timeoutSeconds":      strconv.Itoa(int(watchTimeout.Seconds())),
		} {
			if got := request.query.Get(key); got != want {
				t.Errorf("%s of request %d = %q, want %q", key, i, got, want)
			}
		}
		if request.query.Has("labelSelector") {
			t.Errorf("request %d has a label selector", i)
		}
		if got := request.header.Get("Authorization"); got != "Bearer "+serviceToken {
			t.Errorf("authorization of request %d = %q, want the service token", i, got)
		}
	}
	if requests[0].query.Has("resourceVersion") {
		t.Errorf("the first request has the resource version %q", requests[0].query.Get("resourceVersion"))
	}
	if got := requests[1].query.Get("resourceVersion"); got != "50" {
		t.Errorf("resource version of the next stream = %q, want 50", got)
	}
	for _, line := range []string{`msg="the project namespace is not its cluster name"`, `msg="project skipped`} {
		if strings.Contains(p.logs.String(), line) {
			t.Errorf("the bookmark gave the line %s:\n%s", line, p.logs.String())
		}
	}
}

func TestProjectWatchStartsAgainWithoutAnExpiredResourceVersion(t *testing.T) {
	t.Parallel()
	p := newProjectTest(t, []func(*fakeRancher){failProjectWatch(http.StatusGone, expiredBody)})

	version, err := p.stream(t, "50")
	if !errors.Is(err, errWatchExpired) {
		t.Fatalf("error = %v, want errWatchExpired", err)
	}
	if version != "" {
		t.Errorf("resource version = %q, want an empty value", version)
	}
}

func TestProjectQueueCoalescesAStormOfOneProject(t *testing.T) {
	t.Parallel()
	projects := newClusterWatch(1).projects
	for range 30 {
		projects.put("p-alpha")
	}
	projects.put("p-beta")

	ctx := context.Background()
	var got []string
	for !projects.idle() {
		name, ok := projects.next(ctx)
		if !ok {
			t.Fatal("next returned no project")
		}
		got = append(got, name)
		projects.done()
	}
	if want := []string{"p-alpha", "p-beta"}; !slices.Equal(got, want) {
		t.Errorf("projects = %v, want %v", got, want)
	}
}

func TestPatchQueueKeepsTheOriginOfTheNewerItem(t *testing.T) {
	t.Parallel()
	patches := newClusterWatch(1).patches
	var target namespace
	target.Metadata.Name = "alpha-two"
	patches.put(patchItem{target: target, origin: originWatch})
	patches.put(patchItem{target: target, origin: originProject})

	item, ok := patches.next(context.Background())
	if !ok {
		t.Fatal("next returned no namespace")
	}
	patches.done()
	if item.origin != originProject {
		t.Errorf("origin = %q, want %q", item.origin, originProject)
	}
	if !patches.idle() {
		t.Error("the queue has work after one namespace")
	}
}

func TestSetProjectKeepsTheMapsOfAnEarlierRead(t *testing.T) {
	t.Parallel()
	rancher := newFakeRancher(t)
	syncer, _ := newSyncer(t, rancher, tokenFile(t, serviceToken))
	clusters := map[string]map[string]project{
		"c-1": {"p-alpha": {ID: "c-1:p-alpha", ClusterID: "c-1", Name: "Alpha"}},
	}
	syncer.setClusters(clusters)
	before := syncer.projectsOf("c-1")

	if !syncer.setProject("c-1", "p-alpha", project{ID: "c-1:p-alpha", ClusterID: "c-1", Name: "Alpha Two"}) {
		t.Fatal("setProject of a known cluster returned false")
	}
	if !syncer.setProject("c-1", "p-new", project{ID: "c-1:p-new", ClusterID: "c-1", Name: "New"}) {
		t.Fatal("setProject of a new project returned false")
	}

	if got := before["p-alpha"].Name; got != "Alpha" {
		t.Errorf("name in the earlier read = %q, want Alpha", got)
	}
	if _, ok := before["p-new"]; ok {
		t.Error("the earlier read has p-new")
	}
	if got := clusters["c-1"]["p-alpha"].Name; got != "Alpha" {
		t.Errorf("name in the map of the reconcile run = %q, want Alpha", got)
	}
	after := syncer.projectsOf("c-1")
	if after["p-alpha"].Name != "Alpha Two" || after["p-new"].Name != "New" {
		t.Errorf("snapshot = %v, want p-alpha Alpha Two and p-new New", after)
	}
	if syncer.setProject("c-9", "p-alpha", project{ID: "c-9:p-alpha", ClusterID: "c-9"}) {
		t.Error("setProject of an unknown cluster returned true")
	}
	if syncer.projectsOf("c-9") != nil {
		t.Error("the snapshot has cluster c-9")
	}
}

func TestDeleteProjectKeepsTheMapsOfAnEarlierRead(t *testing.T) {
	t.Parallel()
	rancher := newFakeRancher(t)
	syncer, _ := newSyncer(t, rancher, tokenFile(t, serviceToken))
	clusters := map[string]map[string]project{
		"c-1": {"p-alpha": {ID: "c-1:p-alpha", ClusterID: "c-1", Name: "Alpha"}},
	}
	syncer.setClusters(clusters)
	before := syncer.projectsOf("c-1")

	if !syncer.deleteProject("c-1", "p-alpha") {
		t.Fatal("deleteProject of a known cluster returned false")
	}

	if _, ok := before["p-alpha"]; !ok {
		t.Error("the earlier read lost p-alpha")
	}
	if _, ok := clusters["c-1"]["p-alpha"]; !ok {
		t.Error("the map of the reconcile run lost p-alpha")
	}
	if _, ok := syncer.projectsOf("c-1")["p-alpha"]; ok {
		t.Error("the snapshot still has p-alpha")
	}
	if syncer.deleteProject("c-9", "p-alpha") {
		t.Error("deleteProject of an unknown cluster returned true")
	}
}
