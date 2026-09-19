package projectsync

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	alphaNewPath = "/k8s/clusters/c-1/api/v1/namespaces/alpha-new"

	// addedEvent and modifiedEvent are the two events of a new namespace.
	// Rancher sets the project label after the create, so the ADDED event has
	// no project.
	addedEvent = `{"type":"ADDED","object":{"kind":"Namespace","metadata":` +
		`{"name":"alpha-new","resourceVersion":"30","labels":{},"annotations":{}}}}`
	modifiedEvent = `{"type":"MODIFIED","object":{"kind":"Namespace","metadata":` +
		`{"name":"alpha-new","resourceVersion":"31",` +
		`"labels":{"field.cattle.io/projectId":"p-alpha"},"annotations":{}}}}`
	bookmarkEvent = `{"type":"BOOKMARK","object":{"kind":"Namespace","metadata":{"resourceVersion":"32"}}}`
	expiredEvent  = `{"type":"ERROR","object":{"kind":"Status","reason":"Expired",` +
		`"message":"too old resource version: 5 (9)"}}`

	terminatingBody = `{"kind":"Status","reason":"Forbidden",` +
		`"message":"unable to create new content in namespace alpha-new because it is being terminated"}`
)

// streamOnce fills the project snapshot with one reconcile run, then it reads
// the namespace watch of cluster c-1 until the stream ends.
func streamOnce(t *testing.T, options ...func(*fakeRancher)) (*fakeRancher, *syncBuffer, string, error) {
	t.Helper()
	rancher := newFakeRancher(t, options...)
	syncer, logs := newSyncer(t, rancher, tokenFile(t, serviceToken))
	ctx := context.Background()
	syncer.reconcile(ctx)
	version, err := syncer.streamNamespaces(ctx, "c-1", "")
	return rancher, logs, version, err
}

func watchRequests(rancher *fakeRancher) []recorded {
	var out []recorded
	for _, request := range rancher.all() {
		if request.query.Get("watch") == "true" {
			out = append(out, request)
		}
	}
	return out
}

func TestWatchPatchesTheNamespaceOfAnEvent(t *testing.T) {
	t.Parallel()
	rancher, _, version, err := streamOnce(t, watching("c-1", addedEvent, modifiedEvent))
	if err != nil {
		t.Fatalf("stream the namespace watch: %v", err)
	}
	if version != "31" {
		t.Errorf("resource version = %q, want 31", version)
	}

	patches := requestsOfPath(rancher, alphaNewPath)
	if len(patches) != 1 {
		t.Fatalf("patch requests of %s = %d, want 1", alphaNewPath, len(patches))
	}
	if got := patches[0].body; got != alphaTwoBody {
		t.Errorf("patch of %s = %s, want %s", alphaNewPath, got, alphaTwoBody)
	}
}

func TestWatchRequestCarriesTheWatchParameters(t *testing.T) {
	t.Parallel()
	rancher, _, _, err := streamOnce(t, watching("c-1", bookmarkEvent))
	if err != nil {
		t.Fatalf("stream the namespace watch: %v", err)
	}

	requests := watchRequests(rancher)
	if len(requests) != 1 {
		t.Fatalf("watch requests = %d, want 1", len(requests))
	}
	query := requests[0].query
	for key, want := range map[string]string{
		"labelSelector":       projectLabel,
		"allowWatchBookmarks": "true",
		"timeoutSeconds":      strconv.Itoa(int(watchTimeout.Seconds())),
	} {
		if got := query.Get(key); got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
	if got := requests[0].header.Get("Authorization"); got != "Bearer "+serviceToken {
		t.Errorf("authorization = %q, want the service token", got)
	}
}

func TestWatchTakesTheResourceVersionOfABookmark(t *testing.T) {
	t.Parallel()
	_, _, version, err := streamOnce(t, watching("c-1", modifiedEvent, bookmarkEvent))
	if err != nil {
		t.Fatalf("stream the namespace watch: %v", err)
	}
	if version != "32" {
		t.Errorf("resource version = %q, want 32", version)
	}
}

func TestWatchStartsAgainWithoutAnExpiredResourceVersion(t *testing.T) {
	t.Parallel()
	_, _, version, err := streamOnce(t, watching("c-1", expiredEvent))
	if !errors.Is(err, errWatchExpired) {
		t.Fatalf("error = %v, want errWatchExpired", err)
	}
	if version != "" {
		t.Errorf("resource version = %q, want an empty value", version)
	}
}

func TestWatchSkipsANamespaceThatIsGone(t *testing.T) {
	t.Parallel()
	_, logs, _, err := streamOnce(t,
		watching("c-1", modifiedEvent),
		failPatch("alpha-new", http.StatusNotFound, `{"kind":"Status","reason":"NotFound"}`))
	if err != nil {
		t.Fatalf("stream the namespace watch: %v", err)
	}
	if strings.Contains(logs.String(), `msg="the namespace patch failed"`) {
		t.Errorf("the patch of a namespace that is gone counts as a failure:\n%s", logs.String())
	}
	if !strings.Contains(logs.String(), `msg="namespace patch skipped" cluster=c-1 namespace=alpha-new`) {
		t.Errorf("no skipped line for alpha-new:\n%s", logs.String())
	}
}

func TestWatchSkipsANamespaceInTerminating(t *testing.T) {
	t.Parallel()
	_, logs, _, err := streamOnce(t,
		watching("c-1", modifiedEvent),
		failPatch("alpha-new", http.StatusForbidden, terminatingBody))
	if err != nil {
		t.Fatalf("stream the namespace watch: %v", err)
	}
	if strings.Contains(logs.String(), `msg="the namespace patch failed"`) {
		t.Errorf("the patch of a terminating namespace counts as a failure:\n%s", logs.String())
	}
}

func TestWatchCountsAPatchFailure(t *testing.T) {
	t.Parallel()
	_, logs, _, err := streamOnce(t,
		watching("c-1", modifiedEvent),
		failPatch("alpha-new", http.StatusInternalServerError, `{"kind":"Status","reason":"InternalError"}`))
	if err != nil {
		t.Fatalf("stream the namespace watch: %v", err)
	}
	if !strings.Contains(logs.String(), `msg="the namespace patch failed"`) {
		t.Errorf("no failure line for alpha-new:\n%s", logs.String())
	}
}

func TestWatchSkipsANamespaceOfAnUnknownProject(t *testing.T) {
	t.Parallel()
	event := `{"type":"MODIFIED","object":{"kind":"Namespace","metadata":` +
		`{"name":"alpha-new","resourceVersion":"31",` +
		`"labels":{"field.cattle.io/projectId":"p-gone"},"annotations":{}}}}`
	rancher, logs, _, err := streamOnce(t, watching("c-1", event))
	if err != nil {
		t.Fatalf("stream the namespace watch: %v", err)
	}
	if got := requestsOfPath(rancher, alphaNewPath); len(got) != 0 {
		t.Errorf("patch requests of %s = %d, want 0", alphaNewPath, len(got))
	}
	if !strings.Contains(logs.String(), `msg="namespace skipped" cluster=c-1 namespace=alpha-new project=p-gone`) {
		t.Errorf("no skipped line for alpha-new:\n%s", logs.String())
	}
}

func TestRunOpensOneWatchPerCluster(t *testing.T) {
	t.Parallel()
	rancher := newFakeRancher(t, watching("c-1", bookmarkEvent), watching("c-2", bookmarkEvent))
	syncer, _ := newSyncer(t, rancher, tokenFile(t, serviceToken))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		syncer.Run(ctx)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for len(watchClusters(rancher)) < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done

	if got := watchClusters(rancher); !slices.Equal(got, []string{"c-1", "c-2"}) {
		t.Errorf("watched clusters = %v, want [c-1 c-2]", got)
	}
}

// watchClusters returns the clusters that a watch request named, sorted and
// without a repeat.
func watchClusters(rancher *fakeRancher) []string {
	seen := make(map[string]struct{})
	for _, request := range watchRequests(rancher) {
		cluster := strings.Split(strings.TrimPrefix(request.path, "/k8s/clusters/"), "/")[0]
		seen[cluster] = struct{}{}
	}
	clusters := make([]string, 0, len(seen))
	for cluster := range seen {
		clusters = append(clusters, cluster)
	}
	slices.Sort(clusters)
	return clusters
}

func TestDesiredTakesTheNameKeyOnlyFromTheNamePath(t *testing.T) {
	t.Parallel()
	const key = "example.com/project-name"

	// The project has a label under the name key, and its display name has no
	// valid label value. The name path owns the key, so the namespace gets no
	// label at all.
	source := project{ID: "c-1:p-x", Name: "!!!", Labels: map[string]string{key: "from-the-project"}}
	change := desired(source, namespace{}, []string{key}, nil, key, "", "")

	if !change.empty() {
		t.Errorf("change = %+v, want an empty change", change)
	}
}

func TestDesiredKeepsAKeyThatTheServiceDoesNotOwn(t *testing.T) {
	t.Parallel()
	source := project{ID: "c-1:p-x", Labels: map[string]string{}}
	var target namespace
	target.Metadata.Labels = map[string]string{"cost-center": "set-by-the-tenant"}

	change := desired(source, target, []string{"cost-center"}, nil, "", "", "")

	if !change.empty() {
		t.Errorf("change = %+v, want an empty change", change)
	}
}
