package projectsync

import (
	"context"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"sync"

	"github.com/evil8io/drover/internal/rancherclient"
)

const (
	// namespaceCountKey is the Project annotation with the count of the
	// namespaces of the project. An admission policy of the chart reads it
	// on a Project DELETE.
	namespaceCountKey = managedPrefix + "namespace-count"

	// maxCountValue is the length of the longest value that strconv.Itoa
	// returns for an int64.
	maxCountValue = 20
)

// namespaceSets has, per cluster, the project of each namespace and the
// namespaces of each project. Every read and every write takes mu, so the
// methods change the maps in place.
type namespaceSets struct {
	mu       sync.Mutex
	clusters map[string]*clusterSets
	// journals has the moves of each cluster whose namespace list is in
	// progress, in order. replace applies them again on top of the list.
	journals map[string][]namespaceMove
	// writes has the lock of the count writes of each cluster.
	writes map[string]*sync.Mutex
}

// clusterSets are the sets of one cluster. A namespace in no project is in
// no set.
type clusterSets struct {
	projectOf map[string]string
	members   map[string]map[string]struct{}
}

// namespaceMove is one call of move.
type namespaceMove struct {
	name, project string
}

// beginList starts the journal of cluster. The reconcile run calls it before
// the namespace list of cluster, and replace or endList ends it.
func (n *namespaceSets) beginList(cluster string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.journals == nil {
		n.journals = make(map[string][]namespaceMove)
	}
	n.journals[cluster] = []namespaceMove{}
}

// endList ends the journal of cluster without a replace, after a failed list.
func (n *namespaceSets) endList(cluster string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.journals, cluster)
}

// replace sets the sets of cluster from a full namespace list, and applies
// the journal of the list on top of it. The worker can handle an event while
// the list request runs, and the list can be older than that event.
func (n *namespaceSets) replace(cluster string, items []namespace) {
	sets := &clusterSets{projectOf: make(map[string]string), members: make(map[string]map[string]struct{})}
	for _, item := range items {
		sets.add(item.Metadata.Name, projectOf(item, cluster))
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, change := range n.journals[cluster] {
		sets.set(change.name, change.project)
	}
	delete(n.journals, cluster)
	if n.clusters == nil {
		n.clusters = make(map[string]*clusterSets)
	}
	n.clusters[cluster] = sets
}

// retain removes the sets and the journal of every cluster that clusters
// does not name.
func (n *namespaceSets) retain(clusters []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	maps.DeleteFunc(n.clusters, func(cluster string, _ *clusterSets) bool {
		return !slices.Contains(clusters, cluster)
	})
	maps.DeleteFunc(n.journals, func(cluster string, _ []namespaceMove) bool {
		return !slices.Contains(clusters, cluster)
	})
}

// move puts the namespace name of cluster into the set of project, and
// removes it from its old set. An empty project puts the namespace in no
// set. move returns the projects whose count changed. While a namespace list
// of cluster is in progress, move also adds the move to the journal. Before
// the first replace of cluster, move changes no set and returns nil, because
// a count from events alone misses the namespaces without an event.
func (n *namespaceSets) move(cluster, name, project string) []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if journal, ok := n.journals[cluster]; ok {
		n.journals[cluster] = append(journal, namespaceMove{name: name, project: project})
	}
	sets := n.clusters[cluster]
	if sets == nil {
		return nil
	}
	return sets.set(name, project)
}

// writeLock returns the lock of the count writes of cluster.
func (n *namespaceSets) writeLock(cluster string) *sync.Mutex {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.writes == nil {
		n.writes = make(map[string]*sync.Mutex)
	}
	lock, ok := n.writes[cluster]
	if !ok {
		lock = &sync.Mutex{}
		n.writes[cluster] = lock
	}
	return lock
}

// count returns the count of the namespaces of project in cluster. It
// returns false before the first replace of cluster.
func (n *namespaceSets) count(cluster, project string) (int, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	sets := n.clusters[cluster]
	if sets == nil {
		return 0, false
	}
	return len(sets.members[project]), true
}

// set moves the namespace name to the set of project, and returns the
// projects whose count changed.
func (c *clusterSets) set(name, project string) []string {
	old := c.projectOf[name]
	if old == project {
		return nil
	}
	var changed []string
	if old != "" {
		delete(c.members[old], name)
		if len(c.members[old]) == 0 {
			delete(c.members, old)
		}
		delete(c.projectOf, name)
		changed = append(changed, old)
	}
	if project != "" {
		c.add(name, project)
		changed = append(changed, project)
	}
	return changed
}

func (c *clusterSets) add(name, project string) {
	if project == "" {
		return
	}
	if c.members[project] == nil {
		c.members[project] = make(map[string]struct{})
	}
	c.members[project][name] = struct{}{}
	c.projectOf[name] = project
}

// pruneNamespaceCount returns the value that the service keeps in memory for
// the namespace count annotation. A longer value is no count of the service,
// so it returns "" for it.
func pruneNamespaceCount(value string) string {
	if len(value) > maxCountValue {
		return ""
	}
	return value
}

// countedProject returns the project that the namespace of item counts for.
// A namespace counts until the API server deletes it. refreshed marks an item
// that the worker read again, and a refreshed namespace with a
// deletionTimestamp is in Terminating.
func countedProject(item patchItem, cluster string, refreshed bool) string {
	if item.deleted && (!refreshed || item.target.Metadata.DeletionTimestamp == "") {
		return ""
	}
	return projectOf(item.target, cluster)
}

// writeCounts writes the namespace count of every project of cluster whose
// annotation has another count. It counts each failed patch in run. It takes
// the write lock per project, so that the worker can write between two
// projects.
func (s *Syncer) writeCounts(ctx context.Context, token, cluster string, projects map[string]project, run *counters) {
	fixed := func() (string, error) { return token, nil }
	for _, name := range slices.Sorted(maps.Keys(projects)) {
		if !s.writeCount(ctx, cluster, name, fixed) {
			run.errors++
		}
	}
}

// applyCount moves the namespace of item to the set of its project, and
// writes the count of each project whose count changed. It skips an item of
// the lister, because the lister can list a namespace before a change that
// the worker handled already. A patch that fails is a warning, and the next
// reconcile run repeats the work.
func (s *Syncer) applyCount(ctx context.Context, cluster string, item patchItem, refreshed bool) {
	if item.origin == originProject {
		return
	}
	for _, name := range s.counts.move(cluster, item.target.Metadata.Name, countedProject(item, cluster, refreshed)) {
		s.writeCount(ctx, cluster, name, s.readToken)
	}
}

// correctCount writes the namespace count of project name of cluster after
// a project event, when the stored value differs from the sets. The project
// watch can lag behind the worker, and the value of an old event would stop
// the next write of the worker. The write takes a token of the limiter of
// the cluster.
func (s *Syncer) correctCount(ctx context.Context, watches *watchSet, cluster, name string) {
	if watch := watches.get(cluster); watch != nil {
		ctx = withLimiter(ctx, watch.limiter)
	}
	s.writeCount(ctx, cluster, name, s.readToken)
}

// writeCount writes the namespace count of project name of cluster, when the
// snapshot has the project with another count. The write lock of the cluster
// covers the compare, the patch, and the store, so that two writers never
// compare against the same value. token returns the token of the patch. It
// returns false on an error.
func (s *Syncer) writeCount(ctx context.Context, cluster, name string, token func() (string, error)) bool {
	lock := s.counts.writeLock(cluster)
	lock.Lock()
	defer lock.Unlock()
	count, ok := s.countChange(cluster, name)
	if !ok {
		return true
	}
	value, err := token()
	if err != nil {
		s.logFailure(ctx, slog.LevelWarn, "the namespace count patch failed", err, "cluster", cluster, "project", name)
		return false
	}
	return s.patchCount(ctx, value, cluster, name, count)
}

// readToken reads the token file.
func (s *Syncer) readToken() (string, error) {
	return rancherclient.ReadToken(s.tokenFile)
}

// countChange returns the namespace count of project name of cluster, and
// true when the snapshot has the project with another count.
func (s *Syncer) countChange(cluster, name string) (int, bool) {
	item, ok := s.projectsOf(cluster)[name]
	if !ok {
		return 0, false
	}
	count, ok := s.counts.count(cluster, name)
	if !ok || strconv.Itoa(count) == item.namespaceCount {
		return 0, false
	}
	return count, true
}

// patchCount writes count into the namespace count annotation of the project,
// and stores the value in the snapshot. A 404 or a 409 answer is not an
// error. It returns false on an error.
func (s *Syncer) patchCount(ctx context.Context, token, cluster, name string, count int) bool {
	value := strconv.Itoa(count)
	err := s.patchProjectAnnotation(ctx, token, cluster, name, namespaceCountKey, &value)
	if hasStatus(err, http.StatusNotFound, http.StatusConflict) {
		s.logger.DebugContext(ctx, "the namespace count patch is skipped",
			"cluster", cluster, "project", name, "error", err.Error())
		return true
	}
	if err != nil {
		s.logFailure(ctx, slog.LevelWarn, "the namespace count patch failed", err, "cluster", cluster, "project", name)
		return false
	}
	s.setNamespaceCount(cluster, name, value)
	s.logger.InfoContext(ctx, "the namespace count is written", "cluster", cluster, "project", name, "count", count)
	return true
}

// patchProjectAnnotation writes value into the annotation key of the Project
// object of project name in cluster, with a JSON merge patch of that key only.
// A nil value removes the annotation.
func (s *Syncer) patchProjectAnnotation(ctx context.Context, token, cluster, name, key string, value *string) error {
	path := clusterPath(rancherCluster) + managementProjects + cluster + "/projects/" + name
	body := map[string]any{"metadata": map[string]any{"annotations": map[string]*string{key: value}}}
	return s.send(ctx, token, http.MethodPatch, path, body, nil)
}
