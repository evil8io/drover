package projectsync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/evil8io/drover/internal/rancherclient"
)

const (
	watchAdded    = "ADDED"
	watchModified = "MODIFIED"
	watchDeleted  = "DELETED"
	watchBookmark = "BOOKMARK"
	watchError    = "ERROR"

	// watchTimeout is the timeoutSeconds of a watch request. The server ends
	// the stream after it, and the watcher opens a new one.
	watchTimeout = 5 * time.Minute

	minWatchBackoff = time.Second
	maxWatchBackoff = 30 * time.Second
	// watchBackoffReset is the time that a stream must stay open before the
	// backoff returns to its minimum.
	watchBackoffReset = 10 * time.Second
)

// errWatchExpired marks a watch that needs a new start without a resource
// version, because the server no longer has the history from that version.
var errWatchExpired = errors.New("the watch resource version is too old")

// watchSet has one namespace watch per cluster, and the project watch. Every
// reconcile run gives it the current cluster set, and the set starts and stops
// the watchers.
type watchSet struct {
	syncer *Syncer
	wg     sync.WaitGroup

	mu       sync.Mutex
	watchers map[string]*clusterWatch
	// stopProjects ends the project watch. It is nil until startProjects.
	stopProjects context.CancelFunc
}

// clusterWatch is the state of the watches of one cluster. The namespace
// stream and the lister put a namespace into patches, and the worker of the
// cluster takes it from there. The project watch puts a project name into
// projects, and the lister takes it from there. A value on reset tells the
// worker to clear its name cache. The worker and the lister share limiter.
type clusterWatch struct {
	cancel   context.CancelFunc
	patches  *queue[patchItem]
	projects *queue[string]
	reset    chan struct{}
	limiter  *limiter
}

func newClusterWatch(rate float64) *clusterWatch {
	return &clusterWatch{
		patches:  newQueue(func(item patchItem) string { return item.target.Metadata.Name }),
		projects: newQueue(func(name string) string { return name }),
		reset:    make(chan struct{}, 1),
		limiter:  newLimiter(rate, max(rate, 1)),
	}
}

// resetNames tells the worker to clear its name cache. A signal that waits
// already covers this one.
func (c *clusterWatch) resetNames() {
	select {
	case c.reset <- struct{}{}:
	default:
	}
}

func (s *Syncer) newWatchSet() *watchSet {
	return &watchSet{syncer: s, watchers: make(map[string]*clusterWatch)}
}

// update starts a watcher for every cluster that has none, and it stops the
// watcher of a cluster that clusters does not name.
func (w *watchSet) update(ctx context.Context, clusters []string) {
	if w == nil {
		return
	}
	want := make(map[string]struct{}, len(clusters))
	for _, cluster := range clusters {
		want[cluster] = struct{}{}
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	for cluster, watch := range w.watchers {
		if _, ok := want[cluster]; ok {
			continue
		}
		watch.cancel()
		delete(w.watchers, cluster)
	}
	for _, cluster := range clusters {
		if _, ok := w.watchers[cluster]; ok {
			continue
		}
		watchCtx, cancel := context.WithCancel(ctx)
		watch := newClusterWatch(w.syncer.patchRate)
		watch.cancel = cancel
		w.watchers[cluster] = watch
		w.wg.Add(3)
		go func() {
			defer w.wg.Done()
			w.syncer.watchCluster(watchCtx, cluster, watch.patches)
		}()
		go func() {
			defer w.wg.Done()
			w.syncer.patchWorker(watchCtx, cluster, watch)
		}()
		go func() {
			defer w.wg.Done()
			w.syncer.projectLister(watchCtx, cluster, watch)
		}()
	}
}

// startProjects starts the project watch, once per set. It lives until ctx
// ends or stop.
func (w *watchSet) startProjects(ctx context.Context) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopProjects != nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	w.stopProjects = cancel
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.syncer.watchProjects(ctx, w)
	}()
}

// get returns the watch of a cluster, or nil when the set has none.
func (w *watchSet) get(cluster string) *clusterWatch {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.watchers[cluster]
}

// resetNames tells every worker to clear its name cache. A reconcile run calls
// it, so that a new display name of a project reaches the namespaces, and so
// that the warning about an invalid display name comes again.
func (w *watchSet) resetNames() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, watch := range w.watchers {
		watch.resetNames()
	}
}

// stop ends every watcher, every worker, and the project watch, and it waits
// for the goroutines.
func (w *watchSet) stop() {
	if w == nil {
		return
	}
	w.mu.Lock()
	for cluster, watch := range w.watchers {
		watch.cancel()
		delete(w.watchers, cluster)
	}
	if w.stopProjects != nil {
		w.stopProjects()
	}
	w.mu.Unlock()
	w.wg.Wait()
}

// watchCluster keeps one namespace watch of a cluster open.
func (s *Syncer) watchCluster(ctx context.Context, cluster string, patches *queue[patchItem]) {
	s.keepWatching(ctx, kindNamespace, cluster, func(ctx context.Context, resourceVersion string) (string, error) {
		return s.streamNamespaces(ctx, cluster, resourceVersion, patches)
	})
}

// keepWatching keeps one watch open. open reads one stream from a resource
// version, and returns the resource version to start the next stream from. A
// stream that ends starts again, after a backoff that grows with each failure.
// The reconcile run covers the events that the gap misses. kind and cluster
// name the watch in the logs.
func (s *Syncer) keepWatching(ctx context.Context, kind, cluster string, open func(ctx context.Context, resourceVersion string) (string, error)) {
	var (
		resourceVersion string
		backoff         = minWatchBackoff
	)
	for ctx.Err() == nil {
		start := time.Now()
		next, err := open(ctx, resourceVersion)
		if ctx.Err() != nil {
			return
		}
		resourceVersion = next

		switch {
		case err == nil:
		case errors.Is(err, errWatchExpired):
			resourceVersion = ""
			s.logger.DebugContext(ctx, "the "+kind+" watch needs the full list again",
				"cluster", cluster, "error", err.Error())
		default:
			s.logFailure(ctx, slog.LevelWarn, "the "+kind+" watch failed", err, "cluster", cluster)
		}

		long := time.Since(start) >= watchBackoffReset
		if long {
			backoff = minWatchBackoff
		}
		if err == nil && long {
			continue
		}
		if !waitFor(ctx, backoff) {
			return
		}
		backoff = min(2*backoff, maxWatchBackoff)
	}
}

// streamNamespaces reads one namespace watch stream until it ends. It puts the
// namespace of an ADDED event and of a MODIFIED event into patches.
func (s *Syncer) streamNamespaces(ctx context.Context, cluster, resourceVersion string, patches *queue[patchItem]) (string, error) {
	return s.readWatch(ctx, kindNamespace, cluster, namespacesPath(cluster), projectLabel, resourceVersion,
		func(event watchEvent) (string, error) {
			item, err := event.namespace()
			if err != nil {
				return "", err
			}
			switch event.Type {
			case watchAdded, watchModified:
				patches.put(patchItem{target: s.pruneNamespace(item), origin: originWatch})
			case watchDeleted:
				// A namespace that loses its project label leaves the
				// selection of the watch, and the watch sends DELETED for
				// it, with the object before the change.
				target := s.pruneNamespace(item)
				deleted := target.Metadata.DeletionTimestamp != ""
				patches.put(patchItem{target: target, origin: originWatch, deleted: deleted, refresh: !deleted})
			}
			return item.Metadata.ResourceVersion, nil
		})
}

// readWatch reads one watch stream at path until it ends. It reads the token
// file per stream, because a rotation replaces the token. A non-empty selector
// is the label selector of the watch. handle takes every event but ERROR, and
// returns the resource version of its object, or an error when the event has
// no object of the kind. readWatch returns the last resource version that it
// saw, so that the next stream starts after it. A stream that the server ends
// at its timeout returns no error.
func (s *Syncer) readWatch(ctx context.Context, kind, cluster, path, selector, resourceVersion string, handle func(watchEvent) (string, error)) (string, error) {
	token, err := rancherclient.ReadToken(s.tokenFile)
	if err != nil {
		return resourceVersion, err
	}

	query := url.Values{
		"watch":               []string{"true"},
		"allowWatchBookmarks": []string{"true"},
		"timeoutSeconds":      []string{strconv.Itoa(int(watchTimeout.Seconds()))},
	}
	if selector != "" {
		query.Set("labelSelector", selector)
	}
	if resourceVersion != "" {
		query.Set("resourceVersion", resourceVersion)
	}

	resp, err := s.openStream(ctx, s.target(path, query), token)
	if err != nil {
		return resourceVersion, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxBody))
		status := newStatusError(http.MethodGet, path, resp.StatusCode, body)
		if resp.StatusCode == http.StatusGone {
			return "", fmt.Errorf("%w: %w", errWatchExpired, status)
		}
		return resourceVersion, status
	}

	s.metrics.watchOpened(ctx, cluster, kind)
	defer s.metrics.watchClosed(ctx, cluster, kind)
	s.logger.InfoContext(ctx, "the "+kind+" watch is open", "cluster", cluster)

	decoder := json.NewDecoder(resp.Body)
	for {
		var event watchEvent
		if err := decoder.Decode(&event); err != nil {
			if errors.Is(err, io.EOF) {
				return resourceVersion, nil
			}
			return resourceVersion, fmt.Errorf("read the %s watch of cluster %s: %w", kind, cluster, err)
		}
		s.metrics.watchEvent(ctx, cluster, kind, event.Type)

		if event.Type == watchError {
			return "", fmt.Errorf("%w: %s", errWatchExpired, event.status())
		}
		next, err := handle(event)
		if err != nil {
			s.logger.WarnContext(ctx, "the "+kind+" watch event has no "+kind,
				"cluster", cluster, "type", event.Type, "error", err.Error())
			continue
		}
		if next != "" {
			resourceVersion = next
		}
	}
}

// patchWorker patches the namespaces of one cluster queue, one at a time. Its
// name cache lasts as long as the worker, so a project whose display name has
// no valid label value gives one warning per worker. The limiter of the
// cluster bounds the patches per second.
func (s *Syncer) patchWorker(ctx context.Context, cluster string, watch *clusterWatch) {
	names := make(map[string]string)
	seen := make(map[string]string)
	for {
		item, ok := watch.patches.next(ctx)
		if !ok {
			return
		}
		select {
		case <-watch.reset:
			clear(names)
		default:
		}
		if watch.limiter.wait(ctx) {
			if item.refresh {
				item = s.refreshItem(ctx, cluster, item)
			}
			if !item.deleted {
				s.applyPending(ctx, cluster, item, names)
			}
			if s.serviceAccounts {
				s.applyAccounts(ctx, cluster, watch, item, seen)
			}
		}
		watch.patches.done()
		if ctx.Err() != nil {
			return
		}
	}
}

// applyPending patches one namespace of the queue. The projects come from the
// snapshot of the last reconcile run, so the worker needs no project list of
// its own. It reads the token file per namespace, because a rotation replaces
// the token while the stream stays open. names is the name cache of the
// worker. A patch that fails is a warning, and the next reconcile run repeats
// the work.
func (s *Syncer) applyPending(ctx context.Context, cluster string, item patchItem, names map[string]string) {
	target := item.target
	name := target.Metadata.Name
	projectName := projectOf(target, cluster)
	source, ok := sourceOf(s.projectsOf(cluster), projectName)
	if !ok {
		s.logger.DebugContext(ctx, "namespace skipped",
			"cluster", cluster, "namespace", name, "project", projectName)
		return
	}

	token, err := rancherclient.ReadToken(s.tokenFile)
	if err != nil {
		s.logFailure(ctx, slog.LevelWarn, "the namespace patch failed", err,
			"cluster", cluster, "namespace", name, "project", projectName)
		return
	}

	if _, err := s.applyNamespace(ctx, token, cluster, source, target, names, item.origin); err != nil {
		s.logFailure(ctx, slog.LevelWarn, "the namespace patch failed", err,
			"cluster", cluster, "namespace", name, "project", projectName)
	}
}

// patchItem is a namespace that waits for a patch. origin names the path that
// found it. deleted marks a namespace that the API server deleted. refresh
// marks a target from before its last change, which the worker reads again.
type patchItem struct {
	target  namespace
	origin  string
	deleted bool
	refresh bool
}

// refreshItem reads the namespace of item again. A namespace that is gone
// returns a deleted item. A failed read keeps the target of the event, and
// the next reconcile run repeats the work.
func (s *Syncer) refreshItem(ctx context.Context, cluster string, item patchItem) patchItem {
	item.refresh = false
	token, err := rancherclient.ReadToken(s.tokenFile)
	if err != nil {
		s.logFailure(ctx, slog.LevelWarn, "the namespace request failed", err,
			"cluster", cluster, "namespace", item.target.Metadata.Name)
		return item
	}
	var fresh namespace
	found, err := s.getObject(ctx, token, namespacePath(cluster, item.target.Metadata.Name), &fresh)
	if err != nil {
		s.logFailure(ctx, slog.LevelWarn, "the namespace request failed", err,
			"cluster", cluster, "namespace", item.target.Metadata.Name)
		return item
	}
	if !found {
		item.deleted = true
		return item
	}
	item.target = s.pruneNamespace(fresh)
	item.deleted = item.target.Metadata.DeletionTimestamp != ""
	return item
}

// queue has the items of one cluster that wait for a worker, by the key that
// key returns. A second item of a key replaces the first one, so a storm of
// events on one key gives one run of the worker.
type queue[T any] struct {
	key func(T) string
	// signal has one slot, and a put fills it. The worker waits on it while
	// the queue is empty.
	signal chan struct{}

	mu      sync.Mutex
	pending map[string]T
	order   []string
	active  bool
}

func newQueue[T any](key func(T) string) *queue[T] {
	return &queue[T]{key: key, signal: make(chan struct{}, 1), pending: make(map[string]T)}
}

// put stores item under its key. A key that waits already keeps its place in
// the order, and gets the new item.
func (q *queue[T]) put(item T) {
	key := q.key(item)

	q.mu.Lock()
	if _, ok := q.pending[key]; !ok {
		q.order = append(q.order, key)
	}
	q.pending[key] = item
	q.mu.Unlock()

	select {
	case q.signal <- struct{}{}:
	default:
	}
}

// next returns the item that waits longest, and true. It blocks while the
// queue is empty, and it returns false when ctx ends. The queue counts that
// item as active until done.
func (q *queue[T]) next(ctx context.Context) (T, bool) {
	for {
		q.mu.Lock()
		if len(q.order) > 0 {
			key := q.order[0]
			q.order = q.order[1:]
			item := q.pending[key]
			delete(q.pending, key)
			q.active = true
			q.mu.Unlock()
			return item, true
		}
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			var zero T
			return zero, false
		case <-q.signal:
		}
	}
}

// done ends the active state of the item that next returned.
func (q *queue[T]) done() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.active = false
}

// idle reports whether the queue has no item that waits, and no item that a
// worker handles.
func (q *queue[T]) idle() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.order) == 0 && !q.active
}

// limiter is a token bucket that bounds the requests per second of the
// workers of one cluster.
// It is hand-written, because the module has no external rate package.
type limiter struct {
	rate  float64 // tokens added per second
	burst float64 // maximum tokens held

	mu     sync.Mutex
	tokens float64
	last   time.Time
}

// newLimiter returns a limiter with a full bucket of burst tokens.
func newLimiter(rate, burst float64) *limiter {
	return &limiter{rate: rate, burst: burst, tokens: burst, last: time.Now()}
}

// wait takes one token. It blocks until a token is free. It returns false when
// ctx ends first.
func (l *limiter) wait(ctx context.Context) bool {
	for {
		l.mu.Lock()
		l.refill()
		if l.tokens >= 1 {
			l.tokens--
			l.mu.Unlock()
			return true
		}
		need := time.Duration((1 - l.tokens) / l.rate * float64(time.Second))
		l.mu.Unlock()

		if !waitFor(ctx, need) {
			return false
		}
	}
}

// refill adds the tokens that the time since the last refill earns, up to
// burst. The caller holds mu.
func (l *limiter) refill() {
	now := time.Now()
	if elapsed := now.Sub(l.last); elapsed > 0 {
		l.tokens = min(l.burst, l.tokens+elapsed.Seconds()*l.rate)
		l.last = now
	}
}

// waitFor sleeps for d. It returns false when ctx ends first.
func waitFor(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
