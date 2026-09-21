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

// watchSet has one namespace watch per cluster. Every reconcile run gives it
// the current cluster set, and the set starts and stops the watchers.
type watchSet struct {
	syncer *Syncer
	wg     sync.WaitGroup

	mu       sync.Mutex
	watchers map[string]*clusterWatch
}

// clusterWatch is the state of the namespace watch of one cluster. The stream
// puts a namespace into queue, and the worker of the cluster takes it from
// there. A value on reset tells that worker to clear its name cache.
type clusterWatch struct {
	cancel context.CancelFunc
	queue  *patchQueue
	reset  chan struct{}
}

func newClusterWatch() *clusterWatch {
	return &clusterWatch{queue: newPatchQueue(), reset: make(chan struct{}, 1)}
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
		watch := newClusterWatch()
		watch.cancel = cancel
		w.watchers[cluster] = watch
		w.wg.Add(2)
		go func() {
			defer w.wg.Done()
			w.syncer.watchCluster(watchCtx, cluster, watch.queue)
		}()
		go func() {
			defer w.wg.Done()
			w.syncer.patchWorker(watchCtx, cluster, watch)
		}()
	}
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
		select {
		case watch.reset <- struct{}{}:
		default:
		}
	}
}

// stop ends every watcher and every worker, and it waits for the goroutines.
func (w *watchSet) stop() {
	if w == nil {
		return
	}
	w.mu.Lock()
	for cluster, watch := range w.watchers {
		watch.cancel()
		delete(w.watchers, cluster)
	}
	w.mu.Unlock()
	w.wg.Wait()
}

// watchCluster keeps one namespace watch of a cluster open. A stream that ends
// starts again, after a backoff that grows with each failure. The reconcile
// run covers the namespaces that the gap misses.
func (s *Syncer) watchCluster(ctx context.Context, cluster string, queue *patchQueue) {
	var (
		resourceVersion string
		backoff         = minWatchBackoff
	)
	for ctx.Err() == nil {
		start := time.Now()
		next, err := s.streamNamespaces(ctx, cluster, resourceVersion, queue)
		if ctx.Err() != nil {
			return
		}
		resourceVersion = next

		switch {
		case err == nil:
		case errors.Is(err, errWatchExpired):
			resourceVersion = ""
			s.logger.DebugContext(ctx, "the namespace watch needs the full list again",
				"cluster", cluster, "error", err.Error())
		default:
			s.logFailure(ctx, slog.LevelWarn, "the namespace watch failed", err, "cluster", cluster)
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
// namespace of an ADDED event and of a MODIFIED event into queue. It returns
// the last resource version that it saw, so that the next stream starts after
// it. A stream that the server ends at its timeout returns no error.
func (s *Syncer) streamNamespaces(ctx context.Context, cluster, resourceVersion string, queue *patchQueue) (string, error) {
	token, err := rancherclient.ReadToken(s.tokenFile)
	if err != nil {
		return resourceVersion, err
	}

	path := namespacesPath(cluster)
	query := url.Values{
		"watch":               []string{"true"},
		"labelSelector":       []string{projectLabel},
		"allowWatchBookmarks": []string{"true"},
		"timeoutSeconds":      []string{strconv.Itoa(int(watchTimeout.Seconds()))},
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

	s.metrics.watchOpened(ctx, cluster)
	defer s.metrics.watchClosed(ctx, cluster)
	s.logger.InfoContext(ctx, "the namespace watch is open", "cluster", cluster)

	decoder := json.NewDecoder(resp.Body)
	for {
		var event watchEvent
		if err := decoder.Decode(&event); err != nil {
			if errors.Is(err, io.EOF) {
				return resourceVersion, nil
			}
			return resourceVersion, fmt.Errorf("read the namespace watch of cluster %s: %w", cluster, err)
		}
		s.metrics.watchEvent(ctx, cluster, event.Type)

		if event.Type == watchError {
			return "", fmt.Errorf("%w: %s", errWatchExpired, event.status())
		}
		item, err := event.namespace()
		if err != nil {
			s.logger.WarnContext(ctx, "the namespace watch event has no namespace",
				"cluster", cluster, "type", event.Type, "error", err.Error())
			continue
		}
		if item.Metadata.ResourceVersion != "" {
			resourceVersion = item.Metadata.ResourceVersion
		}
		if event.Type == watchAdded || event.Type == watchModified {
			queue.put(item)
		}
	}
}

// patchWorker patches the namespaces of one cluster queue, one at a time. Its
// name cache lasts as long as the worker, so a project whose display name has
// no valid label value gives one warning per worker. The limiter bounds the
// patches per second of the cluster.
func (s *Syncer) patchWorker(ctx context.Context, cluster string, watch *clusterWatch) {
	names := make(map[string]string)
	patches := newLimiter(s.patchRate, max(s.patchRate, 1))
	for {
		target, ok := watch.queue.next(ctx)
		if !ok {
			return
		}
		select {
		case <-watch.reset:
			clear(names)
		default:
		}
		if patches.wait(ctx) {
			s.applyPending(ctx, cluster, target, names)
		}
		watch.queue.done()
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
func (s *Syncer) applyPending(ctx context.Context, cluster string, target namespace, names map[string]string) {
	name := target.Metadata.Name
	projectName := target.Metadata.Labels[projectLabel]
	source, ok := s.projectsOf(cluster)[projectName]
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

	if _, err := s.applyNamespace(ctx, token, cluster, source, target, names, originWatch); err != nil {
		s.logFailure(ctx, slog.LevelWarn, "the namespace patch failed", err,
			"cluster", cluster, "namespace", name, "project", projectName)
	}
}

// patchQueue has the namespaces of one cluster that wait for a patch, by
// namespace name. A second event of a namespace replaces the first one, so a
// storm of events on one namespace gives one patch.
type patchQueue struct {
	// signal has one slot, and a put fills it. The worker waits on it while
	// the queue is empty.
	signal chan struct{}

	mu      sync.Mutex
	pending map[string]namespace
	order   []string
	active  bool
}

func newPatchQueue() *patchQueue {
	return &patchQueue{signal: make(chan struct{}, 1), pending: make(map[string]namespace)}
}

// put stores target under its namespace name. A name that waits already keeps
// its place in the order, and gets the new object.
func (q *patchQueue) put(target namespace) {
	name := target.Metadata.Name

	q.mu.Lock()
	if _, ok := q.pending[name]; !ok {
		q.order = append(q.order, name)
	}
	q.pending[name] = target
	q.mu.Unlock()

	select {
	case q.signal <- struct{}{}:
	default:
	}
}

// next returns the namespace that waits longest, and true. It blocks while the
// queue is empty, and it returns false when ctx ends. The queue counts that
// namespace as active until done.
func (q *patchQueue) next(ctx context.Context) (namespace, bool) {
	for {
		q.mu.Lock()
		if len(q.order) > 0 {
			name := q.order[0]
			q.order = q.order[1:]
			target := q.pending[name]
			delete(q.pending, name)
			q.active = true
			q.mu.Unlock()
			return target, true
		}
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			return namespace{}, false
		case <-q.signal:
		}
	}
}

// done ends the active state of the namespace that next returned.
func (q *patchQueue) done() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.active = false
}

// idle reports whether the queue has no namespace that waits, and no namespace
// in a patch.
func (q *patchQueue) idle() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.order) == 0 && !q.active
}

// limiter is a token bucket that bounds the patches per second of one cluster.
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
