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
	watchers map[string]context.CancelFunc
}

func (s *Syncer) newWatchSet() *watchSet {
	return &watchSet{syncer: s, watchers: make(map[string]context.CancelFunc)}
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
	for cluster, cancel := range w.watchers {
		if _, ok := want[cluster]; ok {
			continue
		}
		cancel()
		delete(w.watchers, cluster)
	}
	for _, cluster := range clusters {
		if _, ok := w.watchers[cluster]; ok {
			continue
		}
		watchCtx, cancel := context.WithCancel(ctx)
		w.watchers[cluster] = cancel
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			w.syncer.watchCluster(watchCtx, cluster)
		}()
	}
}

// stop ends every watcher, and it waits for the goroutines.
func (w *watchSet) stop() {
	if w == nil {
		return
	}
	w.mu.Lock()
	for cluster, cancel := range w.watchers {
		cancel()
		delete(w.watchers, cluster)
	}
	w.mu.Unlock()
	w.wg.Wait()
}

// watchCluster keeps one namespace watch of a cluster open. A stream that ends
// starts again, after a backoff that grows with each failure. The reconcile
// run covers the namespaces that the gap misses.
func (s *Syncer) watchCluster(ctx context.Context, cluster string) {
	var (
		resourceVersion string
		backoff         = minWatchBackoff
	)
	for ctx.Err() == nil {
		start := time.Now()
		next, err := s.streamNamespaces(ctx, cluster, resourceVersion)
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

// streamNamespaces reads one namespace watch stream until it ends. It returns
// the last resource version that it saw, so that the next stream starts after
// it. A stream that the server ends at its timeout returns no error.
func (s *Syncer) streamNamespaces(ctx context.Context, cluster, resourceVersion string) (string, error) {
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
			s.handleEvent(ctx, cluster, item)
		}
	}
}

// handleEvent patches the namespace of one watch event. The projects come from
// the snapshot of the last reconcile run, so the handler needs no project list
// of its own. It reads the token file per event, because a rotation replaces
// the token while the stream stays open.
func (s *Syncer) handleEvent(ctx context.Context, cluster string, target namespace) {
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

	if _, err := s.applyNamespace(ctx, token, cluster, source, target, make(map[string]string, 1), originWatch); err != nil {
		s.logFailure(ctx, slog.LevelWarn, "the namespace patch failed", err,
			"cluster", cluster, "namespace", name, "project", projectName)
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
