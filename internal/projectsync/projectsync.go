// Package projectsync copies labels and annotations of a Rancher project to the
// namespaces of that project.
package projectsync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/evil8io/drover/internal/rancherclient"
)

const (
	defaultInterval  = 60 * time.Second
	maxTimeout       = 30 * time.Second
	defaultPatchRate = 10

	// maxParallelClusters is the count of clusters that one reconcile run
	// syncs at the same time.
	maxParallelClusters = 8
)

// Config configures the syncer that New returns.
type Config struct {
	// RancherURL is the Rancher URL. The scheme is http or https, and the path is empty.
	RancherURL *url.URL
	// CAFile is a PEM bundle that verifies an https URL. An empty value selects the system pool.
	CAFile string
	// InsecureSkipVerify skips the certificate verification of an https Rancher URL.
	InsecureSkipVerify bool
	// TokenFile contains the API token of the Rancher service user.
	TokenFile string
	// Labels are the label keys of a project that the service copies.
	Labels []string
	// Annotations are the annotation keys of a project that the service copies.
	Annotations []string
	// NameLabel is the label key that gets the display name of the project.
	// Empty turns the label off.
	NameLabel string
	// NameAnnotation is the annotation key that gets the display name of the
	// project. Empty turns the annotation off.
	NameAnnotation string
	// Interval is the time between two runs. Zero selects 60 s.
	Interval time.Duration
	// PatchRate bounds the namespace patches and the project namespace lists
	// per second that the watches of one cluster send, together. Zero selects
	// 10. The burst is the rate, and at least 1.
	PatchRate float64
	// Logger gets one line per namespace, and one summary line per run. Nil selects slog.Default.
	Logger *slog.Logger
	// Version is the version in the User-Agent header. An empty value selects dev.
	Version string
	// MeterProvider creates the meter of the sync metrics, and the meter that
	// instruments the Rancher requests. Nil selects otel.GetMeterProvider().
	MeterProvider metric.MeterProvider
	// TracerProvider creates the tracer of the reconcile spans. Nil selects
	// otel.GetTracerProvider().
	TracerProvider trace.TracerProvider
}

// Syncer reconciles the namespaces of every project that the service user sees.
type Syncer struct {
	rancher        *url.URL
	tokenFile      string
	labels         []string
	annotations    []string
	nameLabel      string
	nameAnnotation string
	interval       time.Duration
	timeout        time.Duration
	patchRate      float64
	userAgent      string
	logger         *slog.Logger
	client         *http.Client
	metrics        *metrics
	tracer         trace.Tracer

	// readLabels and readAnnotations are the namespace keys that the sync
	// reads. A decoded namespace keeps only these keys.
	readLabels      []string
	readAnnotations []string
	// pageCap is the byte limit of one list page.
	pageCap int64

	// tokenMissing keeps the last state of the token file, so that the service
	// logs one warning per state change.
	tokenMissing bool

	// watches has the namespace watch of every cluster, and the project watch.
	// Run creates it, and a direct call of reconcile leaves it nil.
	watches *watchSet

	// mu guards clusters, which the reconcile run and the project watch write,
	// and a watcher reads.
	mu       sync.RWMutex
	clusters map[string]map[string]project
}

// New returns a syncer for the configuration. It needs at least one label key,
// annotation key, name label key, or name annotation key.
func New(cfg Config) (*Syncer, error) {
	if cfg.RancherURL == nil {
		return nil, errors.New("the Rancher URL is required")
	}
	if cfg.RancherURL.Scheme != "http" && cfg.RancherURL.Scheme != "https" {
		return nil, fmt.Errorf("the Rancher URL scheme %q is not http or https", cfg.RancherURL.Scheme)
	}
	if cfg.RancherURL.Host == "" {
		return nil, errors.New("the Rancher URL has no host")
	}
	if p := cfg.RancherURL.Path; p != "" && p != "/" {
		return nil, fmt.Errorf("the Rancher URL path %q is not empty", p)
	}
	if cfg.TokenFile == "" {
		return nil, errors.New("the token file is required")
	}
	if len(cfg.Labels)+len(cfg.Annotations) == 0 && cfg.NameLabel == "" && cfg.NameAnnotation == "" {
		return nil, errors.New("at least one label key, annotation key, name label key, or name annotation key is required")
	}
	keys := slices.Concat(cfg.Labels, cfg.Annotations)
	for _, key := range []string{cfg.NameLabel, cfg.NameAnnotation} {
		if key != "" {
			keys = append(keys, key)
		}
	}
	for _, key := range keys {
		if err := checkKey(key); err != nil {
			return nil, err
		}
	}

	transport, err := rancherclient.Transport(cfg.CAFile, cfg.InsecureSkipVerify)
	if err != nil {
		return nil, err
	}

	rancher := *cfg.RancherURL
	rancher.Path = ""
	rancher.RawPath = ""

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultInterval
	}
	patchRate := cfg.PatchRate
	if patchRate <= 0 {
		patchRate = defaultPatchRate
	}
	version := cfg.Version
	if version == "" {
		version = "dev"
	}

	meterProvider := cfg.MeterProvider
	if meterProvider == nil {
		meterProvider = otel.GetMeterProvider()
	}
	m, err := newMetrics(meterProvider)
	if err != nil {
		return nil, fmt.Errorf("build the sync metrics: %w", err)
	}
	tracerProvider := cfg.TracerProvider
	if tracerProvider == nil {
		tracerProvider = otel.GetTracerProvider()
	}

	readLabels, readAnnotations := namespaceKeys(cfg)

	return &Syncer{
		rancher:         &rancher,
		tokenFile:       cfg.TokenFile,
		labels:          slices.Clone(cfg.Labels),
		annotations:     slices.Clone(cfg.Annotations),
		nameLabel:       cfg.NameLabel,
		nameAnnotation:  cfg.NameAnnotation,
		readLabels:      readLabels,
		readAnnotations: readAnnotations,
		pageCap:         maxBody,
		interval:        interval,
		timeout:         min(interval, maxTimeout),
		patchRate:       patchRate,
		userAgent:       "drover/" + version,
		logger:          logger,
		client:          &http.Client{Transport: rancherclient.WrapTransport(transport, meterProvider)},
		metrics:         m,
		tracer:          tracerProvider.Tracer(tracerName),
	}, nil
}

// Handler answers GET /healthz with 200 and the body ok.
func (s *Syncer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

// Run reconciles once, then at every interval. It also keeps one namespace
// watch per cluster open, so that a new namespace gets its keys at once, and
// one project watch, so that a project change reaches its namespaces at once.
// It returns when ctx is done.
func (s *Syncer) Run(ctx context.Context) {
	s.watches = s.newWatchSet()
	defer s.watches.stop()

	s.reconcile(ctx)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reconcile(ctx)
		}
	}
}

// counters are the numbers of one run, for the summary line.
type counters struct {
	clusters   int
	projects   int
	namespaces int
	patched    int
	errors     int
}

// add merges the numbers of one cluster into the run.
func (c *counters) add(other counters) {
	c.projects += other.projects
	c.namespaces += other.namespaces
	c.patched += other.patched
	c.errors += other.errors
}

func (s *Syncer) reconcile(ctx context.Context) {
	token, err := rancherclient.ReadToken(s.tokenFile)
	if err != nil {
		if !s.tokenMissing {
			s.tokenMissing = true
			s.logger.WarnContext(ctx, "the token file is not available yet", "path", s.tokenFile, "error", err.Error())
		}
		return
	}
	if s.tokenMissing {
		s.tokenMissing = false
		s.logger.InfoContext(ctx, "the token file has a token", "path", s.tokenFile)
	}

	base := ctx
	ctx, span := s.tracer.Start(ctx, "reconcile")
	defer span.End()
	start := time.Now()

	var run counters
	projects, err := s.projects(ctx, token)
	if err != nil {
		run.errors++
		s.logFailure(ctx, slog.LevelError, "the project list request failed", err)
		s.finishReconcile(ctx, span, run, start)
		return
	}

	clusters := s.byCluster(ctx, projects)
	s.setClusters(clusters)
	run.clusters = len(clusters)

	names := slices.Sorted(maps.Keys(clusters))
	// A watcher outlives the run, so it gets the base context, not the span.
	s.watches.update(base, names)
	s.watches.startProjects(base)
	s.watches.resetNames()

	var (
		group sync.WaitGroup
		mu    sync.Mutex
		slots = make(chan struct{}, maxParallelClusters)
	)
	for _, cluster := range names {
		slots <- struct{}{}
		group.Add(1)
		go func() {
			defer group.Done()
			defer func() { <-slots }()

			one := counters{projects: len(clusters[cluster])}
			s.syncCluster(ctx, token, cluster, clusters[cluster], &one)

			mu.Lock()
			defer mu.Unlock()
			run.add(one)
		}()
	}
	group.Wait()

	s.finishReconcile(ctx, span, run, start)
}

// setClusters stores the project map of the run. A watcher reads it, so the
// run replaces the map and never writes into the old one.
func (s *Syncer) setClusters(clusters map[string]map[string]project) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clusters = clusters
}

// projectsOf returns the projects of a cluster, by project name, from the last
// reconcile run and the project watch.
func (s *Syncer) projectsOf(cluster string) map[string]project {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.clusters[cluster]
}

// setProject stores item under its project name in the snapshot of cluster.
// It returns false when the snapshot has no such cluster. A worker and the
// reconcile run read the maps without the lock, so setProject replaces them
// and never writes into the old ones.
func (s *Syncer) setProject(cluster, name string, item project) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	projects, ok := s.clusters[cluster]
	if !ok {
		return false
	}
	next := maps.Clone(projects)
	next[name] = item
	s.clusters = withCluster(s.clusters, cluster, next)
	return true
}

// deleteProject removes a project name from the snapshot of cluster, as
// setProject does. It returns false when the snapshot has no such cluster.
func (s *Syncer) deleteProject(cluster, name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	projects, ok := s.clusters[cluster]
	if !ok {
		return false
	}
	if _, ok := projects[name]; !ok {
		return true
	}
	next := maps.Clone(projects)
	delete(next, name)
	s.clusters = withCluster(s.clusters, cluster, next)
	return true
}

// withCluster returns a copy of clusters with projects at cluster.
func withCluster(clusters map[string]map[string]project, cluster string, projects map[string]project) map[string]map[string]project {
	next := maps.Clone(clusters)
	next[cluster] = projects
	return next
}

// finishReconcile logs the summary line of run, sets the span status when the
// run has an error, and records the reconcile metrics.
func (s *Syncer) finishReconcile(ctx context.Context, span trace.Span, run counters, start time.Time) {
	s.logSummary(ctx, run)

	outcome := outcomeOK
	if run.errors > 0 {
		outcome = outcomeError
		err := fmt.Errorf("the run failed with %d errors", run.errors)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	s.metrics.reconcileDone(ctx, outcome, time.Since(start))
}

// byCluster groups the projects per cluster, by the project name in the
// namespace label. That name is the part of the project id after the colon.
func (s *Syncer) byCluster(ctx context.Context, projects []project) map[string]map[string]project {
	clusters := make(map[string]map[string]project)
	for _, item := range projects {
		_, name, ok := strings.Cut(item.ID, ":")
		if !ok || name == "" || item.ClusterID == "" {
			s.logger.WarnContext(ctx, "the project has no cluster id or no name", "id", item.ID)
			continue
		}
		if clusters[item.ClusterID] == nil {
			clusters[item.ClusterID] = make(map[string]project)
		}
		clusters[item.ClusterID][name] = item
	}
	return clusters
}

func (s *Syncer) syncCluster(ctx context.Context, token, cluster string, projects map[string]project, run *counters) {
	items, err := s.namespaces(ctx, token, cluster, projectLabel)
	if err != nil {
		run.errors++
		s.logListFailure(ctx, err, cluster)
		return
	}
	run.namespaces += len(items)

	nameLabels := make(map[string]string)
	for _, item := range items {
		name := item.Metadata.Name
		projectName := item.Metadata.Labels[projectLabel]
		source, ok := projects[projectName]
		if !ok {
			s.logger.DebugContext(ctx, "namespace skipped",
				"cluster", cluster, "namespace", name, "project", projectName)
			continue
		}

		outcome, err := s.applyNamespace(ctx, token, cluster, source, item, nameLabels, originReconcile)
		if err != nil {
			run.errors++
			s.logFailure(ctx, slog.LevelWarn, "the namespace patch failed", err,
				"cluster", cluster, "namespace", name, "project", projectName)
			continue
		}
		if outcome == resultPatched {
			run.patched++
		}
	}
}

// result is the outcome of one namespace.
type result int

const (
	resultUnchanged result = iota
	resultPatched
	resultSkipped
)

// applyNamespace patches one namespace when it needs a key of its project.
// origin names the path that found the namespace, for the log line and the
// metric. nameLabels caches the sanitised display name per project id.
func (s *Syncer) applyNamespace(ctx context.Context, token, cluster string, source project, target namespace, nameLabels map[string]string, origin string) (result, error) {
	name := target.Metadata.Name
	projectName := target.Metadata.Labels[projectLabel]

	value := s.nameLabelValue(ctx, cluster, source, nameLabels)
	change := desired(source, target, s.labels, s.annotations, s.nameLabel, s.nameAnnotation, value)
	if change.empty() {
		s.logger.DebugContext(ctx, "namespace unchanged",
			"cluster", cluster, "namespace", name, "project", projectName)
		return resultUnchanged, nil
	}

	ctx, span := s.tracer.Start(ctx, "patch_namespace", trace.WithAttributes(
		attribute.String("drover.cluster", cluster),
		attribute.String("drover.origin", origin),
		attribute.String("k8s.namespace.name", name),
	))
	defer span.End()

	if err := s.patchNamespace(ctx, token, cluster, name, change); err != nil {
		if skippable(err) {
			s.logger.DebugContext(ctx, "namespace patch skipped",
				"cluster", cluster, "namespace", name, "project", projectName, "error", err.Error())
			return resultSkipped, nil
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return resultSkipped, err
	}

	s.metrics.namespacePatched(ctx, origin)
	s.logger.InfoContext(ctx, "namespace patched",
		"cluster", cluster, "namespace", name, "project", projectName, "origin", origin,
		"labels", keysOf(change.labels), "annotations", keysOf(change.annotations),
		"removed_labels", change.removeLabels, "removed_annotations", change.removeAnnotations)
	return resultPatched, nil
}

// nameLabelValue returns the sanitised label value of the project's display
// name, cached in cache by project id. It logs one warning per project id when
// the display name has no valid label value. An off name label returns "".
func (s *Syncer) nameLabelValue(ctx context.Context, cluster string, source project, cache map[string]string) string {
	if s.nameLabel == "" {
		return ""
	}
	if value, ok := cache[source.ID]; ok {
		return value
	}
	value := sanitizeLabelValue(source.Name)
	cache[source.ID] = value
	if value == "" {
		s.logger.WarnContext(ctx, "the project display name has no valid label value",
			"cluster", cluster, "project", source.ID)
	}
	return value
}

// patch is the metadata that one namespace needs from its project. A key in a
// remove list gets the value null in the merge patch, which deletes it.
type patch struct {
	labels            map[string]string
	annotations       map[string]string
	removeLabels      []string
	removeAnnotations []string
}

func (p patch) empty() bool {
	return len(p.labels)+len(p.annotations)+len(p.removeLabels)+len(p.removeAnnotations) == 0
}

func (p patch) body() ([]byte, error) {
	var document struct {
		Metadata struct {
			Labels      map[string]*string `json:"labels,omitempty"`
			Annotations map[string]*string `json:"annotations,omitempty"`
		} `json:"metadata"`
	}
	document.Metadata.Labels = mergeFields(p.labels, p.removeLabels)
	document.Metadata.Annotations = mergeFields(p.annotations, p.removeAnnotations)
	return json.Marshal(document)
}

// mergeFields returns the merge-patch document of set and remove. A key of
// remove gets a null value, and a JSON merge patch reads a null as a delete.
func mergeFields(set map[string]string, remove []string) map[string]*string {
	if len(set)+len(remove) == 0 {
		return nil
	}
	out := make(map[string]*string, len(set)+len(remove))
	for key, value := range set {
		out[key] = &value
	}
	for _, key := range remove {
		out[key] = nil
	}
	return out
}

// desired returns the change that the namespace needs. nameLabel and
// nameAnnotation are the configured name keys, empty when off. nameLabelValue
// is the sanitised label value; an empty value skips the label.
func desired(source project, target namespace, labels, annotations []string, nameLabel, nameAnnotation, nameLabelValue string) patch {
	wantLabels := wanted(source.Labels, labels, nameLabel, nameLabelValue)
	wantAnnotations := wanted(source.Annotations, annotations, nameAnnotation, source.Name)

	change := patch{
		labels:            changes(wantLabels, target.Metadata.Labels),
		annotations:       changes(wantAnnotations, target.Metadata.Annotations),
		removeLabels:      removals(wantLabels, target.Metadata.Labels, managedKeys(target, managedLabelsKey, labels, nameLabel)),
		removeAnnotations: removals(wantAnnotations, target.Metadata.Annotations, managedKeys(target, managedAnnotationsKey, annotations, nameAnnotation)),
	}
	change.trackOwned(target, managedLabelsKey, wantLabels)
	change.trackOwned(target, managedAnnotationsKey, wantAnnotations)
	return change
}

// wanted returns the metadata that the namespace needs from the project, that
// is every key of source that keys names, plus the name key with nameValue.
// The name path owns the name key, so keys never selects it from source.
func wanted(source map[string]string, keys []string, nameKey, nameValue string) map[string]string {
	want := make(map[string]string, len(keys)+1)
	for _, key := range keys {
		if key == nameKey {
			continue
		}
		if value, ok := source[key]; ok {
			want[key] = value
		}
	}
	if nameKey != "" && nameValue != "" {
		want[nameKey] = nameValue
	}
	return want
}

// changes returns every key of want that target does not have with the same
// value.
func changes(want, target map[string]string) map[string]string {
	var out map[string]string
	for key, value := range want {
		if have, ok := target[key]; ok && have == value {
			continue
		}
		if out == nil {
			out = make(map[string]string, len(want))
		}
		out[key] = value
	}
	return out
}

// removals returns every key that owned names, that want does not have, and
// that target still has. The service removes only a key that it wrote itself,
// so a key of the tenant stays.
func removals(want, target map[string]string, owned []string) []string {
	var out []string
	for _, key := range owned {
		if _, ok := want[key]; ok {
			continue
		}
		if _, ok := target[key]; !ok {
			continue
		}
		out = append(out, key)
	}
	slices.Sort(out)
	return out
}

// trackOwned writes the keys of want into the managed annotation at key, when
// that annotation does not have them already. An empty want removes the
// annotation.
func (p *patch) trackOwned(target namespace, key string, want map[string]string) {
	value := strings.Join(slices.Sorted(maps.Keys(want)), ",")
	if value == target.Metadata.Annotations[key] {
		return
	}
	if value == "" {
		p.removeAnnotations = append(p.removeAnnotations, key)
		slices.Sort(p.removeAnnotations)
		return
	}
	if p.annotations == nil {
		p.annotations = make(map[string]string, 2)
	}
	p.annotations[key] = value
}

// managedKeys returns the keys that the service owns on the namespace, from
// the managed annotation at key. A tenant can edit that annotation, so only a
// key that the service can write counts: a key of configured, or nameKey.
func managedKeys(target namespace, key string, configured []string, nameKey string) []string {
	value := target.Metadata.Annotations[key]
	if value == "" {
		return nil
	}
	var keys []string
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if part == nameKey || slices.Contains(configured, part) {
			keys = append(keys, part)
		}
	}
	return keys
}

func keysOf(values map[string]string) []string {
	return slices.Sorted(maps.Keys(values))
}

func (s *Syncer) logSummary(ctx context.Context, run counters) {
	s.logger.InfoContext(ctx, "reconcile",
		"clusters", run.clusters, "projects", run.projects, "namespaces", run.namespaces,
		"patched", run.patched, "errors", run.errors)
}

// logListFailure writes the line of a failed namespace list of cluster. A 403
// means that the service user has no binding on the cluster.
func (s *Syncer) logListFailure(ctx context.Context, err error, cluster string, attrs ...any) {
	message := "the namespace list request failed"
	var status statusError
	if errors.As(err, &status) && status.status == http.StatusForbidden {
		message = "the service user has no binding on the cluster"
	}
	s.logFailure(ctx, slog.LevelWarn, message, err, append([]any{"cluster", cluster}, attrs...)...)
}

// logFailure writes one line for a failure. A canceled context is a shutdown,
// so that line is at the debug level.
func (s *Syncer) logFailure(ctx context.Context, level slog.Level, message string, err error, attrs ...any) {
	if errors.Is(err, context.Canceled) {
		level = slog.LevelDebug
	}
	s.logger.Log(ctx, level, message, append(attrs, "error", err.Error())...)
	s.metrics.syncError(ctx)
}
