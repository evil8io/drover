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
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/evil8io/drover/internal/rancherclient"
)

const (
	defaultInterval = 60 * time.Second
	maxTimeout      = 30 * time.Second
)

// Config configures the syncer that New returns.
type Config struct {
	// RancherURL is the Rancher URL. The scheme is http or https, and the path is empty.
	RancherURL *url.URL
	// CAFile is a PEM bundle that verifies an https URL. An empty value selects the system pool.
	CAFile string
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
	userAgent      string
	logger         *slog.Logger
	client         *http.Client
	metrics        *metrics
	tracer         trace.Tracer

	// tokenMissing keeps the last state of the token file, so that the service
	// logs one warning per state change.
	tokenMissing bool
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

	transport, err := rancherclient.Transport(cfg.CAFile)
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

	return &Syncer{
		rancher:        &rancher,
		tokenFile:      cfg.TokenFile,
		labels:         slices.Clone(cfg.Labels),
		annotations:    slices.Clone(cfg.Annotations),
		nameLabel:      cfg.NameLabel,
		nameAnnotation: cfg.NameAnnotation,
		interval:       interval,
		timeout:        min(interval, maxTimeout),
		userAgent:      "drover/" + version,
		logger:         logger,
		client:         &http.Client{Transport: rancherclient.WrapTransport(transport, meterProvider)},
		metrics:        m,
		tracer:         tracerProvider.Tracer(tracerName),
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

// Run reconciles once, then at every interval. It returns when ctx is done.
func (s *Syncer) Run(ctx context.Context) {
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
	run.clusters = len(clusters)
	for _, cluster := range slices.Sorted(maps.Keys(clusters)) {
		run.projects += len(clusters[cluster])
		s.syncCluster(ctx, token, cluster, clusters[cluster], &run)
	}
	s.finishReconcile(ctx, span, run, start)
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
	items, err := s.namespaces(ctx, token, cluster)
	if err != nil {
		run.errors++
		var status statusError
		if errors.As(err, &status) && status.status == http.StatusForbidden {
			s.logFailure(ctx, slog.LevelWarn, "the service user has no binding on the cluster", err, "cluster", cluster)
			return
		}
		s.logFailure(ctx, slog.LevelWarn, "the namespace list request failed", err, "cluster", cluster)
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

		nameLabelValue := s.nameLabelValue(ctx, cluster, source, nameLabels)
		change := desired(source, item, s.labels, s.annotations, s.nameLabel, s.nameAnnotation, nameLabelValue)
		if change.empty() {
			s.logger.DebugContext(ctx, "namespace unchanged",
				"cluster", cluster, "namespace", name, "project", projectName)
			continue
		}
		if err := s.patchNamespace(ctx, token, cluster, name, change); err != nil {
			run.errors++
			s.logFailure(ctx, slog.LevelWarn, "the namespace patch failed", err,
				"cluster", cluster, "namespace", name, "project", projectName)
			continue
		}
		run.patched++
		s.logger.InfoContext(ctx, "namespace patched",
			"cluster", cluster, "namespace", name, "project", projectName,
			"labels", keysOf(change.labels), "annotations", keysOf(change.annotations))
		s.metrics.namespacePatched(ctx)
	}
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

// patch is the metadata that one namespace needs from its project.
type patch struct {
	labels      map[string]string
	annotations map[string]string
}

func (p patch) empty() bool {
	return len(p.labels) == 0 && len(p.annotations) == 0
}

func (p patch) body() ([]byte, error) {
	var document struct {
		Metadata struct {
			Labels      map[string]string `json:"labels,omitempty"`
			Annotations map[string]string `json:"annotations,omitempty"`
		} `json:"metadata"`
	}
	document.Metadata.Labels = p.labels
	document.Metadata.Annotations = p.annotations
	return json.Marshal(document)
}

// desired returns the keys that the namespace needs from the project. nameLabel
// and nameAnnotation are the configured name keys, empty when off.
// nameLabelValue is the sanitised label value; an empty value skips the label.
func desired(source project, target namespace, labels, annotations []string, nameLabel, nameAnnotation, nameLabelValue string) patch {
	sourceLabels, labelKeys := source.Labels, labels
	if nameLabel != "" && nameLabelValue != "" {
		sourceLabels = withKey(sourceLabels, nameLabel, nameLabelValue)
		labelKeys = append(slices.Clone(labels), nameLabel)
	}
	sourceAnnotations, annotationKeys := source.Annotations, annotations
	if nameAnnotation != "" {
		sourceAnnotations = withKey(sourceAnnotations, nameAnnotation, source.Name)
		annotationKeys = append(slices.Clone(annotations), nameAnnotation)
	}
	return patch{
		labels:      changes(sourceLabels, target.Metadata.Labels, labelKeys),
		annotations: changes(sourceAnnotations, target.Metadata.Annotations, annotationKeys),
	}
}

// withKey returns a copy of m with key set to value. An empty key or an empty
// value returns m unchanged.
func withKey(m map[string]string, key, value string) map[string]string {
	if key == "" || value == "" {
		return m
	}
	out := make(map[string]string, len(m)+1)
	maps.Copy(out, m)
	out[key] = value
	return out
}

// changes returns every key of source in keys that target does not have with the
// same value. A key that source does not have is not in the result, so the patch
// never removes a key from the namespace.
func changes(source, target map[string]string, keys []string) map[string]string {
	var out map[string]string
	for _, key := range keys {
		want, ok := source[key]
		if !ok {
			continue
		}
		if have, ok := target[key]; ok && have == want {
			continue
		}
		if out == nil {
			out = make(map[string]string, len(keys))
		}
		out[key] = want
	}
	return out
}

func keysOf(values map[string]string) []string {
	return slices.Sorted(maps.Keys(values))
}

func (s *Syncer) logSummary(ctx context.Context, run counters) {
	s.logger.InfoContext(ctx, "reconcile",
		"clusters", run.clusters, "projects", run.projects, "namespaces", run.namespaces,
		"patched", run.patched, "errors", run.errors)
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
