package projectsync

import (
	"context"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestReconcileRecordsAReconcilesPoint checks that one reconcile run records
// one drover.sync.reconciles point, with the outcome attribute. It reads the
// SDK's own manual reader, not a live exporter.
func TestReconcileRecordsAReconcilesPoint(t *testing.T) {
	t.Parallel()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	rancher := newFakeRancher(t)
	syncer, _ := newSyncer(t, rancher, tokenFile(t, serviceToken+"\n"), func(cfg *Config) {
		cfg.MeterProvider = provider
	})
	syncer.reconcile(context.Background())

	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}

	sum := findSum(t, data, "drover.sync.reconciles")
	if len(sum.DataPoints) != 1 {
		t.Fatalf("data points = %d, want 1", len(sum.DataPoints))
	}
	point := sum.DataPoints[0]
	if point.Value != 1 {
		t.Errorf("value = %d, want 1", point.Value)
	}
	outcome, ok := point.Attributes.Value(attribute.Key("outcome"))
	if !ok || outcome.AsString() != outcomeOK {
		t.Errorf("outcome attribute = %v, ok=%v, want %q", outcome, ok, outcomeOK)
	}
}

// TestReconcileFailureRecordsTheErrorOutcome checks that a reconcile run with
// a namespace list failure records drover.sync.reconciles with outcome error.
func TestReconcileFailureRecordsTheErrorOutcome(t *testing.T) {
	t.Parallel()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	rancher := newFakeRancher(t, forbid("c-1"))
	syncer, _ := newSyncer(t, rancher, tokenFile(t, serviceToken+"\n"), func(cfg *Config) {
		cfg.MeterProvider = provider
	})
	syncer.reconcile(context.Background())

	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}

	sum := findSum(t, data, "drover.sync.reconciles")
	point := sum.DataPoints[0]
	outcome, ok := point.Attributes.Value(attribute.Key("outcome"))
	if !ok || outcome.AsString() != outcomeError {
		t.Errorf("outcome attribute = %v, ok=%v, want %q", outcome, ok, outcomeError)
	}

	errs := findSum(t, data, "drover.sync.errors")
	if len(errs.DataPoints) != 1 || errs.DataPoints[0].Value != 1 {
		t.Errorf("drover.sync.errors points = %v, want one point with value 1", errs.DataPoints)
	}
}

// TestReconcileProducesAReconcileSpan checks that one reconcile run produces
// one span named reconcile. It reads a span recorder, not a live exporter.
func TestReconcileProducesAReconcileSpan(t *testing.T) {
	t.Parallel()
	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	rancher := newFakeRancher(t)
	syncer, _ := newSyncer(t, rancher, tokenFile(t, serviceToken+"\n"), func(cfg *Config) {
		cfg.TracerProvider = tracerProvider
	})
	syncer.reconcile(context.Background())

	var reconcileSpans int
	for _, span := range recorder.Ended() {
		if span.Name() == "reconcile" {
			reconcileSpans++
		}
	}
	if reconcileSpans != 1 {
		t.Errorf("reconcile spans = %d, want 1", reconcileSpans)
	}
}

// findSum returns the metricdata.Sum[int64] of the metric named name, from
// the first scope that has it.
func findSum(t *testing.T, data metricdata.ResourceMetrics, name string) metricdata.Sum[int64] {
	t.Helper()
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %s has type %T, want metricdata.Sum[int64]", name, m.Data)
			}
			return sum
		}
	}
	t.Fatalf("no metric named %s", name)
	return metricdata.Sum[int64]{}
}

// findGauge returns the metricdata.Gauge[int64] of the metric named name,
// from the first scope that has it. It returns false when no scope has the
// metric, because the SDK leaves out a metric without data points.
func findGauge(t *testing.T, data metricdata.ResourceMetrics, name string) (metricdata.Gauge[int64], bool) {
	t.Helper()
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			gauge, ok := m.Data.(metricdata.Gauge[int64])
			if !ok {
				t.Fatalf("metric %s has type %T, want metricdata.Gauge[int64]", name, m.Data)
			}
			return gauge, true
		}
	}
	return metricdata.Gauge[int64]{}, false
}

// TestTrustGaugeOverflowsTheSameProjectsAtEachCollection checks that the
// projects below the cardinality limit are the first projects in the order of
// cluster and project, at each collection.
func TestTrustGaugeOverflowsTheSameProjectsAtEachCollection(t *testing.T) {
	t.Parallel()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithCardinalityLimit(3))
	m, err := newMetrics(provider, false)
	if err != nil {
		t.Fatalf("build the metrics: %v", err)
	}
	statements := []statementStatus{{Name: "deploy", Ready: true}}
	for _, name := range []string{"p-f", "p-c", "p-a", "p-e", "p-b", "p-d"} {
		m.setTrust(acctCluster, name, &trustStatus{Statements: &statements}, nil, nil, func() bool { return true })
	}

	want := []string{"overflow", "p-a", "p-b"}
	for range 5 {
		var data metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &data); err != nil {
			t.Fatalf("collect metrics: %v", err)
		}
		gauge, _ := findGauge(t, data, "drover.sync.trust.statements.ready")
		var got []string
		for _, point := range gauge.DataPoints {
			if _, ok := point.Attributes.Value("otel.metric.overflow"); ok {
				got = append(got, "overflow")
				continue
			}
			project, _ := point.Attributes.Value("project")
			got = append(got, project.AsString())
		}
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Fatalf("points = %v, want %v", got, want)
		}
	}
}

// countProjectPoints returns the count of data points of the gauge metric for
// the project name.
func countProjectPoints(t *testing.T, data metricdata.ResourceMetrics, metric, name string) int {
	t.Helper()
	gauge, _ := findGauge(t, data, metric)
	count := 0
	for _, point := range gauge.DataPoints {
		if project, _ := point.Attributes.Value("project"); project.AsString() == name {
			count++
		}
	}
	return count
}

// TestTrustInfoPointsAreRemovedWithTheReadyPoints checks that each remove of
// the data points of a project removes the points of both trust gauges, and
// keeps the points of another project.
func TestTrustInfoPointsAreRemovedWithTheReadyPoints(t *testing.T) {
	t.Parallel()
	rules := fixtureRules(t)
	statements := parseTrust(twoStatements, rules).statements
	entries := []statementStatus{{Name: "deploy", Ready: true}, {Name: "rotator", Ready: true}}
	status := &trustStatus{Statements: &entries}
	always := func() bool { return true }

	tests := []struct {
		name   string
		remove func(*metrics)
	}{
		{"nil status", func(m *metrics) { m.setTrust(acctCluster, "p-alpha", nil, nil, rules, always) }},
		{"forgetTrust", func(m *metrics) { m.forgetTrust(acctCluster, "p-alpha") }},
		{"pruneTrust", func(m *metrics) { m.pruneTrust(func(ref projectRef) bool { return ref.name == "p-alpha" }) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			reader := sdkmetric.NewManualReader()
			m, err := newMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)), true)
			if err != nil {
				t.Fatalf("build the metrics: %v", err)
			}
			for _, name := range []string{"p-alpha", "p-beta"} {
				m.setTrust(acctCluster, name, status, statements, rules, always)
			}
			tt.remove(m)

			var data metricdata.ResourceMetrics
			if err := reader.Collect(context.Background(), &data); err != nil {
				t.Fatalf("collect metrics: %v", err)
			}
			for _, metric := range []string{"drover.sync.trust.statements.ready", "drover.sync.trust.statements.info"} {
				if got := countProjectPoints(t, data, metric, "p-alpha"); got != 0 {
					t.Errorf("%s points of the removed project = %d, want 0", metric, got)
				}
				if got := countProjectPoints(t, data, metric, "p-beta"); got != 2 {
					t.Errorf("%s points of the other project = %d, want 2", metric, got)
				}
			}
		})
	}
}

func TestClaimsAttribute(t *testing.T) {
	t.Parallel()
	// Each "€" has three bytes, so byte 2048 of long is inside a rune.
	long := `{"ab":"` + strings.Repeat("€", 1000)

	tests := []struct {
		name   string
		claims map[string]any
		first  []string
		want   string
	}{
		{"the first claims, then the other keys sorted", map[string]any{"a": "x", "repository_id": "1", "event_name": []string{"pull_request", "pull_request_target"}}, []string{"repository_id"}, `{"repository_id":"1","a":"x","event_name":["pull_request","pull_request_target"]}`},
		{"list values in their order", map[string]any{"ref": []string{"refs/heads/release", "refs/heads/main"}}, nil, `{"ref":["refs/heads/release","refs/heads/main"]}`},
		{"a value with the separators of the form", map[string]any{"a": `x","b":"y`, "c": "<a&b>\n"}, nil, `{"a":"x\",\"b\":\"y","c":"<a&b>\n"}`},
		{"a first claim that is not bound", map[string]any{"a": "x"}, []string{"repository_id"}, `{"a":"x"}`},
		{"no claims", nil, nil, `{}`},
		{"cut at a rune boundary", map[string]any{"ab": strings.Repeat("€", 1000)}, nil, long[:2047]},
		{"a long claim after the first claim", map[string]any{"a": strings.Repeat("x", 3000), "repository_id": "1"}, []string{"repository_id"}, (`{"repository_id":"1","a":"` + strings.Repeat("x", 3000))[:maxInfoAttribute]},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := claimsAttribute(tt.claims, tt.first)
			if got != tt.want {
				t.Errorf("claimsAttribute = %q (%d bytes), want %q (%d bytes)", got, len(got), tt.want, len(tt.want))
			}
			if len(got) > maxInfoAttribute || !utf8.ValidString(got) {
				t.Errorf("claimsAttribute has %d bytes, valid UTF-8 = %v; want at most %d bytes of valid UTF-8", len(got), utf8.ValidString(got), maxInfoAttribute)
			}
		})
	}
}

// TestWatchRecordsTheEventAndTheOpenStream checks that one namespace watch
// stream records drover.sync.events with the type attribute and the kind
// namespace, and that drover.sync.watches.open returns to zero once the
// stream ends.
func TestWatchRecordsTheEventAndTheOpenStream(t *testing.T) {
	t.Parallel()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	rancher := newFakeRancher(t, watching("c-1", addedEvent, modifiedEvent))
	syncer, _ := newSyncer(t, rancher, tokenFile(t, serviceToken), func(cfg *Config) {
		cfg.MeterProvider = provider
	})
	ctx := context.Background()
	syncer.reconcile(ctx)
	if _, err := watchOnce(t, syncer, "c-1"); err != nil {
		t.Fatalf("stream the namespace watch: %v", err)
	}

	var data metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &data); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}

	events := findSum(t, data, "drover.sync.events")
	types := make(map[string]int64, len(events.DataPoints))
	for _, point := range events.DataPoints {
		value, _ := point.Attributes.Value(attribute.Key("type"))
		types[value.AsString()] = point.Value
		if kind, _ := point.Attributes.Value(attribute.Key("kind")); kind.AsString() != kindNamespace {
			t.Errorf("kind of the %s point = %q, want %q", value.AsString(), kind.AsString(), kindNamespace)
		}
	}
	if types[watchAdded] != 1 || types[watchModified] != 1 {
		t.Errorf("event points = %v, want one ADDED and one MODIFIED", types)
	}

	open := findSum(t, data, "drover.sync.watches.open")
	if len(open.DataPoints) != 1 || open.DataPoints[0].Value != 0 {
		t.Errorf("drover.sync.watches.open points = %v, want one point with value 0", open.DataPoints)
	} else if kind, _ := open.DataPoints[0].Attributes.Value(attribute.Key("kind")); kind.AsString() != kindNamespace {
		t.Errorf("kind of the open point = %q, want %q", kind.AsString(), kindNamespace)
	}

	patched := findSum(t, data, "drover.sync.namespaces.patched")
	var fromTheWatch int64
	for _, point := range patched.DataPoints {
		if origin, ok := point.Attributes.Value(attribute.Key("origin")); ok && origin.AsString() == originWatch {
			fromTheWatch = point.Value
		}
	}
	if fromTheWatch != 1 {
		t.Errorf("namespaces patched from the watch = %d, want 1", fromTheWatch)
	}
}

// TestProjectWatchRecordsTheChangeAndTheKind checks that one project change
// records drover.sync.projects.changed with the cluster of the project, and
// that the stream records drover.sync.events and drover.sync.watches.open
// with the Rancher cluster and the kind project.
func TestProjectWatchRecordsTheChangeAndTheKind(t *testing.T) {
	t.Parallel()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	frame := alphaFrame(t, watchModified, func(object *projectObject) {
		object.Metadata.Labels["cost-center"] = "cc-9"
	})
	p := newProjectTest(t, []func(*fakeRancher){watchingProjects(frame)}, func(cfg *Config) {
		cfg.MeterProvider = provider
	})

	if _, err := p.stream(t, ""); err != nil {
		t.Fatalf("stream the project watch: %v", err)
	}

	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}

	changed := findSum(t, data, "drover.sync.projects.changed")
	if len(changed.DataPoints) != 1 || changed.DataPoints[0].Value != 1 {
		t.Fatalf("drover.sync.projects.changed points = %v, want one point with value 1", changed.DataPoints)
	}
	if cluster, _ := changed.DataPoints[0].Attributes.Value(attribute.Key("drover.cluster")); cluster.AsString() != "c-1" {
		t.Errorf("cluster of the change = %q, want c-1", cluster.AsString())
	}

	want := attribute.NewSet(
		attribute.String("drover.cluster", rancherCluster),
		attribute.String("kind", kindProject),
		attribute.String("type", watchModified))
	var events int64
	for _, point := range findSum(t, data, "drover.sync.events").DataPoints {
		if point.Attributes.Equals(&want) {
			events = point.Value
		}
	}
	if events != 1 {
		t.Errorf("project MODIFIED events = %d, want 1", events)
	}

	wantOpen := attribute.NewSet(
		attribute.String("drover.cluster", rancherCluster),
		attribute.String("kind", kindProject))
	open := findSum(t, data, "drover.sync.watches.open")
	if len(open.DataPoints) != 1 || !open.DataPoints[0].Attributes.Equals(&wantOpen) || open.DataPoints[0].Value != 0 {
		t.Errorf("drover.sync.watches.open points = %v, want one project point with value 0", open.DataPoints)
	}
}
