package projectsync

import (
	"context"
	"testing"

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
	if cluster, _ := changed.DataPoints[0].Attributes.Value(attribute.Key("cluster")); cluster.AsString() != "c-1" {
		t.Errorf("cluster of the change = %q, want c-1", cluster.AsString())
	}

	want := attribute.NewSet(
		attribute.String("cluster", rancherCluster),
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
		attribute.String("cluster", rancherCluster),
		attribute.String("kind", kindProject))
	open := findSum(t, data, "drover.sync.watches.open")
	if len(open.DataPoints) != 1 || !open.DataPoints[0].Attributes.Equals(&wantOpen) || open.DataPoints[0].Value != 0 {
		t.Errorf("drover.sync.watches.open points = %v, want one project point with value 0", open.DataPoints)
	}
}
