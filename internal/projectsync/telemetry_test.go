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
