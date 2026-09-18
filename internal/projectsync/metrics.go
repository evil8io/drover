package projectsync

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// meterName and tracerName are the instrumentation scope of the sync metrics
// and the sync spans.
const (
	meterName  = "github.com/evil8io/drover/internal/projectsync"
	tracerName = meterName
)

const (
	outcomeOK    = "ok"
	outcomeError = "error"
)

// metrics has the instruments that record the outcome of a reconcile run.
type metrics struct {
	reconciles metric.Int64Counter
	patched    metric.Int64Counter
	errors     metric.Int64Counter
	duration   metric.Float64Histogram
}

// newMetrics creates the instruments of the syncer, on the meter that
// provider gives for meterName.
func newMetrics(provider metric.MeterProvider) (*metrics, error) {
	meter := provider.Meter(meterName)

	reconciles, err := meter.Int64Counter("drover.sync.reconciles",
		metric.WithDescription("Reconcile runs that the syncer completes."),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	patched, err := meter.Int64Counter("drover.sync.namespaces.patched",
		metric.WithDescription("Namespaces that a reconcile run patches."),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	errs, err := meter.Int64Counter("drover.sync.errors",
		metric.WithDescription("Errors that a reconcile run logs."),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	duration, err := meter.Float64Histogram("drover.sync.duration",
		metric.WithDescription("Duration of a reconcile run."),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}

	return &metrics{reconciles: reconciles, patched: patched, errors: errs, duration: duration}, nil
}

// reconcileDone records one finished reconcile run, with its outcome and its
// duration in seconds.
func (m *metrics) reconcileDone(ctx context.Context, outcome string, duration time.Duration) {
	m.reconciles.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
	m.duration.Record(ctx, duration.Seconds())
}

// namespacePatched records one namespace that a reconcile run patches.
func (m *metrics) namespacePatched(ctx context.Context) {
	m.patched.Add(ctx, 1)
}

// syncError records one error that a reconcile run logs.
func (m *metrics) syncError(ctx context.Context) {
	m.errors.Add(ctx, 1)
}
