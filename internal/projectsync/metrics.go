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

	// originReconcile and originWatch name the path that patched a namespace.
	originReconcile = "reconcile"
	originWatch     = "watch"
)

// metrics has the instruments that record the outcome of a reconcile run.
type metrics struct {
	reconciles metric.Int64Counter
	patched    metric.Int64Counter
	errors     metric.Int64Counter
	duration   metric.Float64Histogram
	events     metric.Int64Counter
	watches    metric.Int64UpDownCounter
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
		metric.WithDescription("Namespaces that the syncer patches."),
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
	events, err := meter.Int64Counter("drover.sync.events",
		metric.WithDescription("Namespace watch events that the syncer reads."),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	watches, err := meter.Int64UpDownCounter("drover.sync.watches.open",
		metric.WithDescription("Namespace watch streams that are open."),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}

	return &metrics{
		reconciles: reconciles,
		patched:    patched,
		errors:     errs,
		duration:   duration,
		events:     events,
		watches:    watches,
	}, nil
}

// reconcileDone records one finished reconcile run, with its outcome and its
// duration in seconds.
func (m *metrics) reconcileDone(ctx context.Context, outcome string, duration time.Duration) {
	m.reconciles.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
	m.duration.Record(ctx, duration.Seconds())
}

// namespacePatched records one namespace that the syncer patches. origin is
// the path that found the namespace, the reconcile run or the watch.
func (m *metrics) namespacePatched(ctx context.Context, origin string) {
	m.patched.Add(ctx, 1, metric.WithAttributes(attribute.String("origin", origin)))
}

// watchEvent records one event of the namespace watch of a cluster.
func (m *metrics) watchEvent(ctx context.Context, cluster, eventType string) {
	m.events.Add(ctx, 1, metric.WithAttributes(
		attribute.String("cluster", cluster),
		attribute.String("type", eventType)))
}

// watchOpened and watchClosed track the open watch streams per cluster.
func (m *metrics) watchOpened(ctx context.Context, cluster string) {
	m.watches.Add(ctx, 1, metric.WithAttributes(attribute.String("cluster", cluster)))
}

func (m *metrics) watchClosed(ctx context.Context, cluster string) {
	m.watches.Add(ctx, -1, metric.WithAttributes(attribute.String("cluster", cluster)))
}

// syncError records one error that a reconcile run logs.
func (m *metrics) syncError(ctx context.Context) {
	m.errors.Add(ctx, 1)
}
