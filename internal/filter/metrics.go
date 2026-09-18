package filter

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// meterName is the instrumentation scope of the filter metrics.
const meterName = "github.com/evil8io/drover/internal/filter"

// metrics has the instruments that record the outcome of a request that the
// filter answers.
type metrics struct {
	requests      metric.Int64Counter
	requestDur    metric.Float64Histogram
	watchesOpen   metric.Int64UpDownCounter
	eventsDropped metric.Int64Counter
}

// newMetrics creates the instruments of the filter, on the meter that
// provider gives for meterName.
func newMetrics(provider metric.MeterProvider) (*metrics, error) {
	meter := provider.Meter(meterName)

	requests, err := meter.Int64Counter("drover.filter.requests",
		metric.WithDescription("Namespace list and watch requests that the filter answers."),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	requestDur, err := meter.Float64Histogram("drover.filter.request.duration",
		metric.WithDescription("Duration of a namespace list or watch request that the filter answers."),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	watchesOpen, err := meter.Int64UpDownCounter("drover.filter.watches.open",
		metric.WithDescription("Filtered namespace watch streams that are open."),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	eventsDropped, err := meter.Int64Counter("drover.filter.events.dropped",
		metric.WithDescription("Namespace watch events that the filter drops, because the caller may not see the object."),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}

	return &metrics{
		requests:      requests,
		requestDur:    requestDur,
		watchesOpen:   watchesOpen,
		eventsDropped: eventsDropped,
	}, nil
}

// recordRequest records one namespace list or watch request, with its
// outcome, cluster and watch attributes, and its duration in seconds.
func (m *metrics) recordRequest(ctx context.Context, result listResult, duration time.Duration) {
	attrs := metric.WithAttributes(
		attribute.String("outcome", result.outcome),
		attribute.String("cluster", result.cluster),
		attribute.Bool("watch", result.watch),
	)
	m.requests.Add(ctx, 1, attrs)
	m.requestDur.Record(ctx, duration.Seconds(), attrs)
}

// watchOpened raises drover.filter.watches.open by one, for a filtered watch
// stream that starts.
func (m *metrics) watchOpened(ctx context.Context) {
	m.watchesOpen.Add(ctx, 1)
}

// watchClosed lowers drover.filter.watches.open by one, for a filtered watch
// stream that ends.
func (m *metrics) watchClosed(ctx context.Context) {
	m.watchesOpen.Add(ctx, -1)
}

// eventDropped records one dropped watch event, for the cluster.
func (m *metrics) eventDropped(ctx context.Context, cluster string) {
	m.eventsDropped.Add(ctx, 1, metric.WithAttributes(attribute.String("cluster", cluster)))
}
