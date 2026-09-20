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
	requests         metric.Int64Counter
	requestDur       metric.Float64Histogram
	watchesOpen      metric.Int64UpDownCounter
	watchesRejected  metric.Int64Counter
	eventsDropped    metric.Int64Counter
	fetchesThrottled metric.Int64Counter
	fanoutNS         metric.Int64Histogram
	fanoutsCapped    metric.Int64Counter
	fanoutsSkipped   metric.Int64Counter
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
	watchesRejected, err := meter.Int64Counter("drover.filter.watches.rejected",
		metric.WithDescription("Upgraded namespace watch streams that the filter ends at once, because the websocket connection has an extension."),
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
	fetchesThrottled, err := meter.Int64Counter("drover.filter.fetch.throttled",
		metric.WithDescription("Fetches of an allowed set that the fetch rate limit throttles, answered with 429."),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}

	fanoutNS, err := meter.Int64Histogram("drover.filter.fanout.namespaces",
		metric.WithDescription("Namespaces that one fan-out of a cluster-wide list requests."),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	fanoutsCapped, err := meter.Int64Counter("drover.filter.fanout.capped",
		metric.WithDescription("Cluster-wide lists that the filter answers with 403, because the caller may see more namespaces than the fan-out limit."),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	fanoutsSkipped, err := meter.Int64Counter("drover.filter.fanout.skipped",
		metric.WithDescription("Namespaces of a fan-out that give no collection, and that the merged answer leaves out."),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}

	return &metrics{
		requests:         requests,
		requestDur:       requestDur,
		watchesOpen:      watchesOpen,
		watchesRejected:  watchesRejected,
		eventsDropped:    eventsDropped,
		fetchesThrottled: fetchesThrottled,
		fanoutNS:         fanoutNS,
		fanoutsCapped:    fanoutsCapped,
		fanoutsSkipped:   fanoutsSkipped,
	}, nil
}

// recordRequest records one list or watch request, with its path, outcome,
// cluster and watch attributes, and its duration in seconds. The resource of
// a collection request is not an attribute, because a cluster with many
// custom resources would give the instrument an unbounded attribute set.
func (m *metrics) recordRequest(ctx context.Context, result listResult, duration time.Duration) {
	attrs := metric.WithAttributes(
		attribute.String("path", result.path),
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

// watchRejected records one upgraded watch stream that the filter ends at
// once, for the cluster.
func (m *metrics) watchRejected(ctx context.Context, cluster string) {
	m.watchesRejected.Add(ctx, 1, metric.WithAttributes(attribute.String("cluster", cluster)))
}

// eventDropped records one dropped watch event, for the cluster.
func (m *metrics) eventDropped(ctx context.Context, cluster string) {
	m.eventsDropped.Add(ctx, 1, metric.WithAttributes(attribute.String("cluster", cluster)))
}

// fetchThrottled records one fetch that the rate limit throttles, with a 429 answer.
func (m *metrics) fetchThrottled(ctx context.Context) {
	m.fetchesThrottled.Add(ctx, 1)
}

// fanoutNamespaces records the namespace count of one fan-out, for the cluster.
func (m *metrics) fanoutNamespaces(ctx context.Context, cluster string, count int) {
	m.fanoutNS.Record(ctx, int64(count), metric.WithAttributes(attribute.String("cluster", cluster)))
}

// fanoutCapped records one cluster-wide list above the fan-out limit, for the cluster.
func (m *metrics) fanoutCapped(ctx context.Context, cluster string) {
	m.fanoutsCapped.Add(ctx, 1, metric.WithAttributes(attribute.String("cluster", cluster)))
}

// fanoutSkipped records one namespace that the merged answer leaves out, for the cluster.
func (m *metrics) fanoutSkipped(ctx context.Context, cluster string) {
	m.fanoutsSkipped.Add(ctx, 1, metric.WithAttributes(attribute.String("cluster", cluster)))
}
