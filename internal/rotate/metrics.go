package rotate

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// meterName and tracerName are the instrumentation scope of the rotate
// metrics and the rotate spans.
const (
	meterName  = "github.com/evil8io/drover/internal/rotate"
	tracerName = meterName
)

// metrics has the instruments that record the outcome of one run.
type metrics struct {
	steps    metric.Int64Counter
	duration metric.Float64Histogram
}

// newMetrics creates the instruments of the rotator, on the meter that
// provider gives for meterName.
func newMetrics(provider metric.MeterProvider) (*metrics, error) {
	meter := provider.Meter(meterName)

	steps, err := meter.Int64Counter("drover.rotate.steps",
		metric.WithDescription("Steps of a token rotation run, with their outcome."),
		metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	duration, err := meter.Float64Histogram("drover.rotate.duration",
		metric.WithDescription("Duration of a token rotation run."),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}

	return &metrics{steps: steps, duration: duration}, nil
}

// step records one finished step, with its name and its outcome.
func (m *metrics) step(ctx context.Context, step, outcome string) {
	m.steps.Add(ctx, 1, metric.WithAttributes(
		attribute.String("step", step),
		attribute.String("outcome", outcome),
	))
}

// recordDuration records the duration of one run, in seconds.
func (m *metrics) recordDuration(ctx context.Context, d time.Duration) {
	m.duration.Record(ctx, d.Seconds())
}
