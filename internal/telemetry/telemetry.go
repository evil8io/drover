// Package telemetry sets up OpenTelemetry tracing and metrics for a drover
// command, and adds the trace id and the span id of a log record to its JSON
// output.
package telemetry

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"google.golang.org/grpc/credentials"
)

// metricInterval is the export interval of the periodic metric reader. A 60 s
// interval leaves a one-minute rate query empty in this platform.
const metricInterval = 30 * time.Second

// Config configures Setup.
type Config struct {
	// Endpoint is the OTLP gRPC endpoint of the collector, as host:port or as
	// a URL. An empty value turns telemetry off.
	Endpoint string
	// Traces sends spans to Endpoint.
	Traces bool
	// Metrics sends metrics to Endpoint, and starts the Go runtime metrics.
	Metrics bool
	// ServiceName is the service.name resource attribute. An empty value sets
	// no service.name, so that a collector can derive one.
	ServiceName string
	// Version is the service.version resource attribute.
	Version string
	// ObservableGaugeCardinalityLimit is the cardinality limit of each
	// observable gauge. With zero or a negative value, the limit is the default of the SDK.
	ObservableGaugeCardinalityLimit int
}

// Telemetry has the providers that Setup starts. Shutdown stops them.
type Telemetry struct {
	tracerProvider *sdktrace.TracerProvider
	meterProvider  *sdkmetric.MeterProvider
}

// Setup sets the global propagator to a composite of TraceContext and
// Baggage. It does this also when Config.Endpoint is empty, so a command
// continues an incoming traceparent even with telemetry off.
//
// An empty Config.Endpoint returns a Telemetry with no exporter: every span
// and every measurement then drops, and Shutdown does nothing.
//
// A set Config.Endpoint starts the trace provider and the metric provider
// that Config selects, over OTLP on gRPC, and sets them as the global
// providers. The trace sampler is always AlwaysSample. A set Config.Metrics
// also starts the Go runtime metrics.
func Setup(ctx context.Context, cfg Config) (*Telemetry, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	if cfg.Endpoint == "" {
		return &Telemetry{}, nil
	}

	target, secure, err := parseEndpoint(cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("the OTLP endpoint: %w", err)
	}

	res, err := newResource(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("build the telemetry resource: %w", err)
	}

	tel := &Telemetry{}

	if cfg.Traces {
		if tel.tracerProvider, err = setupTraces(ctx, target, secure, res); err != nil {
			return nil, err
		}
		otel.SetTracerProvider(tel.tracerProvider)
	}

	if cfg.Metrics {
		if tel.meterProvider, err = setupMetrics(ctx, target, secure, res, cfg.ObservableGaugeCardinalityLimit); err != nil {
			return nil, err
		}
		otel.SetMeterProvider(tel.meterProvider)

		if err := runtime.Start(runtime.WithMeterProvider(tel.meterProvider)); err != nil {
			return nil, fmt.Errorf("start the Go runtime metrics: %w", err)
		}
	}

	return tel, nil
}

// newResource returns the resource of the providers: the SDK attributes, the
// attributes of OTEL_RESOURCE_ATTRIBUTES and OTEL_SERVICE_NAME,
// service.version, and service.name when Config.ServiceName is not empty. It
// has no detector of resource.Default, because that one adds
// unknown_service:<executable> as service.name, and a collector derives
// service.name only for a resource without one. An invalid
// OTEL_RESOURCE_ATTRIBUTES goes to the error handler of otel, as it does for
// resource.Default.
func newResource(ctx context.Context, cfg Config) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{semconv.ServiceVersion(cfg.Version)}
	if cfg.ServiceName != "" {
		attrs = append(attrs, semconv.ServiceName(cfg.ServiceName))
	}
	res, err := resource.New(ctx, resource.WithTelemetrySDK(), resource.WithFromEnv(), resource.WithAttributes(attrs...))
	if errors.Is(err, resource.ErrPartialResource) {
		otel.Handle(err)
		err = nil
	}
	return res, err
}

func setupTraces(ctx context.Context, target string, secure bool, res *resource.Resource) (*sdktrace.TracerProvider, error) {
	options := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(target)}
	if secure {
		options = append(options, otlptracegrpc.WithTLSCredentials(credentials.NewTLS(&tls.Config{})))
	} else {
		options = append(options, otlptracegrpc.WithInsecure())
	}
	exporter, err := otlptracegrpc.New(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("start the trace exporter: %w", err)
	}
	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(newURLQueryRedactingExporter(exporter)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	), nil
}

// setupMetrics starts the metric provider. A gaugeLimit above zero is the
// cardinality limit of each observable gauge. An instrument of every other kind
// has the limit of the provider.
func setupMetrics(ctx context.Context, target string, secure bool, res *resource.Resource, gaugeLimit int) (*sdkmetric.MeterProvider, error) {
	options := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(target)}
	if secure {
		options = append(options, otlpmetricgrpc.WithTLSCredentials(credentials.NewTLS(&tls.Config{})))
	} else {
		options = append(options, otlpmetricgrpc.WithInsecure())
	}
	exporter, err := otlpmetricgrpc.New(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("start the metric exporter: %w", err)
	}
	readerOptions := []sdkmetric.PeriodicReaderOption{sdkmetric.WithInterval(metricInterval)}
	if gaugeLimit > 0 {
		readerOptions = append(readerOptions, sdkmetric.WithCardinalityLimitSelector(func(kind sdkmetric.InstrumentKind) (int, bool) {
			if kind == sdkmetric.InstrumentKindObservableGauge {
				return gaugeLimit, false
			}
			return 0, true
		}))
	}
	reader := sdkmetric.NewPeriodicReader(exporter, readerOptions...)
	return sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
	), nil
}

// Shutdown flushes and stops every provider that Setup started, and returns
// their joined error. Shutdown on the Telemetry of an empty endpoint does
// nothing.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	var errs []error
	if t.tracerProvider != nil {
		errs = append(errs, t.tracerProvider.ForceFlush(ctx))
		errs = append(errs, t.tracerProvider.Shutdown(ctx))
	}
	if t.meterProvider != nil {
		errs = append(errs, t.meterProvider.ForceFlush(ctx))
		errs = append(errs, t.meterProvider.Shutdown(ctx))
	}
	return errors.Join(errs...)
}
