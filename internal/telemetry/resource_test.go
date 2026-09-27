package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// providerResources returns the resource of a span and the resource of the
// metrics that the SDK providers give with the resource of cfg, so that a test
// sees what WithResource adds.
func providerResources(t *testing.T, cfg Config) map[string]*resource.Resource {
	t.Helper()
	ctx := context.Background()
	res, err := newResource(ctx, cfg)
	if err != nil {
		t.Fatalf("newResource: %v", err)
	}

	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithResource(res), sdktrace.WithSpanProcessor(recorder))
	_, span := tracerProvider.Tracer("test").Start(ctx, "test")
	span.End()
	ended := recorder.Ended()
	if len(ended) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(ended))
	}

	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), sdkmetric.WithReader(reader))
	counter, err := meterProvider.Meter("test").Int64Counter("test")
	if err != nil {
		t.Fatalf("counter: %v", err)
	}
	counter.Add(ctx, 1)
	var data metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &data); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	return map[string]*resource.Resource{"trace": ended[0].Resource(), "metric": data.Resource}
}

func value(res *resource.Resource, key string) (string, bool) {
	v, ok := res.Set().Value(attribute.Key(key))
	return v.AsString(), ok
}

func TestResourceHasNoServiceNameForAnEmptyName(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "k8s.pod.uid=uid-1")

	for kind, res := range providerResources(t, Config{Version: "1.2.3"}) {
		if got, ok := value(res, "service.name"); ok {
			t.Errorf("%s resource service.name = %q, want no key", kind, got)
		}
		if got, _ := value(res, "k8s.pod.uid"); got != "uid-1" {
			t.Errorf("%s resource k8s.pod.uid = %q, want uid-1", kind, got)
		}
		if got, _ := value(res, "service.version"); got != "1.2.3" {
			t.Errorf("%s resource service.version = %q, want 1.2.3", kind, got)
		}
		if got, _ := value(res, "telemetry.sdk.language"); got != "go" {
			t.Errorf("%s resource telemetry.sdk.language = %q, want go", kind, got)
		}
	}
}

func TestResourceHasTheServiceName(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "k8s.pod.uid=uid-1")

	for kind, res := range providerResources(t, Config{ServiceName: "drover", Version: "1.2.3"}) {
		if got, _ := value(res, "service.name"); got != "drover" {
			t.Errorf("%s resource service.name = %q, want drover", kind, got)
		}
		if got, _ := value(res, "k8s.pod.uid"); got != "uid-1" {
			t.Errorf("%s resource k8s.pod.uid = %q, want uid-1", kind, got)
		}
	}
}
