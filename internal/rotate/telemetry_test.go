package rotate

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestRunProducesARotateSpanWithAChildPerStep checks that a rotation run
// produces the rotate span, with a child span for every step it runs. It
// reads a span recorder, not a live exporter.
func TestRunProducesARotateSpanWithAChildPerStep(t *testing.T) {
	t.Parallel()
	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	rancher := newFakeRancher(t)
	token := rancher.addToken("token-old", testDescription, 30*time.Hour, 48*time.Hour, true)
	kube := newFakeKube(t, map[string]string{testKey: token.value})
	h := newHarness(t, kube, rancher)
	withPasswordSecret(h)
	h.cfg.TracerProvider = tracerProvider

	if err := h.run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	spans := recorder.Ended()
	var run sdktrace.ReadOnlySpan
	for _, span := range spans {
		if span.Name() == "rotate" {
			run = span
		}
	}
	if run == nil {
		t.Fatal("no span named rotate")
	}

	wantSteps := []string{
		stepPasswordSync, stepSecretGet, stepTokenCheck, stepLogin,
		stepTokenCreate, stepSecretPatch, stepTokenPrune, stepLogout,
	}
	for _, step := range wantSteps {
		found := false
		for _, span := range spans {
			if span.Name() != step {
				continue
			}
			found = true
			if got := span.Parent().SpanID(); got != run.SpanContext().SpanID() {
				t.Errorf("the span %s has parent %s, want the rotate span %s", step, got, run.SpanContext().SpanID())
			}
		}
		if !found {
			t.Errorf("no span named %s", step)
		}
	}
}

// TestRunLoginFailureFailsTheStepAndTheRun checks a failed login. The login
// span and the rotate span both get the error status, and drover.rotate.steps
// records the outcome failed for the step.
func TestRunLoginFailureFailsTheStepAndTheRun(t *testing.T) {
	t.Parallel()
	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	rancher := newFakeRancher(t)
	rancher.loginStatus = 401
	kube := newFakeKube(t, nil)
	h := newHarness(t, kube, rancher)
	h.cfg.TracerProvider = tracerProvider
	h.cfg.MeterProvider = meterProvider

	if err := h.run(); err == nil {
		t.Fatal("Run returned no error")
	}

	var loginSpan, runSpan sdktrace.ReadOnlySpan
	for _, span := range recorder.Ended() {
		switch span.Name() {
		case stepLogin:
			loginSpan = span
		case "rotate":
			runSpan = span
		}
	}
	if loginSpan == nil {
		t.Fatal("no span named login")
	}
	if got := loginSpan.Status().Code; got != codes.Error {
		t.Errorf("the login span status is %v, want codes.Error", got)
	}
	if runSpan == nil {
		t.Fatal("no span named rotate")
	}
	if got := runSpan.Status().Code; got != codes.Error {
		t.Errorf("the rotate span status is %v, want codes.Error", got)
	}

	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	sum := findSum(t, data, "drover.rotate.steps")
	found := false
	for _, point := range sum.DataPoints {
		step, ok := point.Attributes.Value(attribute.Key("step"))
		if !ok || step.AsString() != stepLogin {
			continue
		}
		found = true
		outcome, ok := point.Attributes.Value(attribute.Key("outcome"))
		if !ok || outcome.AsString() != outcomeFailed {
			t.Errorf("the login step outcome attribute = %v, ok=%v, want %q", outcome, ok, outcomeFailed)
		}
	}
	if !found {
		t.Fatal("no drover.rotate.steps point for the login step")
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
