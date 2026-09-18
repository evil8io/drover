package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestLogHandlerAddsTraceFields(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := slog.New(NewLogHandler(slog.NewJSONHandler(&buf, nil)))

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{0x01},
		SpanID:  trace.SpanID{0x02},
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	logger.InfoContext(ctx, "hello")

	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("parse the log line %q: %v", buf.String(), err)
	}
	if got := line["trace_id"]; got != sc.TraceID().String() {
		t.Errorf("trace_id = %v, want %q", got, sc.TraceID().String())
	}
	if got := line["span_id"]; got != sc.SpanID().String() {
		t.Errorf("span_id = %v, want %q", got, sc.SpanID().String())
	}
}

func TestLogHandlerSkipsAnInvalidSpan(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := slog.New(NewLogHandler(slog.NewJSONHandler(&buf, nil)))

	logger.InfoContext(context.Background(), "hello")

	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("parse the log line %q: %v", buf.String(), err)
	}
	if _, ok := line["trace_id"]; ok {
		t.Error("the log line has a trace_id, want none")
	}
	if _, ok := line["span_id"]; ok {
		t.Error("the log line has a span_id, want none")
	}
}
