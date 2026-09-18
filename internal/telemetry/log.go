package telemetry

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// logHandler wraps a slog.Handler and adds trace_id and span_id from the span
// context of a log record.
type logHandler struct {
	next slog.Handler
}

// NewLogHandler wraps next so a log record gets a trace_id attribute and a
// span_id attribute, from the span context of the record's context, when
// that span context is valid.
func NewLogHandler(next slog.Handler) slog.Handler {
	return &logHandler{next: next}
}

func (h *logHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *logHandler) Handle(ctx context.Context, record slog.Record) error {
	if span := trace.SpanContextFromContext(ctx); span.IsValid() {
		record.AddAttrs(
			slog.String("trace_id", span.TraceID().String()),
			slog.String("span_id", span.SpanID().String()),
		)
	}
	return h.next.Handle(ctx, record)
}

func (h *logHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &logHandler{next: h.next.WithAttrs(attrs)}
}

func (h *logHandler) WithGroup(name string) slog.Handler {
	return &logHandler{next: h.next.WithGroup(name)}
}
