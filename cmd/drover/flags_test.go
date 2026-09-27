package main

import (
	"flag"
	"io"
	"testing"
)

func TestTelemetryFlagsServiceName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		env  map[string]string
		args []string
		want string
	}{
		{"default", nil, nil, "drover"},
		{"from OTEL_SERVICE_NAME", map[string]string{"OTEL_SERVICE_NAME": "custom"}, nil, "custom"},
		{"flag over OTEL_SERVICE_NAME", map[string]string{"OTEL_SERVICE_NAME": "custom"}, []string{"--service-name", "other"}, "other"},
		{"explicitly empty", nil, []string{"--service-name="}, ""},
		{"explicitly empty over OTEL_SERVICE_NAME", map[string]string{"OTEL_SERVICE_NAME": "custom"}, []string{"--service-name="}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			flags := flag.NewFlagSet("drover test", flag.ContinueOnError)
			flags.SetOutput(io.Discard)
			tf := registerTelemetryFlags(flags, func(name string) string { return tt.env[name] })
			if err := flags.Parse(tt.args); err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got := tf.config().ServiceName; got != tt.want {
				t.Errorf("service name = %q, want %q", got, tt.want)
			}
		})
	}
}
