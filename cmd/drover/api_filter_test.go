package main

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func tokenFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write the token file: %v", err)
	}
	return path
}

func TestParseConfigErrors(t *testing.T) {
	t.Parallel()
	token := tokenFile(t, "token\n")

	tests := []struct {
		name string
		args []string
	}{
		{"no upstream", []string{"--token-file", token}},
		{"no token file", []string{"--upstream", "https://rancher.example.com"}},
		{"upstream with a path", []string{"--upstream", "https://rancher.example.com/rancher", "--token-file", token}},
		{"upstream with another scheme", []string{"--upstream", "ftp://rancher.example.com", "--token-file", token}},
		{"upstream without a host", []string{"--upstream", "https://", "--token-file", token}},
		{"unknown log level", []string{"--upstream", "https://rancher.example.com", "--token-file", token, "--log-level", "trace"}},
		{"version is not a subcommand flag", []string{"--version"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := parseConfig(test.args, io.Discard, noEnvironment); err == nil {
				t.Errorf("parseConfig(%v) returned no error", test.args)
			}
		})
	}
}

func TestParseConfigAcceptsAbsentTokenFile(t *testing.T) {
	t.Parallel()
	token := tokenFile(t, "token\n")
	missing := token + ".missing"
	empty := tokenFile(t, "")

	for _, path := range []string{missing, empty} {
		cfg, err := parseConfig([]string{
			"--upstream", "https://rancher.example.com",
			"--token-file", path,
		}, io.Discard, noEnvironment)
		if err != nil {
			t.Fatalf("parseConfig with token file %q: %v", path, err)
		}
		if cfg.tokenFile != path {
			t.Errorf("token file = %q, want %q", cfg.tokenFile, path)
		}
	}
}

func TestTokenFileReady(t *testing.T) {
	t.Parallel()
	token := tokenFile(t, "token\n")
	empty := tokenFile(t, "")
	blank := tokenFile(t, "  \n")

	if !tokenFileReady(token) {
		t.Error("tokenFileReady with a token, want true")
	}
	if tokenFileReady(token + ".missing") {
		t.Error("tokenFileReady with an absent file, want false")
	}
	if tokenFileReady(empty) {
		t.Error("tokenFileReady with an empty file, want false")
	}
	if tokenFileReady(blank) {
		t.Error("tokenFileReady with a blank file, want false")
	}
}

func TestParseConfigValid(t *testing.T) {
	t.Parallel()
	token := tokenFile(t, "token\n")

	cfg, err := parseConfig([]string{
		"--listen", "127.0.0.1:9090",
		"--upstream", "https://rancher.example.com/",
		"--token-file", token,
		"--cache-ttl", "30s",
		"--log-level", "debug",
		"--shutdown-grace", "5s",
		"--otlp-endpoint", "collector:4317",
		"--otlp-traces=false",
		"--otlp-metrics=false",
		"--service-name", "custom",
	}, io.Discard, noEnvironment)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.listen != "127.0.0.1:9090" {
		t.Errorf("listen = %q, want 127.0.0.1:9090", cfg.listen)
	}
	if got := cfg.upstream.String(); got != "https://rancher.example.com" {
		t.Errorf("upstream = %q, want https://rancher.example.com", got)
	}
	if cfg.tokenFile != token {
		t.Errorf("token file = %q, want %q", cfg.tokenFile, token)
	}
	if cfg.cacheTTL != 30*time.Second {
		t.Errorf("cache TTL = %s, want 30s", cfg.cacheTTL)
	}
	if cfg.logLevel != slog.LevelDebug {
		t.Errorf("log level = %s, want debug", cfg.logLevel)
	}
	if cfg.shutdownGrace != 5*time.Second {
		t.Errorf("shutdown grace = %s, want 5s", cfg.shutdownGrace)
	}
	if cfg.telemetry.Endpoint != "collector:4317" {
		t.Errorf("otlp endpoint = %q, want collector:4317", cfg.telemetry.Endpoint)
	}
	if cfg.telemetry.Traces {
		t.Error("otlp traces = true, want false")
	}
	if cfg.telemetry.Metrics {
		t.Error("otlp metrics = true, want false")
	}
	if cfg.telemetry.ServiceName != "custom" {
		t.Errorf("service name = %q, want custom", cfg.telemetry.ServiceName)
	}
	if cfg.telemetry.Version != version {
		t.Errorf("telemetry version = %q, want %q", cfg.telemetry.Version, version)
	}
}

func TestParseConfigShutdownGraceDefault(t *testing.T) {
	t.Parallel()
	token := tokenFile(t, "token\n")

	cfg, err := parseConfig([]string{
		"--upstream", "https://rancher.example.com",
		"--token-file", token,
	}, io.Discard, noEnvironment)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.shutdownGrace != 20*time.Second {
		t.Errorf("shutdown grace = %s, want 20s", cfg.shutdownGrace)
	}
}

func TestParseConfigTelemetryDefaults(t *testing.T) {
	t.Parallel()
	token := tokenFile(t, "token\n")

	cfg, err := parseConfig([]string{
		"--upstream", "https://rancher.example.com",
		"--token-file", token,
	}, io.Discard, noEnvironment)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.telemetry.Endpoint != "" {
		t.Errorf("otlp endpoint = %q, want empty", cfg.telemetry.Endpoint)
	}
	if !cfg.telemetry.Traces {
		t.Error("otlp traces = false, want true")
	}
	if !cfg.telemetry.Metrics {
		t.Error("otlp metrics = false, want true")
	}
	if cfg.telemetry.ServiceName != "drover" {
		t.Errorf("service name = %q, want drover", cfg.telemetry.ServiceName)
	}
}

func TestParseConfigTelemetryFromEnvironment(t *testing.T) {
	t.Parallel()
	token := tokenFile(t, "token\n")
	environment := map[string]string{
		"OTEL_EXPORTER_OTLP_ENDPOINT": "collector:4317",
		"OTEL_SERVICE_NAME":           "custom",
	}
	getenv := func(name string) string { return environment[name] }

	cfg, err := parseConfig([]string{
		"--upstream", "https://rancher.example.com",
		"--token-file", token,
	}, io.Discard, getenv)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.telemetry.Endpoint != "collector:4317" {
		t.Errorf("otlp endpoint = %q, want collector:4317", cfg.telemetry.Endpoint)
	}
	if cfg.telemetry.ServiceName != "custom" {
		t.Errorf("service name = %q, want custom", cfg.telemetry.ServiceName)
	}
}
