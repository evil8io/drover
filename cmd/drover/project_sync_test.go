package main

import (
	"io"
	"strings"
	"testing"
	"time"
)

func TestParseProjectSyncConfigTelemetryDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := parseProjectSyncConfig([]string{
		"--rancher-url", "https://rancher.example.com",
		"--token-file", "/dev/null",
		"--labels", "cost-center",
	}, io.Discard, noEnvironment)
	if err != nil {
		t.Fatalf("parseProjectSyncConfig: %v", err)
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
	if cfg.telemetry.Version != version {
		t.Errorf("telemetry version = %q, want %q", cfg.telemetry.Version, version)
	}
}

func TestParseProjectSyncConfigInsecureSkipVerify(t *testing.T) {
	t.Parallel()

	cfg, err := parseProjectSyncConfig([]string{
		"--rancher-url", "https://rancher.example.com",
		"--token-file", "/dev/null",
		"--labels", "cost-center",
	}, io.Discard, noEnvironment)
	if err != nil {
		t.Fatalf("parseProjectSyncConfig: %v", err)
	}
	if cfg.insecureSkipVerify {
		t.Error("insecure skip verify = true, want false")
	}

	cfg, err = parseProjectSyncConfig([]string{
		"--rancher-url", "https://rancher.example.com",
		"--token-file", "/dev/null",
		"--labels", "cost-center",
		"--rancher-insecure-skip-verify",
	}, io.Discard, noEnvironment)
	if err != nil {
		t.Fatalf("parseProjectSyncConfig: %v", err)
	}
	if !cfg.insecureSkipVerify {
		t.Error("insecure skip verify = false, want true")
	}
}

func TestParseProjectSyncConfigTelemetryFlags(t *testing.T) {
	t.Parallel()

	cfg, err := parseProjectSyncConfig([]string{
		"--rancher-url", "https://rancher.example.com",
		"--token-file", "/dev/null",
		"--labels", "cost-center",
		"--otlp-endpoint", "collector:4317",
		"--otlp-traces=false",
		"--otlp-metrics=false",
		"--service-name", "custom",
	}, io.Discard, noEnvironment)
	if err != nil {
		t.Fatalf("parseProjectSyncConfig: %v", err)
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
}

func TestParseProjectSyncConfigTelemetryFromEnvironment(t *testing.T) {
	t.Parallel()
	environment := map[string]string{
		"OTEL_EXPORTER_OTLP_ENDPOINT": "collector:4317",
		"OTEL_SERVICE_NAME":           "custom",
	}
	getenv := func(name string) string { return environment[name] }

	cfg, err := parseProjectSyncConfig([]string{
		"--rancher-url", "https://rancher.example.com",
		"--token-file", "/dev/null",
		"--labels", "cost-center",
	}, io.Discard, getenv)
	if err != nil {
		t.Fatalf("parseProjectSyncConfig: %v", err)
	}
	if cfg.telemetry.Endpoint != "collector:4317" {
		t.Errorf("otlp endpoint = %q, want collector:4317", cfg.telemetry.Endpoint)
	}
	if cfg.telemetry.ServiceName != "custom" {
		t.Errorf("service name = %q, want custom", cfg.telemetry.ServiceName)
	}
}

// openbaoArgs are the flags of a valid project-sync with the OpenBao config
// on, followed by args.
func openbaoArgs(args ...string) []string {
	return append([]string{
		"--rancher-url", "https://rancher.cattle-system",
		"--token-file", "/dev/null",
		"--service-accounts",
		"--openbao-address", "http://drover-openbao-active.drover-system:8200",
		"--openbao-jwt-file", "/var/run/secrets/openbao/token",
		"--openbao-rancher-url", "https://rancher.example.com/",
	}, args...)
}

func TestParseProjectSyncConfigOpenBao(t *testing.T) {
	t.Parallel()

	cfg, err := parseProjectSyncConfig(openbaoArgs(), io.Discard, noEnvironment)
	if err != nil {
		t.Fatalf("parseProjectSyncConfig: %v", err)
	}
	bao := cfg.openbao
	if bao == nil {
		t.Fatal("OpenBao config = nil, want one")
	}
	if got := bao.Address.String(); got != "http://drover-openbao-active.drover-system:8200" {
		t.Errorf("address = %q", got)
	}
	if got := bao.RancherURL.String(); got != "https://rancher.example.com" {
		t.Errorf("rancher url = %q, want https://rancher.example.com", got)
	}
	if bao.JWTFile != "/var/run/secrets/openbao/token" {
		t.Errorf("jwt file = %q", bao.JWTFile)
	}
	if bao.AuthPath != "kubernetes" || bao.Role != "project-sync" || bao.MountPrefix != "kubernetes" || bao.TokenTTL != 24*time.Hour {
		t.Errorf("defaults = auth path %q, role %q, mount prefix %q, ttl %s; want kubernetes, project-sync, kubernetes, 24h0m0s",
			bao.AuthPath, bao.Role, bao.MountPrefix, bao.TokenTTL)
	}
	if bao.CredentialTTL != 15*time.Minute || bao.CredentialMaxTTL != 2*time.Hour {
		t.Errorf("credential lifetimes = %s and %s, want 15m0s and 2h0m0s", bao.CredentialTTL, bao.CredentialMaxTTL)
	}

	cfg, err = parseProjectSyncConfig(openbaoArgs("--openbao-credential-ttl", "10m", "--openbao-credential-max-ttl", "3h"), io.Discard, noEnvironment)
	if err != nil {
		t.Fatalf("parseProjectSyncConfig with credential lifetimes: %v", err)
	}
	if cfg.openbao.CredentialTTL != 10*time.Minute || cfg.openbao.CredentialMaxTTL != 3*time.Hour {
		t.Errorf("credential lifetimes = %s and %s, want 10m0s and 3h0m0s", cfg.openbao.CredentialTTL, cfg.openbao.CredentialMaxTTL)
	}

	cfg, err = parseProjectSyncConfig([]string{
		"--rancher-url", "https://rancher.cattle-system",
		"--token-file", "/dev/null",
		"--service-accounts",
		"--openbao-role", "other",
	}, io.Discard, noEnvironment)
	if err != nil {
		t.Fatalf("parseProjectSyncConfig without an address: %v", err)
	}
	if cfg.openbao != nil {
		t.Error("OpenBao config without an address = set, want nil")
	}
}

func TestParseProjectSyncConfigOpenBaoErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args []string
		want string
	}{
		{"address without service accounts", []string{
			"--rancher-url", "https://rancher.cattle-system", "--token-file", "/dev/null", "--labels", "team",
			"--openbao-address", "http://openbao:8200",
		}, "-openbao-address needs -service-accounts"},
		{"another OpenBao flag without service accounts", []string{
			"--rancher-url", "https://rancher.cattle-system", "--token-file", "/dev/null", "--labels", "team",
			"--openbao-role", "other",
		}, "-openbao-role needs -service-accounts"},
		{"address with a path", openbaoArgs("--openbao-address", "http://openbao:8200/v1"), "-openbao-address path"},
		{"address with another scheme", openbaoArgs("--openbao-address", "ftp://openbao:8200"), "-openbao-address scheme"},
		{"no rancher url", openbaoArgs("--openbao-rancher-url", ""), "-openbao-rancher-url is required"},
		{"rancher url over http", openbaoArgs("--openbao-rancher-url", "http://rancher.example.com"), "is not https"},
		{"rancher url with a path", openbaoArgs("--openbao-rancher-url", "https://rancher.example.com/rancher"), "-openbao-rancher-url path"},
		{"no jwt file", openbaoArgs("--openbao-jwt-file", ""), "-openbao-jwt-file is required"},
		{"empty auth path", openbaoArgs("--openbao-auth-path", "/"), "-openbao-auth-path is empty"},
		{"empty role", openbaoArgs("--openbao-role", ""), "-openbao-role is empty"},
		{"empty mount prefix", openbaoArgs("--openbao-mount-prefix", ""), "-openbao-mount-prefix is empty"},
		{"short token lifetime", openbaoArgs("--openbao-token-ttl", "5m"), "-openbao-token-ttl 5m0s is shorter"},
		{"zero credential lifetime", openbaoArgs("--openbao-credential-ttl", "0s"), "-openbao-credential-ttl 0s is shorter than 10m0s"},
		{"credential lifetime below 10 minutes", openbaoArgs("--openbao-credential-ttl", "9m59s"), "-openbao-credential-ttl 9m59s is shorter than 10m0s"},
		{"credential lifetime over its maximum", openbaoArgs("--openbao-credential-ttl", "3h"), "-openbao-credential-max-ttl 2h0m0s is shorter than -openbao-credential-ttl 3h0m0s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := parseProjectSyncConfig(tt.args, io.Discard, noEnvironment)
			if err == nil {
				t.Fatal("parseProjectSyncConfig = nil error, want an error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q, want it to contain %q", err, tt.want)
			}
		})
	}
}
