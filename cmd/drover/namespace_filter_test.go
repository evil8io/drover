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
	empty := tokenFile(t, "")

	tests := []struct {
		name string
		args []string
	}{
		{"no upstream", []string{"--token-file", token}},
		{"no token file", []string{"--upstream", "https://rancher.example.com"}},
		{"upstream with a path", []string{"--upstream", "https://rancher.example.com/rancher", "--token-file", token}},
		{"upstream with another scheme", []string{"--upstream", "ftp://rancher.example.com", "--token-file", token}},
		{"upstream without a host", []string{"--upstream", "https://", "--token-file", token}},
		{"token file that does not exist", []string{"--upstream", "https://rancher.example.com", "--token-file", token + ".missing"}},
		{"empty token file", []string{"--upstream", "https://rancher.example.com", "--token-file", empty}},
		{"unknown log level", []string{"--upstream", "https://rancher.example.com", "--token-file", token, "--log-level", "trace"}},
		{"version is not a subcommand flag", []string{"--version"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := parseConfig(test.args, io.Discard); err == nil {
				t.Errorf("parseConfig(%v) returned no error", test.args)
			}
		})
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
	}, io.Discard)
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
}
