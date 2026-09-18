// Command rancher-namespace-filter is an HTTP reverse proxy in front of a
// Rancher server.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/evil8io/rancher-namespace-filter/internal/filter"
)

var version = "dev"

type config struct {
	listen      string
	upstream    *url.URL
	caFile      string
	tokenFile   string
	cacheTTL    time.Duration
	logLevel    slog.Level
	showVersion bool
}

func main() {
	cfg, err := parseConfig(os.Args[1:], os.Stderr)
	if err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(os.Stderr, err)
		}
		os.Exit(2)
	}
	if cfg.showVersion {
		fmt.Println(version)
		return
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.logLevel}))
	handler, err := filter.New(filter.Config{
		Upstream:  cfg.upstream,
		CAFile:    cfg.caFile,
		TokenFile: cfg.tokenFile,
		CacheTTL:  cfg.cacheTTL,
		Logger:    logger,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	baseCtx, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()

	server := &http.Server{
		Addr:              cfg.listen,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return baseCtx },
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelDebug),
	}

	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() { serveErr <- server.ListenAndServe() }()

	logger.Info("start",
		"version", version,
		"listen", cfg.listen,
		"upstream", cfg.upstream.String(),
		"cache_ttl", cfg.cacheTTL.String(),
	)

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("the server stopped", "error", err.Error())
			os.Exit(1)
		}
	case <-signalCtx.Done():
		stop()
		logger.Info("shutdown")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Warn("the shutdown did not complete", "error", err.Error())
		}
	}
}

func parseConfig(args []string, output io.Writer) (config, error) {
	flags := flag.NewFlagSet("rancher-namespace-filter", flag.ContinueOnError)
	flags.SetOutput(output)

	var (
		cfg      config
		upstream string
		logLevel string
	)
	flags.StringVar(&cfg.listen, "listen", ":8080", "listen address")
	flags.StringVar(&upstream, "upstream", "", "Rancher URL, http:// or https://")
	flags.StringVar(&cfg.caFile, "upstream-ca-file", "", "PEM bundle that verifies an https upstream")
	flags.StringVar(&cfg.tokenFile, "token-file", "", "file with the API token of the service user")
	flags.DurationVar(&cfg.cacheTTL, "cache-ttl", 15*time.Second, "lifetime of a cached allowed set")
	flags.StringVar(&logLevel, "log-level", "info", "debug, info, warn or error")
	flags.BoolVar(&cfg.showVersion, "version", false, "print the version and exit")

	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	if cfg.showVersion {
		return cfg, nil
	}

	level, err := parseLevel(logLevel)
	if err != nil {
		return config{}, err
	}
	cfg.logLevel = level

	if upstream == "" {
		return config{}, errors.New("-upstream is required")
	}
	target, err := url.Parse(upstream)
	if err != nil {
		return config{}, fmt.Errorf("-upstream: %w", err)
	}
	if target.Scheme != "http" && target.Scheme != "https" {
		return config{}, fmt.Errorf("-upstream scheme %q is not http or https", target.Scheme)
	}
	if target.Host == "" {
		return config{}, errors.New("-upstream has no host")
	}
	if target.Path != "" && target.Path != "/" {
		return config{}, fmt.Errorf("-upstream path %q is not empty", target.Path)
	}
	target.Path = ""
	target.RawPath = ""
	cfg.upstream = target

	if cfg.tokenFile == "" {
		return config{}, errors.New("-token-file is required")
	}
	token, err := os.ReadFile(cfg.tokenFile)
	if err != nil {
		return config{}, fmt.Errorf("-token-file: %w", err)
	}
	if len(bytes.TrimSpace(token)) == 0 {
		return config{}, fmt.Errorf("-token-file %s is empty", cfg.tokenFile)
	}
	return cfg, nil
}

func parseLevel(name string) (slog.Level, error) {
	switch name {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	}
	return 0, fmt.Errorf("-log-level %q is not debug, info, warn or error", name)
}
