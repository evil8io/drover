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

	"github.com/evil8io/drover/internal/filter"
	"github.com/evil8io/drover/internal/telemetry"
)

// telemetryShutdownGrace is the time that runNamespaceFilter gives Telemetry.Shutdown.
const telemetryShutdownGrace = 5 * time.Second

type config struct {
	listen        string
	upstream      *url.URL
	caFile        string
	tokenFile     string
	cacheTTL      time.Duration
	logLevel      slog.Level
	shutdownGrace time.Duration
	telemetry     telemetry.Config
}

func runNamespaceFilter(args []string) int {
	cfg, err := parseConfig(args, os.Stderr, os.Getenv)
	if err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(os.Stderr, err)
		}
		return 2
	}

	logger := slog.New(telemetry.NewLogHandler(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.logLevel})))
	if !tokenFileReady(cfg.tokenFile) {
		logger.Warn("the token file is not available yet", "path", cfg.tokenFile)
	}

	tel, err := telemetry.Setup(context.Background(), cfg.telemetry)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), telemetryShutdownGrace)
		defer cancel()
		if err := tel.Shutdown(shutdownCtx); err != nil {
			logger.Error("the telemetry shutdown failed", "error", err.Error())
		}
	}()

	svc, err := filter.New(filter.Config{
		Upstream:  cfg.upstream,
		CAFile:    cfg.caFile,
		TokenFile: cfg.tokenFile,
		CacheTTL:  cfg.cacheTTL,
		Logger:    logger,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}

	baseCtx, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()

	server := &http.Server{
		Addr:              cfg.listen,
		Handler:           svc,
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
		"shutdown_grace", cfg.shutdownGrace.String(),
	)

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("the server stopped", "error", err.Error())
			return 1
		}
	case <-signalCtx.Done():
		stop()
		ended := svc.StartDrain()
		logger.Info("drain", "streams_ended", ended)

		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.shutdownGrace)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("the server did not stop", "error", err.Error())
			return 1
		}
		logger.Info("stop")
	}
	return 0
}

func parseConfig(args []string, output io.Writer, getenv func(string) string) (config, error) {
	flags := flag.NewFlagSet("drover namespace-filter", flag.ContinueOnError)
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
	flags.DurationVar(&cfg.shutdownGrace, "shutdown-grace", 20*time.Second, "grace period for the shutdown after SIGTERM or SIGINT")
	tf := registerTelemetryFlags(flags, getenv)

	if err := flags.Parse(args); err != nil {
		return config{}, err
	}

	level, err := parseLevel(logLevel)
	if err != nil {
		return config{}, err
	}
	cfg.logLevel = level
	cfg.telemetry = tf.config()

	target, err := parseRancherURL("-upstream", upstream)
	if err != nil {
		return config{}, err
	}
	cfg.upstream = target

	if cfg.tokenFile == "" {
		return config{}, errors.New("-token-file is required")
	}
	return cfg, nil
}

// tokenFileReady reports whether path names a file with a non-blank token.
func tokenFileReady(path string) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	return len(bytes.TrimSpace(data)) > 0
}
