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

// telemetryShutdownGrace is the time that runAPIFilter gives Telemetry.Shutdown.
const telemetryShutdownGrace = 5 * time.Second

type config struct {
	listen             string
	upstream           *url.URL
	caFile             string
	insecureSkipVerify bool
	tokenFile          string
	cacheTTL           time.Duration
	maxCacheEntries    int
	fetchRate          float64
	fetchRatePerCaller float64
	fanout             bool
	fanoutMaxNS        int
	fanoutWorkers      int
	fanoutMaxInflight  int
	fanoutWatchMaxNS   int
	maxWatches         int
	callerMaxWatches   int
	logLevel           slog.Level
	shutdownGrace      time.Duration
	telemetry          telemetry.Config
}

func runAPIFilter(args []string) int {
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
	if cfg.insecureSkipVerify {
		logger.Warn("the certificate of Rancher is not verified")
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
		Upstream:           cfg.upstream,
		CAFile:             cfg.caFile,
		InsecureSkipVerify: cfg.insecureSkipVerify,
		TokenFile:          cfg.tokenFile,
		CacheTTL:           cfg.cacheTTL,
		MaxCacheEntries:    cfg.maxCacheEntries,
		FetchRate:          cfg.fetchRate,
		FetchRatePerCaller: cfg.fetchRatePerCaller,

		Fanout:                   cfg.fanout,
		FanoutMaxNamespaces:      cfg.fanoutMaxNS,
		FanoutConcurrency:        cfg.fanoutWorkers,
		FanoutMaxInflight:        cfg.fanoutMaxInflight,
		FanoutMaxWatchNamespaces: cfg.fanoutWatchMaxNS,
		MaxWatches:               cfg.maxWatches,
		MaxWatchesPerCaller:      cfg.callerMaxWatches,

		Logger: logger,
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
		"upstream", cfg.upstream.Redacted(),
		"insecure_skip_verify", cfg.insecureSkipVerify,
		"cache_ttl", cfg.cacheTTL.String(),
		"max_cache_entries", cfg.maxCacheEntries,
		"fetch_rate", cfg.fetchRate,
		"fetch_rate_per_caller", cfg.fetchRatePerCaller,
		"fanout", cfg.fanout,
		"fanout_max_namespaces", cfg.fanoutMaxNS,
		"fanout_concurrency", cfg.fanoutWorkers,
		"fanout_max_inflight", cfg.fanoutMaxInflight,
		"fanout_max_watch_namespaces", cfg.fanoutWatchMaxNS,
		"max_watches", cfg.maxWatches,
		"max_watches_per_caller", cfg.callerMaxWatches,
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
	flags := flag.NewFlagSet("drover api-filter", flag.ContinueOnError)
	flags.SetOutput(output)

	var (
		cfg      config
		upstream string
		logLevel string
	)
	flags.StringVar(&cfg.listen, "listen", ":8080", "listen address")
	flags.StringVar(&upstream, "upstream", "", "Rancher URL, http:// or https://")
	flags.StringVar(&cfg.caFile, "upstream-ca-file", "", "PEM bundle that verifies an https upstream")
	flags.BoolVar(&cfg.insecureSkipVerify, "upstream-insecure-skip-verify", false, "skip the certificate verification of an https upstream")
	flags.StringVar(&cfg.tokenFile, "token-file", "", "file with the API token of the service user")
	flags.DurationVar(&cfg.cacheTTL, "cache-ttl", 15*time.Second, "lifetime of a cached allowed set")
	flags.IntVar(&cfg.maxCacheEntries, "max-cache-entries", 1000, "hard bound on the cached allowed sets")
	flags.Float64Var(&cfg.fetchRate, "fetch-rate", 50, "fetches per second that the shared rate limit allows, for a fetch of an allowed set")
	flags.Float64Var(&cfg.fetchRatePerCaller, "fetch-rate-per-caller", 5, "fetches per second that the rate limit of one caller credential allows, for a fetch of an allowed set")
	flags.BoolVar(&cfg.fanout, "fanout", false, "answer a cluster-wide list of a namespaced kind with one request per allowed namespace")
	flags.IntVar(&cfg.fanoutMaxNS, "fanout-max-namespaces", 200, "count of allowed namespaces above which a fan-out answers 403")
	flags.IntVar(&cfg.fanoutWorkers, "fanout-concurrency", 16, "namespaced requests of one fan-out that run at a time")
	flags.IntVar(&cfg.fanoutMaxInflight, "fanout-max-inflight", 64, "namespaced requests of all fan-outs that run at a time")
	flags.IntVar(&cfg.fanoutWatchMaxNS, "fanout-max-watch-namespaces", 50, "count of allowed namespaces above which a cluster-wide watch answers 403")
	flags.IntVar(&cfg.maxWatches, "max-watches", 1000, "count of open watch streams above which a new watch answers 503")
	flags.IntVar(&cfg.callerMaxWatches, "max-watches-per-caller", 100, "count of open watch streams of one caller above which a new watch of that caller answers 503")
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
