package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/evil8io/drover/internal/projectsync"
	"github.com/evil8io/drover/internal/telemetry"
)

type projectSyncConfig struct {
	listen             string
	rancherURL         *url.URL
	caFile             string
	insecureSkipVerify bool
	tokenFile          string
	labels             []string
	annotations        []string
	nameLabel          string
	nameAnnotation     string
	interval           time.Duration
	logLevel           slog.Level
	telemetry          telemetry.Config
}

func runProjectSync(args []string) int {
	cfg, err := parseProjectSyncConfig(args, os.Stderr, os.Getenv)
	if err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(os.Stderr, err)
		}
		return 2
	}

	logger := slog.New(telemetry.NewLogHandler(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.logLevel})))
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

	syncer, err := projectsync.New(projectsync.Config{
		RancherURL:         cfg.rancherURL,
		CAFile:             cfg.caFile,
		InsecureSkipVerify: cfg.insecureSkipVerify,
		TokenFile:          cfg.tokenFile,
		Labels:             cfg.labels,
		Annotations:        cfg.annotations,
		NameLabel:          cfg.nameLabel,
		NameAnnotation:     cfg.nameAnnotation,
		Interval:           cfg.interval,
		Logger:             logger,
		Version:            version,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}

	server := &http.Server{
		Addr:              cfg.listen,
		Handler:           syncer.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelDebug),
	}

	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() { serveErr <- server.ListenAndServe() }()

	logger.Info("start",
		"version", version,
		"listen", cfg.listen,
		"rancher", cfg.rancherURL.Redacted(),
		"insecure_skip_verify", cfg.insecureSkipVerify,
		"interval", cfg.interval.String(),
		"labels", cfg.labels,
		"annotations", cfg.annotations,
		"name_label", cfg.nameLabel,
		"name_annotation", cfg.nameAnnotation,
	)

	syncDone := make(chan struct{})
	go func() {
		defer close(syncDone)
		syncer.Run(signalCtx)
	}()

	select {
	case err := <-serveErr:
		stop()
		<-syncDone
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("the server stopped", "error", err.Error())
			return 1
		}
	case <-signalCtx.Done():
		stop()
		logger.Info("shutdown")
		<-syncDone
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Warn("the shutdown did not complete", "error", err.Error())
		}
	}
	return 0
}

func parseProjectSyncConfig(args []string, output io.Writer, getenv func(string) string) (projectSyncConfig, error) {
	flags := flag.NewFlagSet("drover project-sync", flag.ContinueOnError)
	flags.SetOutput(output)

	var (
		cfg            projectSyncConfig
		rancherURL     string
		labels         string
		annotations    string
		nameLabel      string
		nameAnnotation string
		logLevel       string
	)
	flags.StringVar(&cfg.listen, "listen", ":8080", "listen address")
	flags.StringVar(&rancherURL, "rancher-url", "", "Rancher URL, http:// or https://")
	flags.StringVar(&cfg.caFile, "rancher-ca-file", "", "PEM bundle that verifies an https Rancher URL")
	flags.BoolVar(&cfg.insecureSkipVerify, "rancher-insecure-skip-verify", false, "skip the certificate verification of an https Rancher URL")
	flags.StringVar(&cfg.tokenFile, "token-file", "", "file with the API token of the service user")
	flags.StringVar(&labels, "labels", "", "comma-separated label keys of a project to copy")
	flags.StringVar(&annotations, "annotations", "", "comma-separated annotation keys of a project to copy")
	flags.StringVar(&nameLabel, "name-label", "", "label key on the namespace that gets the display name of the project")
	flags.StringVar(&nameAnnotation, "name-annotation", "",
		"annotation key on the namespace that gets the display name of the project")
	flags.DurationVar(&cfg.interval, "interval", 60*time.Second, "time between two runs")
	flags.StringVar(&logLevel, "log-level", "info", "debug, info, warn or error")
	tf := registerTelemetryFlags(flags, getenv)

	if err := flags.Parse(args); err != nil {
		return projectSyncConfig{}, err
	}

	level, err := parseLevel(logLevel)
	if err != nil {
		return projectSyncConfig{}, err
	}
	cfg.logLevel = level
	cfg.telemetry = tf.config()

	target, err := parseRancherURL("-rancher-url", rancherURL)
	if err != nil {
		return projectSyncConfig{}, err
	}
	cfg.rancherURL = target

	if cfg.tokenFile == "" {
		return projectSyncConfig{}, errors.New("-token-file is required")
	}
	if cfg.interval <= 0 {
		return projectSyncConfig{}, fmt.Errorf("-interval %s is not positive", cfg.interval)
	}

	if cfg.labels, err = projectsync.ParseKeys(labels); err != nil {
		return projectSyncConfig{}, fmt.Errorf("-labels: %w", err)
	}
	if cfg.annotations, err = projectsync.ParseKeys(annotations); err != nil {
		return projectSyncConfig{}, fmt.Errorf("-annotations: %w", err)
	}
	if cfg.nameLabel, err = parseNameKey("name-label", nameLabel); err != nil {
		return projectSyncConfig{}, err
	}
	if cfg.nameAnnotation, err = parseNameKey("name-annotation", nameAnnotation); err != nil {
		return projectSyncConfig{}, err
	}
	if len(cfg.labels)+len(cfg.annotations) == 0 && cfg.nameLabel == "" && cfg.nameAnnotation == "" {
		return projectSyncConfig{}, errors.New("-labels, -annotations, -name-label or -name-annotation needs at least one key")
	}
	return cfg, nil
}

// parseNameKey validates the key of a -name-label or -name-annotation flag.
// name is the flag name, for the error message. An empty or blank value
// returns an empty key and no error.
func parseNameKey(name, value string) (string, error) {
	if strings.TrimSpace(value) == "" {
		return "", nil
	}
	keys, err := projectsync.ParseKeys(value)
	if err != nil {
		return "", fmt.Errorf("-%s: %w", name, err)
	}
	if len(keys) != 1 {
		return "", fmt.Errorf("-%s takes one key, not a list", name)
	}
	return keys[0], nil
}
