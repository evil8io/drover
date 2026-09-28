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
	serviceAccounts    bool
	openbao            *projectsync.OpenBaoConfig
	interval           time.Duration
	patchRate          float64
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
		ServiceAccounts:    cfg.serviceAccounts,
		OpenBao:            cfg.openbao,
		Interval:           cfg.interval,
		PatchRate:          cfg.patchRate,
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
		"patch_rate", cfg.patchRate,
		"labels", cfg.labels,
		"annotations", cfg.annotations,
		"name_label", cfg.nameLabel,
		"name_annotation", cfg.nameAnnotation,
		"service_accounts", cfg.serviceAccounts,
		"openbao_address", openbaoAddress(cfg.openbao),
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
		cfg               projectSyncConfig
		rancherURL        string
		labels            string
		annotations       string
		nameLabel         string
		nameAnnotation    string
		logLevel          string
		openbaoAddr       string
		openbaoRancherURL string
		openbao           projectsync.OpenBaoConfig
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
	flags.BoolVar(&cfg.serviceAccounts, "service-accounts", false,
		"keep one ServiceAccount per project role for every project, in an account project of each cluster")
	flags.StringVar(&openbaoAddr, "openbao-address", "",
		"OpenBao URL, http:// or https://, that gets the Kubernetes secrets engine config of every cluster; empty turns it off")
	flags.StringVar(&openbao.AuthPath, "openbao-auth-path", "kubernetes", "Kubernetes auth mount of OpenBao that the service logs in to")
	flags.StringVar(&openbao.Role, "openbao-role", "project-sync", "role of the OpenBao auth mount")
	flags.StringVar(&openbao.JWTFile, "openbao-jwt-file", "", "file with the ServiceAccount token of the pod, for the OpenBao login")
	flags.StringVar(&openbao.MountPrefix, "openbao-mount-prefix", "kubernetes",
		"path prefix of the secrets engine mounts; the config of a cluster is at <prefix>/<cluster id>/config")
	flags.StringVar(&openbaoRancherURL, "openbao-rancher-url", "", "Rancher URL that OpenBao uses, https:// only")
	flags.DurationVar(&openbao.TokenTTL, "openbao-token-ttl", 24*time.Hour,
		"requested lifetime of the cluster token that OpenBao gets; the API server can shorten it")
	flags.DurationVar(&openbao.CredentialTTL, "openbao-credential-ttl", 15*time.Minute,
		"default lifetime of a credential of a project role that OpenBao creates")
	flags.DurationVar(&openbao.CredentialMaxTTL, "openbao-credential-max-ttl", 2*time.Hour,
		"longest lifetime of a credential of a project role that OpenBao creates")
	flags.DurationVar(&cfg.interval, "interval", 60*time.Second, "time between two runs")
	flags.Float64Var(&cfg.patchRate, "patch-rate", 10,
		"namespace patches and project namespace lists per second that the watches of one cluster send, together")
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
	if len(cfg.labels)+len(cfg.annotations) == 0 && cfg.nameLabel == "" && cfg.nameAnnotation == "" && !cfg.serviceAccounts {
		return projectSyncConfig{}, errors.New("-labels, -annotations, -name-label or -name-annotation needs at least one key, or -service-accounts must be set")
	}
	if cfg.openbao, err = parseOpenBao(flags, cfg.serviceAccounts, openbaoAddr, openbaoRancherURL, openbao); err != nil {
		return projectSyncConfig{}, err
	}
	return cfg, nil
}

// parseOpenBao validates the OpenBao flags. It returns nil when address is
// empty, because that turns the OpenBao config off.
func parseOpenBao(flags *flag.FlagSet, serviceAccounts bool, address, rancherURL string, cfg projectsync.OpenBaoConfig) (*projectsync.OpenBaoConfig, error) {
	var set []string
	flags.Visit(func(f *flag.Flag) {
		if strings.HasPrefix(f.Name, "openbao-") {
			set = append(set, f.Name)
		}
	})
	if len(set) > 0 && !serviceAccounts {
		return nil, fmt.Errorf("-%s needs -service-accounts", set[0])
	}
	if address == "" {
		return nil, nil
	}

	var err error
	if cfg.Address, err = parseServiceURL("-openbao-address", address, false); err != nil {
		return nil, err
	}
	if rancherURL == "" {
		return nil, errors.New("-openbao-rancher-url is required, because -openbao-address is set")
	}
	if cfg.RancherURL, err = parseServiceURL("-openbao-rancher-url", rancherURL, true); err != nil {
		return nil, err
	}
	if cfg.JWTFile == "" {
		return nil, errors.New("-openbao-jwt-file is required, because -openbao-address is set")
	}
	for _, item := range []struct{ name, value string }{
		{"-openbao-auth-path", cfg.AuthPath},
		{"-openbao-role", cfg.Role},
		{"-openbao-mount-prefix", cfg.MountPrefix},
	} {
		if strings.Trim(item.value, "/") == "" {
			return nil, fmt.Errorf("%s is empty", item.name)
		}
	}
	if cfg.TokenTTL < 10*time.Minute {
		return nil, fmt.Errorf("-openbao-token-ttl %s is shorter than 10m0s, the minimum of a token request", cfg.TokenTTL)
	}
	if cfg.CredentialTTL < time.Second {
		return nil, fmt.Errorf("-openbao-credential-ttl %s is shorter than 1s", cfg.CredentialTTL)
	}
	if cfg.CredentialMaxTTL < cfg.CredentialTTL {
		return nil, fmt.Errorf("-openbao-credential-max-ttl %s is shorter than -openbao-credential-ttl %s", cfg.CredentialMaxTTL, cfg.CredentialTTL)
	}
	return &cfg, nil
}

// openbaoAddress returns the OpenBao address for the start line, or "" when
// the OpenBao config is off.
func openbaoAddress(cfg *projectsync.OpenBaoConfig) string {
	if cfg == nil {
		return ""
	}
	return cfg.Address.Redacted()
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
