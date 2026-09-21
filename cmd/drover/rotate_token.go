package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/evil8io/drover/internal/rotate"
	"github.com/evil8io/drover/internal/telemetry"
)

const (
	defaultServiceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"
	rotateTimeout            = 2 * time.Minute
)

type rotateConfig struct {
	rancher                   *url.URL
	rancherCAFile             string
	rancherInsecureSkipVerify bool
	kube                      *url.URL
	kubeTokenFile             string
	kubeCAFile                string
	namespace                 string
	secret                    string
	key                       string
	username                  string
	password                  string
	ttl                       time.Duration
	renewBefore               time.Duration
	keep                      int
	description               string
	logLevel                  slog.Level
	telemetry                 telemetry.Config
}

func runRotateToken(args []string) int {
	cfg, err := parseRotateConfig(args, os.Stderr, os.Getenv)
	if err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(os.Stderr, err)
		}
		return 2
	}

	logger := slog.New(telemetry.NewLogHandler(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.logLevel})))
	if cfg.rancherInsecureSkipVerify {
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
			logger.Warn("the telemetry shutdown failed", "error", err.Error())
		}
	}()

	rancherClient, err := rotate.NewClient(cfg.rancherCAFile, cfg.rancherInsecureSkipVerify)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	kubeClient, err := rotate.NewClient(cfg.kubeCAFile, false)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}

	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(signalCtx, rotateTimeout)
	defer cancel()

	err = rotate.Run(ctx, rotate.Config{
		Rancher:       cfg.rancher,
		RancherClient: rancherClient,
		Kube:          cfg.kube,
		KubeClient:    kubeClient,
		KubeTokenFile: cfg.kubeTokenFile,
		Namespace:     cfg.namespace,
		Secret:        cfg.secret,
		Key:           cfg.key,
		Username:      cfg.username,
		Password:      cfg.password,
		TTL:           cfg.ttl,
		RenewBefore:   cfg.renewBefore,
		Keep:          cfg.keep,
		Description:   cfg.description,
		UserAgent:     "drover/" + version,
		Logger:        logger,
	})
	if err != nil {
		logger.Error("the rotation failed", "error", err.Error())
		return 1
	}
	return 0
}

func parseRotateConfig(args []string, output io.Writer, getenv func(string) string) (rotateConfig, error) {
	flags := flag.NewFlagSet("drover rotate-token", flag.ContinueOnError)
	flags.SetOutput(output)

	var (
		cfg            rotateConfig
		rancherURL     string
		kubeURL        string
		credentialsDir string
		tokenSecret    string
		serviceAccount string
		logLevel       string
	)
	flags.StringVar(&rancherURL, "rancher-url", "", "Rancher URL, http:// or https://")
	flags.StringVar(&cfg.rancherCAFile, "rancher-ca-file", "", "PEM bundle that verifies an https Rancher URL")
	flags.BoolVar(&cfg.rancherInsecureSkipVerify, "rancher-insecure-skip-verify", false, "skip the certificate verification of an https Rancher URL")
	flags.StringVar(&credentialsDir, "credentials-dir", "", "directory with the files username and password")
	flags.StringVar(&tokenSecret, "token-secret", "", "namespace/name of the Secret with the API token")
	flags.StringVar(&cfg.key, "token-key", "token", "key of the token inside the Secret")
	flags.DurationVar(&cfg.ttl, "ttl", 48*time.Hour, "lifetime of a new token")
	flags.DurationVar(&cfg.renewBefore, "renew-before", 24*time.Hour, "remaining lifetime that starts a rotation")
	flags.IntVar(&cfg.keep, "keep", 2, "number of tokens to keep, the new token included, at least 2")
	flags.StringVar(&cfg.description, "description", "drover rotate-token", "description of the tokens of this command")
	flags.StringVar(&kubeURL, "kube-url", "", "Kubernetes API URL, default from the in-cluster environment")
	flags.StringVar(&serviceAccount, "kube-service-account-dir", defaultServiceAccountDir,
		"directory with the ServiceAccount token and ca.crt")
	flags.StringVar(&logLevel, "log-level", "info", "debug, info, warn or error")
	tf := registerTelemetryFlags(flags, getenv)

	if err := flags.Parse(args); err != nil {
		return rotateConfig{}, err
	}

	level, err := parseLevel(logLevel)
	if err != nil {
		return rotateConfig{}, err
	}
	cfg.logLevel = level
	cfg.telemetry = tf.config()

	if rancherURL == "" {
		return rotateConfig{}, errors.New("-rancher-url is required")
	}
	if cfg.rancher, err = parseServiceURL("-rancher-url", rancherURL); err != nil {
		return rotateConfig{}, err
	}

	if kubeURL == "" {
		host, port := getenv("KUBERNETES_SERVICE_HOST"), getenv("KUBERNETES_SERVICE_PORT")
		if host == "" || port == "" {
			return rotateConfig{}, errors.New("-kube-url is required, because the in-cluster environment is absent")
		}
		kubeURL = "https://" + net.JoinHostPort(host, port)
	}
	if cfg.kube, err = parseServiceURL("-kube-url", kubeURL); err != nil {
		return rotateConfig{}, err
	}

	if serviceAccount == "" {
		return rotateConfig{}, errors.New("-kube-service-account-dir is required")
	}
	cfg.kubeTokenFile = filepath.Join(serviceAccount, "token")
	if ca := filepath.Join(serviceAccount, "ca.crt"); fileExists(ca) {
		cfg.kubeCAFile = ca
	}

	if cfg.namespace, cfg.secret, err = parseSecretRef(tokenSecret); err != nil {
		return rotateConfig{}, err
	}
	if cfg.key == "" {
		return rotateConfig{}, errors.New("-token-key is required")
	}

	if credentialsDir == "" {
		return rotateConfig{}, errors.New("-credentials-dir is required")
	}
	if cfg.username, err = readCredential(credentialsDir, "username"); err != nil {
		return rotateConfig{}, err
	}
	if cfg.password, err = readCredential(credentialsDir, "password"); err != nil {
		return rotateConfig{}, err
	}

	if cfg.ttl <= 0 {
		return rotateConfig{}, errors.New("-ttl must be longer than zero")
	}
	if cfg.renewBefore <= 0 {
		return rotateConfig{}, errors.New("-renew-before must be longer than zero")
	}
	if cfg.renewBefore >= cfg.ttl {
		return rotateConfig{}, fmt.Errorf("-renew-before %s is not shorter than -ttl %s, so every run rotates",
			cfg.renewBefore, cfg.ttl)
	}
	if cfg.keep < 1 {
		return rotateConfig{}, errors.New("-keep must be 1 or more, because the new token counts")
	}
	if cfg.description == "" {
		return rotateConfig{}, errors.New("-description is required, because it selects the tokens to delete")
	}
	return cfg, nil
}

func parseServiceURL(name, raw string) (*url.URL, error) {
	target, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	if target.Scheme != "http" && target.Scheme != "https" {
		return nil, fmt.Errorf("%s scheme %q is not http or https", name, target.Scheme)
	}
	if target.Host == "" {
		return nil, fmt.Errorf("%s has no host", name)
	}
	if target.Path != "" && target.Path != "/" {
		return nil, fmt.Errorf("%s path %q is not empty", name, target.Path)
	}
	target.Path = ""
	target.RawPath = ""
	return target, nil
}

func parseSecretRef(raw string) (namespace, name string, err error) {
	if raw == "" {
		return "", "", errors.New("-token-secret is required")
	}
	namespace, name, found := strings.Cut(raw, "/")
	if !found || namespace == "" || name == "" || strings.Contains(name, "/") {
		return "", "", fmt.Errorf("-token-secret %q is not namespace/name", raw)
	}
	return namespace, name, nil
}

func readCredential(dir, name string) (string, error) {
	path := filepath.Join(dir, name)
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("-credentials-dir: %w", err)
	}
	value := strings.TrimSpace(string(data))
	if value == "" {
		return "", fmt.Errorf("-credentials-dir: the file %s is empty", path)
	}
	return value, nil
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}
