package main

import (
	"fmt"
	"log/slog"
	"net/url"
)

// parseRancherURL returns the URL of a Rancher server. The name is the flag name,
// for the error message.
func parseRancherURL(name, raw string) (*url.URL, error) {
	if raw == "" {
		return nil, fmt.Errorf("%s is required", name)
	}
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
