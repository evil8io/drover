// Package rancherclient has the HTTP parts that every subcommand shares: the
// transport with its certificate pool, and the token file of the service user.
package rancherclient

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

// Transport returns the transport of a Rancher client. An empty caFile selects
// the system certificate pool.
func Transport(caFile string) (*http.Transport, error) {
	tr := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		DisableCompression:  true,
		ForceAttemptHTTP2:   false,
		TLSNextProto:        map[string]func(string, *tls.Conn) http.RoundTripper{},
		TLSHandshakeTimeout: 10 * time.Second,
		IdleConnTimeout:     90 * time.Second,
		MaxIdleConnsPerHost: 100,
	}
	if caFile == "" {
		return tr, nil
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read the CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("the CA file %s has no certificate", caFile)
	}
	tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	return tr, nil
}

// ErrTokenUnavailable marks a ReadToken failure that a later read recovers
// from, once the token file gets its content.
var ErrTokenUnavailable = errors.New("the token file is not available yet")

// ReadToken returns the token in the file at path, without the space around it.
func ReadToken(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrTokenUnavailable, err)
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", fmt.Errorf("%w: %s is empty", ErrTokenUnavailable, path)
	}
	return token, nil
}
