package rotate

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"
)

// NewClient returns a client for the Rancher API or the Kubernetes API. An empty
// caFile selects the system pool. insecureSkipVerify skips the certificate
// verification of an https Rancher URL.
func NewClient(caFile string, insecureSkipVerify bool) (*http.Client, error) {
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSClientConfig:     &tls.Config{MinVersion: tls.VersionTLS12},
		TLSHandshakeTimeout: 10 * time.Second,
		IdleConnTimeout:     30 * time.Second,
	}
	if insecureSkipVerify {
		transport.TLSClientConfig.InsecureSkipVerify = true
	}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read the CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("the CA file %s has no certificate", caFile)
		}
		transport.TLSClientConfig.RootCAs = pool
	}
	return &http.Client{Transport: transport, Timeout: 30 * time.Second, CheckRedirect: noRedirect}, nil
}

// noRedirect keeps every redirect answer as a normal response. A followed 307
// or 308 answer resends the login body, with the password, to the redirect
// target.
func noRedirect(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}
