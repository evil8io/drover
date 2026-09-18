package filter

import (
	"bytes"
	"io"
	"net/http"
	"regexp"
	"strings"
)

const (
	outcomeNative      = "native"
	outcomeFiltered    = "filtered"
	outcomePassthrough = "passthrough"
	outcomeDenied      = "denied"
	outcomeError       = "error"

	impersonatePrefix = "impersonate-"
)

var (
	namespacesPath = regexp.MustCompile(`^/k8s/clusters/([^/]+)/api/v1/namespaces/?$`)
	reviewsPath    = regexp.MustCompile(`^/k8s/clusters/([^/]+)/apis/authorization\.k8s\.io/v1/selfsubjectaccessreviews/?$`)
)

func (s *Service) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodGet {
		if m := namespacesPath.FindStringSubmatch(req.URL.Path); m != nil {
			return s.roundTripNamespaces(req, m[1])
		}
	}
	if req.Method == http.MethodPost {
		if m := reviewsPath.FindStringSubmatch(req.URL.Path); m != nil {
			return s.roundTripReview(req, m[1])
		}
	}
	return s.base.RoundTrip(req)
}

func hasImpersonation(header http.Header) bool {
	for name := range header {
		if len(name) >= len(impersonatePrefix) && strings.EqualFold(name[:len(impersonatePrefix)], impersonatePrefix) {
			return true
		}
	}
	return false
}

// readLimited reads at most limit bytes. It reports whether the reader has more.
func readLimited(r io.Reader, limit int64) (data []byte, tooLarge bool, err error) {
	if r == nil {
		return nil, false, nil
	}
	data, err = io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(data)) > limit {
		return data, true, nil
	}
	return data, false, nil
}

func setBody(req *http.Request, body []byte) {
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	req.TransferEncoding = nil
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}
}

type joinedBody struct {
	io.Reader
	io.Closer
}
