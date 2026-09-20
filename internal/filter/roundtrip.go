package filter

import (
	"bytes"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
)

const (
	outcomeNative      = "native"
	outcomeFiltered    = "filtered"
	outcomePassthrough = "passthrough"
	outcomeDenied      = "denied"
	outcomeFannedOut   = "fanout"
	outcomeCapped      = "capped"
	outcomeError       = "error"

	// pathNamespaces and pathCollection name the two request shapes that the
	// filter answers. Each is the message of the log line, and the path
	// attribute of the request metrics.
	pathNamespaces = "namespaces"
	pathCollection = "collection"

	impersonatePrefix = "impersonate-"
)

var (
	namespacesPath = regexp.MustCompile(`^/k8s/clusters/([^/]+)/api/v1/namespaces/?$`)
	reviewsPath    = regexp.MustCompile(`^/k8s/clusters/([^/]+)/apis/authorization\.k8s\.io/v1/selfsubjectaccessreviews/?$`)
	// A core collection has one segment after the version, and a group
	// collection has one after the group and the version. A path with more
	// segments names a namespaced collection, a single object, or a
	// subresource, and a path with fewer names discovery. Neither is a
	// cluster-wide collection, so both pass through.
	coreCollectionPath  = regexp.MustCompile(`^/k8s/clusters/([^/]+)/api/v1/([^/]+)/?$`)
	groupCollectionPath = regexp.MustCompile(`^/k8s/clusters/([^/]+)/apis/([^/]+)/([^/]+)/([^/]+)/?$`)
)

func (s *Service) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodGet {
		if m := namespacesPath.FindStringSubmatch(req.URL.Path); m != nil {
			return s.roundTripNamespaces(req, m[1])
		}
		if target, ok := s.collectionTarget(req.URL.Path); ok {
			return s.roundTripCollection(req, target)
		}
	}
	if req.Method == http.MethodPost {
		if m := reviewsPath.FindStringSubmatch(req.URL.Path); m != nil {
			return s.roundTripReview(req, m[1])
		}
	}
	return s.base.RoundTrip(req)
}

// collectionTarget reports whether path names a cluster-wide collection that
// the fan-out answers.
func (s *Service) collectionTarget(path string) (collectionTarget, bool) {
	if !s.fanoutEnabled {
		return collectionTarget{}, false
	}
	if m := coreCollectionPath.FindStringSubmatch(path); m != nil {
		return collectionTarget{
			cluster:  m[1],
			apiPath:  "/k8s/clusters/" + m[1] + "/api/v1",
			resource: m[2],
		}, true
	}
	if m := groupCollectionPath.FindStringSubmatch(path); m != nil {
		return collectionTarget{
			cluster:  m[1],
			apiPath:  "/k8s/clusters/" + m[1] + "/apis/" + m[2] + "/" + m[3],
			resource: m[4],
		}, true
	}
	return collectionTarget{}, false
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

// setResponseBody replaces the body of resp with data, and makes the length
// headers agree with it.
func setResponseBody(resp *http.Response, data []byte) {
	resp.Body = io.NopCloser(bytes.NewReader(data))
	resp.ContentLength = int64(len(data))
	resp.TransferEncoding = nil
	if resp.Header.Get("Content-Length") != "" {
		resp.Header.Set("Content-Length", strconv.Itoa(len(data)))
	}
}

type joinedBody struct {
	io.Reader
	io.Closer
}
