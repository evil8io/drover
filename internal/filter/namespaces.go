package filter

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/evil8io/drover/internal/rancherclient"
)

const (
	nameLabel    = "kubernetes.io/metadata.name"
	maxDrainBody = 1 << 20
)

func (s *Service) roundTripNamespaces(req *http.Request, cluster string) (*http.Response, error) {
	start := s.now()
	watch, _ := strconv.ParseBool(req.URL.Query().Get("watch"))
	result := listResult{cluster: cluster, watch: watch}

	if hasImpersonation(req.Header) {
		resp, err := s.base.RoundTrip(req)
		if err != nil {
			return nil, s.listError(req, start, result, err)
		}
		result.outcome, result.status = outcomePassthrough, resp.StatusCode
		s.logList(req.Context(), start, result)
		return resp, nil
	}

	resp, err := s.base.RoundTrip(req)
	if err != nil {
		return nil, s.listError(req, start, result, err)
	}
	if resp.StatusCode != http.StatusForbidden {
		result.outcome, result.status = outcomeNative, resp.StatusCode
		s.logList(req.Context(), start, result)
		return resp, nil
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrainBody))
	_ = resp.Body.Close()

	names, denied, err := s.allowedNamespaces(req.Context(), cluster, req.Header)
	switch {
	case denied != nil:
		result.outcome, result.status = outcomeDenied, denied.StatusCode
		s.logList(req.Context(), start, result)
		return denied, nil
	case err != nil:
		return s.statusError(req, start, result, err), nil
	}
	s.logger.DebugContext(req.Context(), "allowed namespaces", "cluster", cluster, "names", names)

	token, err := rancherclient.ReadToken(s.tokenFile)
	if err != nil {
		return s.statusError(req, start, result, err), nil
	}

	privileged := req.Clone(req.Context())
	privileged.Header.Set("Authorization", "Bearer "+token)
	privileged.Header.Del("Cookie")

	if watch {
		privileged.Header.Set("Accept", filterJSONAccept(req.Header.Get("Accept")))
	} else {
		query := privileged.URL.Query()
		query.Set("labelSelector", mergeSelector(query.Get("labelSelector"), names))
		privileged.URL.RawQuery = query.Encode()
	}

	filtered, err := s.base.RoundTrip(privileged)
	if err != nil {
		return nil, s.listError(req, start, result, err)
	}
	if filtered.StatusCode == http.StatusForbidden {
		s.logger.WarnContext(req.Context(), "the service token has no cluster-owner binding", "cluster", cluster)
	}
	// A plain watch stream is the only response the event filter reads. An
	// upgraded connection has no JSON body, and an error status has no events.
	if watch && filtered.StatusCode == http.StatusOK {
		filtered.Body = filterWatchBody(req.Context(), filtered.Body, func(name string, labels map[string]string) bool {
			return s.allowedNamespace(req.Context(), cluster, req.Header, name, labels)
		}, s.logger, s.watches)
		// A dropped event changes the byte count, so the length of upstream no
		// longer applies.
		filtered.ContentLength = -1
		filtered.Header.Del("Content-Length")
	}
	result.outcome, result.status, result.count = outcomeFiltered, filtered.StatusCode, len(names)
	s.logList(req.Context(), start, result)
	return filtered, nil
}

// filterJSONAccept keeps every entry of the Accept header of the caller whose
// media type is application/json, and drops every other entry, for example a
// protobuf or a CBOR entry that the stream filter cannot read. It sets
// application/json when no entry remains.
func filterJSONAccept(accept string) string {
	var kept []string
	for _, entry := range strings.Split(accept, ",") {
		entry = strings.TrimSpace(entry)
		mediaType := entry
		if i := strings.IndexByte(entry, ';'); i >= 0 {
			mediaType = entry[:i]
		}
		if strings.EqualFold(strings.TrimSpace(mediaType), jsonContentType) {
			kept = append(kept, entry)
		}
	}
	if len(kept) == 0 {
		return jsonContentType
	}
	return strings.Join(kept, ",")
}

// mergeSelector appends the name requirement to the selector of the caller. An
// empty set gives a selector that matches nothing.
func mergeSelector(caller string, names []string) string {
	requirement := nameLabel + ",!" + nameLabel
	if len(names) > 0 {
		requirement = nameLabel + " in (" + strings.Join(names, ",") + ")"
	}
	if caller == "" {
		return requirement
	}
	return caller + "," + requirement
}

type listResult struct {
	cluster string
	outcome string
	status  int
	count   int
	watch   bool
	err     error
}

// listError logs the failed request. The caller returns the error, and the
// error handler of the proxy writes the Status body.
func (s *Service) listError(req *http.Request, start time.Time, result listResult, err error) error {
	result.outcome, result.err = outcomeError, err
	s.logList(req.Context(), start, result)
	return err
}

func (s *Service) statusError(req *http.Request, start time.Time, result listResult, err error) *http.Response {
	result.outcome, result.status, result.err = outcomeError, http.StatusBadGateway, err
	s.logList(req.Context(), start, result)
	return statusResponse(req, http.StatusBadGateway, reasonInternalError, serviceName+": "+err.Error())
}

func (s *Service) logList(ctx context.Context, start time.Time, result listResult) {
	attrs := []any{"cluster", result.cluster, "outcome", result.outcome, "status", result.status}
	if result.outcome == outcomeFiltered {
		attrs = append(attrs, "count", result.count)
	}
	attrs = append(attrs, "watch", result.watch, "duration_ms", s.now().Sub(start).Milliseconds())

	level := slog.LevelInfo
	if result.err != nil {
		attrs = append(attrs, "error", result.err.Error())
		switch {
		case errors.Is(result.err, context.Canceled):
			level = slog.LevelDebug
		case errors.Is(result.err, rancherclient.ErrTokenUnavailable):
			level = slog.LevelWarn
		default:
			level = slog.LevelError
		}
	}
	s.logger.Log(ctx, level, "namespaces", attrs...)
}
