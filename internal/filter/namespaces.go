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

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/evil8io/drover/internal/rancherclient"
)

const (
	nameLabel    = "kubernetes.io/metadata.name"
	maxDrainBody = 1 << 20
)

func (s *Service) roundTripNamespaces(req *http.Request, cluster string) (*http.Response, error) {
	start := s.now()
	watch := watchRequested(req.URL.Query())
	result := listResult{cluster: cluster, path: pathNamespaces, watch: watch}

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

	set, denied, err := s.allowed(req.Context(), cluster, req.Header)
	switch {
	case denied != nil:
		result.outcome, result.status = outcomeDenied, denied.StatusCode
		s.logList(req.Context(), start, result)
		return denied, nil
	case err != nil:
		return s.statusError(req, start, result, err), nil
	}
	result.user = set.user
	s.logger.DebugContext(req.Context(), "allowed namespaces", "cluster", cluster, "names", set.names)

	if watch && s.watches.len() >= s.maxWatches {
		return s.tooManyWatches(req, start, result), nil
	}

	token, err := rancherclient.ReadToken(s.tokenFile)
	if err != nil {
		return s.statusError(req, start, result, err), nil
	}

	privileged := req.Clone(req.Context())
	privileged.Header.Set("Authorization", "Bearer "+token)
	privileged.Header.Del("Cookie")
	privileged.Header.Del("Accept-Encoding")
	// The filter reads no compressed payload. Rancher then negotiates no
	// extension, and the client gets that same answer.
	privileged.Header.Del(extensionsHeader)

	selected := false
	var callerSelector, watchLabelSelector string
	if watch {
		privileged.Header.Set("Accept", filterJSONAccept(req.Header.Get("Accept")))
		query := privileged.URL.Query()
		callerSelector = query.Get("labelSelector")
		if selector, ok := watchSelector(callerSelector, set); ok {
			query.Set("labelSelector", selector)
			privileged.URL.RawQuery = query.Encode()
			selected, watchLabelSelector = true, selector
		}
	} else {
		query := privileged.URL.Query()
		query.Set("labelSelector", namespaceSelector(query.Get("labelSelector"), set))
		privileged.URL.RawQuery = query.Encode()
	}

	filtered, err := s.base.RoundTrip(privileged)
	if err != nil {
		return nil, s.listError(req, start, result, err)
	}
	if filtered.StatusCode == http.StatusForbidden {
		s.logger.WarnContext(req.Context(), "the service token has no cluster-owner binding", "cluster", cluster)
	}
	// A watch answer has the events in a chunked body, or in the websocket
	// frames of an upgraded connection. An error status has no events.
	if watch {
		allow := func(name string, labels map[string]string) bool {
			return s.allowedNamespace(req.Context(), cluster, req.Header, name, labels)
		}
		var writer *io.PipeWriter
		switch filtered.StatusCode {
		case http.StatusOK:
			filtered.Body, writer = filterWatchBody(req.Context(), filtered.Body, allow, s.logger, s.watches, s.metrics, cluster)
			// A dropped event changes the byte count, so the length of upstream
			// no longer applies, and the filter writes plain JSON.
			filtered.ContentLength = -1
			filtered.Header.Del("Content-Length")
			filtered.Header.Del("Content-Encoding")
		case http.StatusSwitchingProtocols:
			writer, err = filterWatchUpgrade(req.Context(), filtered, allow, s.logger, s.watches, s.metrics, cluster)
			if err != nil {
				_ = filtered.Body.Close()
				return s.statusError(req, start, result, err), nil
			}
		}
		if selected && writer != nil {
			go s.endOnSelectorChange(req.Context(), cluster, req.Header, callerSelector, watchLabelSelector, writer)
		}
	}
	result.outcome, result.status, result.count = outcomeFiltered, filtered.StatusCode, len(set.names)
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

// namespaceSelector picks the label selector for the privileged list.
// A namespace with the project label of a project the caller may see is a
// namespace the caller may list. The watch path applies that same rule per
// event. The project selector is therefore equivalent to the name selector
// when set.extras is empty. Unlike the name selector, it also stays the same
// across the pages of one chunked list.
func namespaceSelector(caller string, set allowedSet) string {
	if len(set.projects) > 0 && len(set.extras) == 0 {
		return mergeProjectSelector(caller, set.projects)
	}
	return mergeSelector(caller, set.names)
}

// watchSelector picks the label selector for the privileged watch, and
// reports whether the watch gets one. The selector of a watch does not change
// while the stream runs, so a name selector hides a namespace that Rancher
// puts in a project of the caller later. A project selector has no such gap, because a
// new namespace of a project matches by itself. A caller with a namespace
// outside its own projects gets no selector, because only the event filter
// separates that case.
func watchSelector(caller string, set allowedSet) (string, bool) {
	switch {
	case len(set.names) == 0 && len(set.projects) == 0:
		return mergeSelector(caller, nil), true
	case len(set.projects) > 0 && len(set.extras) == 0:
		return mergeProjectSelector(caller, set.projects), true
	default:
		return "", false
	}
}

// endOnSelectorChange ends the watch stream of writer once the selector that
// the allowed set of the caller gives differs from selector. A watch with a
// selector gets no event outside that selector, so the event filter cannot
// show a namespace that Rancher grants later. The client re-lists and
// re-watches after the end of the stream, and the new watch gets the selector
// of the new allowed set. The re-read of the allowed set goes through the
// cache of a plain list, so it adds no request beyond the one fetch per cache
// TTL.
func (s *Service) endOnSelectorChange(ctx context.Context, cluster string, header http.Header, caller, selector string, writer *io.PipeWriter) {
	done := s.watches.done(writer)
	ticker := time.NewTicker(s.cache.ttl)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
		}

		set, denied, err := s.allowed(ctx, cluster, header)
		if denied != nil {
			_ = denied.Body.Close()
		}
		if denied != nil || err != nil {
			continue
		}
		if current, ok := watchSelector(caller, set); ok && current == selector {
			continue
		}
		s.logger.InfoContext(ctx, "ended a namespace watch, because the selector of the caller changed", "cluster", cluster)
		s.watches.end(writer)
		return
	}
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

// mergeProjectSelector appends the project requirement to the selector of the
// caller.
func mergeProjectSelector(caller string, projects []string) string {
	requirement := projectLabel + " in (" + strings.Join(projects, ",") + ")"
	if caller == "" {
		return requirement
	}
	return caller + "," + requirement
}

type listResult struct {
	cluster  string
	path     string
	resource string
	outcome  string
	status   int
	count    int
	watch    bool
	user     string
	err      error
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
	return statusResponse(req, http.StatusBadGateway, reasonInternalError, serviceName+": "+publicMessage(err))
}

// tooManyWatches answers 503, because the open watch streams of the service
// are at the bound. The client retries, and a stream that ended in between
// frees a slot.
func (s *Service) tooManyWatches(req *http.Request, start time.Time, result listResult) *http.Response {
	result.outcome, result.status = outcomeCapped, http.StatusServiceUnavailable
	s.logList(req.Context(), start, result)
	message := serviceName + ": the open watch streams are at the limit of " + strconv.Itoa(s.maxWatches)
	return statusResponse(req, http.StatusServiceUnavailable, reasonUnavailable, message)
}

func (s *Service) logList(ctx context.Context, start time.Time, result listResult) {
	duration := s.now().Sub(start)
	s.metrics.recordRequest(ctx, result, s.clusters.attribute(result.cluster), duration)

	attrs := []any{"cluster", result.cluster, "outcome", result.outcome, "status", result.status}
	if result.resource != "" {
		attrs = append(attrs, "resource", result.resource)
	}
	if result.outcome == outcomeFiltered || result.outcome == outcomeFannedOut || result.outcome == outcomeCapped || result.outcome == outcomeEmpty {
		attrs = append(attrs, "count", result.count)
	}
	if result.user != "" {
		attrs = append(attrs, "user", result.user)
	}
	attrs = append(attrs, "watch", result.watch, "duration_ms", duration.Milliseconds())

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
	s.logger.Log(ctx, level, result.path, attrs...)
	setCallerAttribute(ctx, result.user)
}

// setCallerAttribute sets drover.user on the span of ctx, when user is not
// empty. A metric never has this attribute. A user name has an unbounded
// value set, and a metric attribute with that shape is a cardinality fault.
func setCallerAttribute(ctx context.Context, user string) {
	if user == "" {
		return
	}
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("drover.user", user))
}
