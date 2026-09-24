package filter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
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

	var slot *watchSlot
	handedOver := false
	if watch {
		var limit string
		if slot, limit = s.watches.reserve(watchCaller(req.Header)); slot == nil {
			return s.tooManyWatches(req, start, result, limit), nil
		}
		defer func() {
			if !handedOver {
				s.watches.release(slot)
			}
		}()
	}

	token, err := rancherclient.ReadToken(s.tokenFile)
	if err != nil {
		return s.statusError(req, start, result, err), nil
	}

	privileged := privilegedRequest(req, token)

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
		s.logger.WarnContext(req.Context(), "the service token may not list namespaces", "cluster", cluster)
	}
	// A watch answer has the events in a chunked body, or in the websocket
	// frames of an upgraded connection. An error status has no events.
	if watch {
		allow := func(name string, labels map[string]string) bool {
			return s.allowedNamespace(req.Context(), cluster, req.Header, name, labels)
		}
		var relay *watchRelay
		switch filtered.StatusCode {
		case http.StatusOK:
			filtered.Body, relay = filterWatchBody(req.Context(), filtered.Body, allow, s.logger, s.watches, slot, s.metrics, cluster)
			handedOver = true
			// A dropped event changes the byte count, so the length of upstream
			// no longer applies, and the filter writes plain JSON.
			filtered.ContentLength = -1
			filtered.Header.Del("Content-Length")
			filtered.Header.Del("Content-Encoding")
		case http.StatusSwitchingProtocols:
			relay, err = filterWatchUpgrade(req.Context(), filtered, allow, s.logger, s.watches, slot, s.metrics, cluster)
			if err != nil {
				_ = filtered.Body.Close()
				return s.statusError(req, start, result, err), nil
			}
			handedOver = true
		}
		if relay != nil {
			go s.trackNamespaceWatch(req.Context(), newNamespaceWatch(req, cluster, set, callerSelector, watchLabelSelector, selected, relay))
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
// A namespace with the project label of a project in set.projects is a
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
// reports whether the watch gets one. A project selector matches a new
// namespace of a project by itself, so the stream needs no swap of its
// upstream for it. A name selector needs a swap for every new namespace. A
// caller with a namespace outside its own projects gets no selector, because
// only the event filter separates that case.
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

// namespaceWatch is the state of one namespace watch that
// trackNamespaceWatch keeps. selector and selected are the case of the current
// upstream watch, as watchSelector gives it. names is the allowed set that the
// stream of the client follows.
type namespaceWatch struct {
	req     *http.Request
	cluster string
	relay   *watchRelay
	caller  string

	selector string
	selected bool
	names    []string

	// synthesize is false for a stream that takes no event of the service.
	// An Accept with an as parameter asks for another form of the object,
	// for example a table that needs the columns of the stream. The service
	// also does not apply a label or a field selector of the caller to its
	// own events.
	synthesize bool
}

func newNamespaceWatch(req *http.Request, cluster string, set allowedSet, caller, selector string, selected bool, relay *watchRelay) *namespaceWatch {
	return &namespaceWatch{
		req:      req.Clone(req.Context()),
		cluster:  cluster,
		relay:    relay,
		caller:   caller,
		selector: selector,
		selected: selected,
		names:    slices.Clone(set.names),
		synthesize: !transformsObject(filterJSONAccept(req.Header.Get("Accept"))) &&
			caller == "" && req.URL.Query().Get("fieldSelector") == "",
	}
}

// transformsObject reports whether an entry of accept asks for another form
// of the object with an as parameter, for example as=Table for a server-side
// table.
func transformsObject(accept string) bool {
	for _, entry := range strings.Split(accept, ",") {
		params := strings.Split(entry, ";")
		for _, param := range params[1:] {
			key, _, _ := strings.Cut(param, "=")
			if strings.EqualFold(strings.TrimSpace(key), "as") {
				return true
			}
		}
	}
	return false
}

// trackNamespaceWatch keeps the stream of w open across a change of the
// allowed set of the caller. It re-reads the allowed set once per cache TTL,
// through the cache of a plain list, so it adds no request beyond the one
// fetch per cache TTL.
func (s *Service) trackNamespaceWatch(ctx context.Context, w *namespaceWatch) {
	done := s.watches.done(w.relay.writer)
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

		set, denied, err := s.allowed(ctx, w.cluster, w.req.Header)
		if denied != nil {
			_ = denied.Body.Close()
		}
		if denied != nil || err != nil {
			continue
		}
		if !s.followAllowedSet(ctx, w, set) {
			return
		}
	}
}

// followAllowedSet brings the stream of w to set, and reports false once the
// stream ends. A name that the caller loses gets a DELETED event first. A
// watch with a selector then swaps its upstream when the selector changes. A
// watch without a selector keeps its upstream for its whole life, and a name
// that the caller gains gets an ADDED event with the object that
// readNamespace returns. A stream that takes no event of the service ends
// instead, when a change needs such an event.
func (s *Service) followAllowedSet(ctx context.Context, w *namespaceWatch, set allowedSet) bool {
	lost := subtract(w.names, set.names)
	gained := subtract(set.names, w.names)
	selector, selected := watchSelector(w.caller, set)
	if len(lost) == 0 && len(gained) == 0 && (!w.selected || (selected && selector == w.selector)) {
		return true
	}

	if !w.synthesize && (len(lost) > 0 || (!w.selected && len(gained) > 0)) {
		s.expireNamespaceWatch(ctx, w, "ended a namespace watch that takes no event of the service, because the allowed namespaces of the caller changed")
		return false
	}
	for _, name := range lost {
		if !s.emitNamespaceEvent(ctx, w, watchDeleted, name, deletedNamespace(name)) {
			return false
		}
	}
	w.names = subtract(w.names, lost)

	if !w.selected {
		for _, name := range gained {
			object, ok := s.readNamespace(ctx, w, name)
			if !ok {
				continue
			}
			if !s.emitNamespaceEvent(ctx, w, watchAdded, name, object) {
				return false
			}
			w.names = append(w.names, name)
		}
		return true
	}

	if !selected {
		s.expireNamespaceWatch(ctx, w, "ended a namespace watch, because the allowed namespaces of the caller need a watch without a selector")
		return false
	}
	if selector != w.selector {
		if err := s.swapNamespaceWatch(ctx, w, selector); err != nil {
			if errors.Is(err, errStreamEnded) || ctx.Err() != nil {
				return false
			}
			s.expireNamespaceWatch(ctx, w, "ended a namespace watch, because the swap of its upstream failed", "error", err.Error())
			return false
		}
		s.logger.InfoContext(ctx, "swapped the upstream of a namespace watch, because the allowed namespaces of the caller changed",
			"cluster", w.cluster, "lost", len(lost), "gained", len(gained))
		w.selector = selector
	}
	w.names = slices.Clone(set.names)
	return true
}

// swapNamespaceWatch opens a privileged watch with selector and without a
// resourceVersion, and puts it in place of the upstream of w. The API server
// starts such a watch with an ADDED event for each namespace under the
// selector, so the namespaces that the caller gains reach the client. A client
// takes the ADDED event of a namespace that it has already as an update.
//
// The swap takes one token of the fetch limit of the caller, and it fails
// without one. The caller decides when its allowed set changes, and every
// swap replays the namespaces under the selector for each open stream of that
// caller.
func (s *Service) swapNamespaceWatch(ctx context.Context, w *namespaceWatch, selector string) error {
	if err := s.callers.get(callerHash(w.req.Header)).wait(ctx, 0); err != nil {
		return err
	}
	token, err := rancherclient.ReadToken(s.tokenFile)
	if err != nil {
		return err
	}

	out := privilegedRequest(w.req, token)
	out.Header.Set("Accept", filterJSONAccept(w.req.Header.Get("Accept")))
	query := out.URL.Query()
	query.Set("labelSelector", selector)
	for _, key := range []string{"resourceVersion", "resourceVersionMatch", "sendInitialEvents"} {
		query.Del(key)
	}
	out.URL.RawQuery = query.Encode()

	want := http.StatusOK
	if w.relay.upgraded != nil {
		want = http.StatusSwitchingProtocols
		out.Header.Set("Sec-WebSocket-Key", websocketKey())
	}
	resp, err := s.base.RoundTrip(out)
	if err != nil {
		return err
	}
	if resp.StatusCode != want {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrainBody))
		_ = resp.Body.Close()
		return fmt.Errorf("the new upstream watch returned %s", resp.Status)
	}
	if extensions := resp.Header.Get(extensionsHeader); w.relay.upgraded != nil && extensions != "" {
		_ = resp.Body.Close()
		return fmt.Errorf("the new upstream watch names the websocket extension %q", extensions)
	}
	return w.relay.swap(resp)
}

// readNamespace reads the namespace name with the service token, in the media
// type of the watch. Steve lists that name for the caller, so the caller may
// read it. It reports false for an answer that is not 200 with one JSON
// object. The tracker then tries the name again at the next re-read.
func (s *Service) readNamespace(ctx context.Context, w *namespaceWatch, name string) (json.RawMessage, bool) {
	token, err := rancherclient.ReadToken(s.tokenFile)
	if err != nil {
		s.logger.DebugContext(ctx, "read a gained namespace", "cluster", w.cluster, "namespace", name, "reason", err.Error())
		return nil, false
	}
	target := *s.upstream
	target.Path = "/k8s/clusters/" + w.cluster + "/api/v1/namespaces/" + name
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		s.logger.DebugContext(ctx, "read a gained namespace", "cluster", w.cluster, "namespace", name, "reason", err.Error())
		return nil, false
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", filterJSONAccept(w.req.Header.Get("Accept")))

	resp, err := s.base.RoundTrip(req)
	if err != nil {
		s.logger.DebugContext(ctx, "read a gained namespace", "cluster", w.cluster, "namespace", name, "reason", err.Error())
		return nil, false
	}
	body, tooLarge, err := readLimited(resp.Body, maxDrainBody)
	_ = resp.Body.Close()
	var reason string
	var object bytes.Buffer
	switch {
	case err != nil:
		reason = err.Error()
	case resp.StatusCode != http.StatusOK:
		reason = "status " + resp.Status
	case tooLarge:
		reason = "the response is larger than the limit"
	case !startsWithObject(body) || json.Compact(&object, body) != nil:
		reason = "the response is not one JSON object"
	default:
		return object.Bytes(), true
	}
	s.logger.DebugContext(ctx, "read a gained namespace", "cluster", w.cluster, "namespace", name, "reason", reason)
	return nil, false
}

// emitNamespaceEvent writes the watch event of type with object to the stream
// of w, and reports false once the stream ends.
func (s *Service) emitNamespaceEvent(ctx context.Context, w *namespaceWatch, eventType, name string, object json.RawMessage) bool {
	event := json.RawMessage(`{"type":"` + eventType + `","object":` + string(object) + `}`)
	if err := w.relay.emit(event); err != nil {
		return false
	}
	s.logger.DebugContext(ctx, "wrote a watch event of the service", "cluster", w.cluster, "type", eventType, "namespace", name)
	return true
}

// expireNamespaceWatch ends the stream of w after an ERROR event with the
// Status 410 Expired. A client takes that event as the end of its
// resourceVersion window, and it lists again before it watches.
func (s *Service) expireNamespaceWatch(ctx context.Context, w *namespaceWatch, message string, attrs ...any) {
	_ = w.relay.emit(expiredEvent(serviceName + ": the allowed namespaces of the caller changed"))
	s.logger.InfoContext(ctx, message, append([]any{"cluster", w.cluster}, attrs...)...)
	s.watches.end(w.relay.writer)
}

// deletedNamespace returns the object of the DELETED event of a namespace
// that the caller loses: its kind and its name only. The client listed that
// name before, so the event tells it nothing new.
func deletedNamespace(name string) json.RawMessage {
	quoted, _ := json.Marshal(name)
	return json.RawMessage(`{"kind":"Namespace","apiVersion":"v1","metadata":{"name":` + string(quoted) + `}}`)
}

// subtract returns the names of from that names does not have, in the order
// of from.
func subtract(from, names []string) []string {
	have := make(map[string]struct{}, len(names))
	for _, name := range names {
		have[name] = struct{}{}
	}
	var out []string
	for _, name := range from {
		if _, ok := have[name]; !ok {
			out = append(out, name)
		}
	}
	return out
}

// privilegedRequest returns a copy of the request of the caller for the
// service token: with that token in Authorization, and without the cookie and
// the Accept-Encoding of the caller.
func privilegedRequest(req *http.Request, token string) *http.Request {
	privileged := req.Clone(req.Context())
	privileged.Header.Set("Authorization", "Bearer "+token)
	privileged.Header.Del("Cookie")
	privileged.Header.Del("Accept-Encoding")
	// The filter reads no compressed payload. Rancher then negotiates no
	// extension, and the client gets that same answer.
	privileged.Header.Del(extensionsHeader)
	return privileged
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

// tooManyWatches answers 503, because the open watch streams of the service,
// or of the caller when limit is limitCaller, are at the bound. The client
// retries, and a stream that ended in between frees a slot.
func (s *Service) tooManyWatches(req *http.Request, start time.Time, result listResult, limit string) *http.Response {
	result.outcome, result.status = outcomeCapped, http.StatusServiceUnavailable
	s.logList(req.Context(), start, result)
	s.metrics.watchCapped(req.Context(), s.clusters.attribute(result.cluster), limit)
	message := serviceName + ": the open watch streams are at the limit of " + strconv.Itoa(s.watches.maxWatches)
	if limit == limitCaller {
		message = serviceName + ": the open watch streams of the caller are at the per-caller limit of " + strconv.Itoa(s.watches.maxWatchesPerCaller)
	}
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
