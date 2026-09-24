package filter

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// defaultFanoutMaxWatchNamespaces bounds a merged watch below the bound of
	// a list. A list holds one upstream request per namespace for the length
	// of that request. A watch holds one upstream connection per namespace for
	// the whole life of the stream, and every client of the caller holds its
	// own set.
	defaultFanoutMaxWatchNamespaces = 50

	// websocketGUID is the constant of RFC 6455. The accept value of a
	// handshake is the SHA-1 of the key of the client and this value.
	websocketGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

	// binarySubprotocol is the websocket subprotocol whose message payload is
	// the watch stream itself.
	binarySubprotocol = "binary.k8s.io"

	watchBookmark = "BOOKMARK"

	// initialEventsEndAnnotation marks the BOOKMARK event that ends the
	// initial events of a watch-list.
	initialEventsEndAnnotation = "k8s.io/initial-events-end"
)

// upstreamWatch is one open watch of one namespace.
type upstreamWatch struct {
	namespace string
	body      io.ReadCloser
}

// watchMerge is one merged watch stream. The service writes the events of
// every upstream watch into writer, and the client reads the other side of
// that pipe. An upgraded merge writes each event as one websocket frame, and
// a chunked merge writes it as one line of the newline-delimited stream.
type watchMerge struct {
	svc      *Service
	cluster  string
	resource string

	writer   *io.PipeWriter
	upgraded bool
	base64   bool

	stop chan struct{}
	once sync.Once

	mu     sync.Mutex
	bodies []io.Closer

	// watchList is true when the caller asked for the initial events. pending
	// counts the upstream watches whose initial events did not end yet,
	// endObject is the object of the first end bookmark, and endVersion is the
	// lowest resourceVersion of the end bookmarks.
	watchList  bool
	pending    int
	endObject  json.RawMessage
	endVersion string
}

// mergedWatch answers a cluster-wide watch of a namespaced kind. The caller
// has the watch permission inside the namespaces of its projects, and none at
// cluster scope, so the native request returns 403. The answer is then one
// upstream watch per allowed namespace, with the credentials of the caller,
// merged into one stream. The caller gets no permission that it does not have
// already, because every upstream watch carries its own credentials.
//
// The set of upstream watches is fixed while the stream runs. A namespace that
// the caller gains or loses therefore ends the stream, and the client re-lists
// and re-watches. An upstream watch that ends ends the merged stream for the
// same reason: the client repairs a gap with a new list, and a stream that
// stays open with one dead namespace shows stale data without saying so.
func (s *Service) mergedWatch(req *http.Request, target collectionTarget, set allowedSet, denied *http.Response, start time.Time, result listResult) *http.Response {
	ctx := req.Context()
	names := set.names
	result.count = len(names)

	slot, limit := s.watches.reserve(watchCaller(req.Header))
	if slot == nil {
		_ = denied.Body.Close()
		return s.tooManyWatches(req, start, result, limit)
	}
	handedOver := false
	defer func() {
		if !handedOver {
			s.watches.release(slot)
		}
	}()
	if len(names) > s.fanoutMaxWatchNamespaces {
		_ = denied.Body.Close()
		s.metrics.fanoutCapped(ctx, target.cluster)
		result.outcome, result.status = outcomeCapped, http.StatusForbidden
		s.logList(ctx, start, result)
		message := serviceName + ": the caller may see " + strconv.Itoa(len(names)) +
			" namespaces, above the fan-out watch limit of " + strconv.Itoa(s.fanoutMaxWatchNamespaces)
		return statusResponse(req, http.StatusForbidden, reasonForbidden, message)
	}

	// A caller that may see no namespace gets a stream without an event, as
	// the list path gives it a collection without an element. A cluster-scoped
	// kind keeps the native answer, because such an object has no namespace.
	var kind, apiVersion string
	if len(names) == 0 {
		var namespaced, ok bool
		kind, apiVersion, namespaced, ok = s.discoverResource(req, target)
		if !ok || !namespaced {
			result.outcome, result.status = outcomeNative, denied.StatusCode
			s.logList(ctx, start, result)
			return denied
		}
	}

	streams, clusterScoped := s.openWatches(req, target, names)
	// A namespaced path of a kind that has no namespace scope, for example
	// nodes, answers 404 in every namespace.
	if clusterScoped || (len(names) > 0 && len(streams) == 0) {
		closeWatches(streams)
		result.outcome, result.status = outcomeNative, denied.StatusCode
		s.logList(ctx, start, result)
		return denied
	}
	_ = denied.Body.Close()

	merge, resp := s.newMerge(req, target, slot)
	handedOver = true
	merge.watchList = watchListRequested(req.URL.Query())
	merge.pending = len(streams)
	for _, stream := range streams {
		merge.hold(stream.body)
	}
	for _, stream := range streams {
		go merge.follow(ctx, stream)
	}
	if merge.watchList && len(streams) == 0 {
		// No upstream watch sends an end bookmark, so the merge sends one
		// itself, from the kind that discovery gave.
		go merge.endInitialEventsWithout(kind, apiVersion)
	}
	go merge.finishOnEnd(ctx)
	go s.endWatchOnAllowedSetChange(ctx, target.cluster, req.Header, names, merge)

	result.outcome, result.status = outcomeFannedOut, resp.StatusCode
	s.logList(ctx, start, result)
	s.metrics.fanoutNamespaces(ctx, target.cluster, len(names))
	return resp
}

// openWatches opens one upstream watch per namespace, at the same time. A
// namespace that answers anything but 200 drops out of the merge, because the
// caller may hold the watch permission in some of its namespaces only. It
// reports whether a namespace answered 404, which says that the kind has no
// namespace scope.
func (s *Service) openWatches(req *http.Request, target collectionTarget, names []string) (streams []upstreamWatch, clusterScoped bool) {
	ctx := req.Context()
	var mu sync.Mutex
	var group sync.WaitGroup

	for _, name := range names {
		group.Add(1)
		go func(name string) {
			defer group.Done()
			resp, err := s.base.RoundTrip(namespacedWatchRequest(req, target, name))
			if err != nil {
				s.metrics.fanoutSkipped(ctx, target.cluster)
				s.logger.DebugContext(ctx, "the namespaced watch failed",
					"cluster", target.cluster, "resource", target.resource, "namespace", name, "error", err.Error())
				return
			}
			if resp.StatusCode != http.StatusOK {
				_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrainBody))
				_ = resp.Body.Close()
				s.metrics.fanoutSkipped(ctx, target.cluster)
				s.logger.DebugContext(ctx, "the namespaced watch returned no stream",
					"cluster", target.cluster, "resource", target.resource, "namespace", name, "status", resp.StatusCode)
				mu.Lock()
				clusterScoped = clusterScoped || resp.StatusCode == http.StatusNotFound
				mu.Unlock()
				return
			}
			mu.Lock()
			streams = append(streams, upstreamWatch{namespace: name, body: resp.Body})
			mu.Unlock()
		}(name)
	}

	group.Wait()
	return streams, clusterScoped
}

func closeWatches(streams []upstreamWatch) {
	for _, stream := range streams {
		_ = stream.body.Close()
	}
}

// namespacedWatchRequest returns the upstream watch request of one namespace.
// It keeps the selectors, the resourceVersion and the timeout of the caller,
// so each upstream watch reports the same window of the stream that a native
// watch of that namespace reports.
//
// It drops allowWatchBookmarks. A bookmark names the resourceVersion that one
// stream reached, and the merged stream has one such value per namespace, so a
// bookmark of one namespace is not a resume point of the merge. A watch-list
// keeps it, because the bookmark that ends the initial events needs it.
//
// It drops the websocket handshake of the caller, because each upstream watch
// takes the chunked transport. The merge owns the transport of the client, and
// it encodes the merged stream for it.
func namespacedWatchRequest(req *http.Request, target collectionTarget, name string) *http.Request {
	out := namespacedRequest(req, target, name)
	stripUpgrade(out.Header)

	query := out.URL.Query()
	if watchListRequested(query) {
		query.Set("allowWatchBookmarks", "true")
	} else {
		query.Del("allowWatchBookmarks")
	}
	out.URL.RawQuery = query.Encode()
	return out
}

// stripUpgrade removes the websocket handshake of the caller from an upstream
// request, so the upstream answers a plain body.
func stripUpgrade(header http.Header) {
	for _, name := range []string{
		"Connection",
		"Upgrade",
		"Sec-Websocket-Key",
		"Sec-Websocket-Version",
		"Sec-Websocket-Protocol",
		extensionsHeader,
	} {
		header.Del(name)
	}
}

// newMerge returns the merge and the answer of the client. It registers the
// merge with slot. An upgrade request gets the protocol switch of RFC 6455,
// which the merge answers itself, because it has no single upstream
// connection to relay. Every other request gets a chunked body.
func (s *Service) newMerge(req *http.Request, target collectionTarget, slot *watchSlot) (*watchMerge, *http.Response) {
	ctx := req.Context()
	reader, writer := io.Pipe()
	merge := &watchMerge{
		svc:      s,
		cluster:  target.cluster,
		resource: target.resource,
		writer:   writer,
		stop:     make(chan struct{}),
	}

	key := req.Header.Get("Sec-Websocket-Key")
	if !isWebsocketUpgrade(req.Header) || key == "" {
		s.watches.add(writer, false, slot)
		s.metrics.watchOpened(ctx)
		return merge, &http.Response{
			Status:        "200 OK",
			StatusCode:    http.StatusOK,
			Proto:         "HTTP/1.1",
			ProtoMajor:    1,
			ProtoMinor:    1,
			Header:        http.Header{"Content-Type": []string{jsonContentType}},
			Body:          reader,
			ContentLength: -1,
			Request:       req,
		}
	}

	subprotocol, base64Encode := pickSubprotocol(req.Header.Get("Sec-Websocket-Protocol"))
	merge.upgraded, merge.base64 = true, base64Encode
	header := http.Header{
		"Connection":           []string{"Upgrade"},
		"Upgrade":              []string{"websocket"},
		"Sec-Websocket-Accept": []string{websocketAccept(key)},
	}
	if subprotocol != "" {
		header.Set("Sec-Websocket-Protocol", subprotocol)
	}

	s.watches.add(writer, true, slot)
	s.metrics.watchOpened(ctx)
	s.logger.DebugContext(ctx, "opened an upgraded merged watch",
		"cluster", target.cluster, "resource", target.resource, "subprotocol", subprotocol)

	frames, clientWriter := io.Pipe()
	go merge.readClientFrames(frames)

	return merge, &http.Response{
		Status:     "101 Switching Protocols",
		StatusCode: http.StatusSwitchingProtocols,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     header,
		Body:       &mergedUpgrade{PipeReader: reader, frames: clientWriter},
		// A protocol switch has the content length of a response of the 1xx
		// class, which is zero. A length of -1 makes the response writer add
		// Connection: close, which ends the connection that the switch opens.
		ContentLength: 0,
		Request:       req,
	}
}

// mergedUpgrade is the body of an upgraded merged watch. Read gives the frames
// of the merged stream. Write takes the frames of the client, which carry a
// close or a ping frame only, because a watch client sends no data.
// httputil.ReverseProxy needs this interface for an upgraded connection.
type mergedUpgrade struct {
	*io.PipeReader
	frames *io.PipeWriter
}

func (u *mergedUpgrade) Write(p []byte) (int, error) {
	return u.frames.Write(p)
}

func (u *mergedUpgrade) Close() error {
	_ = u.frames.Close()
	return u.PipeReader.Close()
}

// follow reads the events of one upstream watch, and writes each of them to
// the client. The upstream applies the credentials of the caller and the
// namespace, so every event of it belongs to the client and the merge drops
// none. The end of the upstream ends the merged stream.
func (m *watchMerge) follow(ctx context.Context, stream upstreamWatch) {
	decoder := json.NewDecoder(stream.body)
	for {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			if ctx.Err() == nil {
				m.end("ended a merged watch, because an upstream watch ended", "namespace", stream.namespace)
			}
			return
		}
		if isBookmark(raw) {
			if m.watchList {
				if err := m.endInitialEvents(raw); err != nil {
					return
				}
			}
			continue
		}
		if err := m.write(raw); err != nil {
			return
		}
	}
}

// isBookmark reports whether the event is a BOOKMARK. The merged stream
// sends none, because the resourceVersion of one namespace is no resume point
// of the merge, and a client that resumes from it loses the events of every
// other namespace. The one exception is the bookmark that ends the initial
// events of a watch-list, which endInitialEvents merges.
func isBookmark(raw json.RawMessage) bool {
	var event struct {
		Type string `json:"type"`
	}
	return json.Unmarshal(raw, &event) == nil && event.Type == watchBookmark
}

// endInitialEvents counts one upstream watch whose initial events ended, when
// the bookmark has the end annotation, and it writes the merged end once every
// upstream watch reached it. The merged bookmark has the object of the first
// end bookmark, with the lowest resourceVersion of them, for the reason that
// the merged list reports the lowest value.
func (m *watchMerge) endInitialEvents(raw json.RawMessage) error {
	var event struct {
		Object json.RawMessage `json:"object"`
	}
	if json.Unmarshal(raw, &event) != nil {
		return nil
	}
	version, ok := initialEventsEndVersion(event.Object)
	if !ok {
		return nil
	}

	m.mu.Lock()
	if m.pending == 0 {
		m.mu.Unlock()
		return nil
	}
	if m.endObject == nil {
		m.endObject = event.Object
	}
	m.endVersion = lowestResourceVersion(m.endVersion, version)
	m.pending--
	done := m.pending == 0
	object, lowest := m.endObject, m.endVersion
	m.mu.Unlock()
	if !done {
		return nil
	}

	bookmark, err := mergedBookmark(object, lowest)
	if err != nil {
		return err
	}
	return m.write(bookmark)
}

// endInitialEventsWithout writes the merged end for a caller with no upstream
// watch, from the kind and the group version of the resource.
func (m *watchMerge) endInitialEventsWithout(kind, apiVersion string) {
	object := []byte(`{"kind":` + strconv.Quote(kind) + `,"apiVersion":` + strconv.Quote(apiVersion) + `,"metadata":{}}`)
	bookmark, err := mergedBookmark(object, "")
	if err != nil {
		return
	}
	_ = m.write(bookmark)
}

// initialEventsEndVersion returns the resourceVersion of a bookmark object
// that has the end annotation, and reports whether the object has it.
func initialEventsEndVersion(object json.RawMessage) (string, bool) {
	var metadata struct {
		Metadata struct {
			ResourceVersion string            `json:"resourceVersion"`
			Annotations     map[string]string `json:"annotations"`
		} `json:"metadata"`
	}
	if json.Unmarshal(object, &metadata) != nil {
		return "", false
	}
	if metadata.Metadata.Annotations[initialEventsEndAnnotation] != "true" {
		return "", false
	}
	return metadata.Metadata.ResourceVersion, true
}

// mergedBookmark returns the BOOKMARK event that ends the initial events of
// the merge: the object, with version as its resourceVersion and with the end
// annotation.
func mergedBookmark(object json.RawMessage, version string) (json.RawMessage, error) {
	var fields map[string]any
	if err := json.Unmarshal(object, &fields); err != nil {
		return nil, err
	}
	metadata, _ := fields["metadata"].(map[string]any)
	if metadata == nil {
		metadata = map[string]any{}
	}
	annotations, _ := metadata["annotations"].(map[string]any)
	if annotations == nil {
		annotations = map[string]any{}
	}
	annotations[initialEventsEndAnnotation] = "true"
	metadata["annotations"] = annotations
	metadata["resourceVersion"] = version
	fields["metadata"] = metadata
	return json.Marshal(map[string]any{"type": watchBookmark, "object": fields})
}

// write sends one event to the client, in the encoding of its transport.
func (m *watchMerge) write(raw json.RawMessage) error {
	line := make([]byte, 0, len(raw)+1)
	line = append(line, raw...)
	line = append(line, '\n')
	if m.upgraded {
		line = encodeMessage(m.base64, line)
	}
	return m.writeRaw(line)
}

// writeRaw writes bytes that are ready for the transport. Every upstream watch
// writes into the same pipe, so the write takes the lock of the merge.
func (m *watchMerge) writeRaw(p []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, err := m.writer.Write(p)
	return err
}

// hold keeps the body of one upstream watch, so the end of the merge closes it.
func (m *watchMerge) hold(body io.Closer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bodies = append(m.bodies, body)
}

// end ends the merged stream with a clean end of the stream. An upgraded
// stream gets a websocket close frame first. The first caller wins, because
// every upstream watch ends the merge on its own end.
func (m *watchMerge) end(message string, attrs ...any) {
	m.once.Do(func() {
		attrs = append([]any{"cluster", m.cluster, "resource", m.resource}, attrs...)
		m.svc.logger.Info(message, attrs...)
		m.svc.watches.end(m.writer)
		close(m.stop)
	})
}

// finishOnEnd releases the merge once the stream ends, or once the client
// leaves. It takes the merge out of the registry, lowers the open-watch count,
// and closes every upstream watch, which ends the goroutine of each.
func (m *watchMerge) finishOnEnd(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-m.stop:
	}

	m.svc.watches.remove(m.writer)
	m.svc.metrics.watchClosed(context.WithoutCancel(ctx))
	_ = m.writer.Close()

	m.mu.Lock()
	bodies := m.bodies
	m.bodies = nil
	m.mu.Unlock()
	for _, body := range bodies {
		_ = body.Close()
	}
}

// readClientFrames reads the frames that the client sends on an upgraded
// merge. A close frame ends the merged stream, and a ping frame gets a pong.
// The merge answers both itself, because it has no upstream connection that
// answers them.
func (m *watchMerge) readClientFrames(frames io.ReadCloser) {
	defer func() { _ = frames.Close() }()

	src := bufio.NewReader(frames)
	for {
		header, err := readFrameHeader(src)
		if err != nil {
			return
		}
		if header.length > maxBuffer {
			m.end("ended a merged watch, because a client frame is above the buffer limit")
			return
		}
		payload := make([]byte, header.length)
		if _, err := io.ReadFull(src, payload); err != nil {
			return
		}
		switch header.opcode {
		case opcodeClose:
			m.end("ended a merged watch, because the client closed the connection")
			return
		case opcodePing:
			if header.length <= maxControlPayload {
				_ = m.writeRaw(controlFrame(opcodePong, unmask(payload, header)))
			}
		}
	}
}

// controlFrame returns one unmasked control frame with the payload. A
// server-to-client frame has no mask, and a control frame has no continuation.
func controlFrame(opcode byte, payload []byte) []byte {
	frame := []byte{0x80 | opcode, byte(len(payload))}
	return append(frame, payload...)
}

// endWatchOnAllowedSetChange ends the merged stream once the allowed
// namespaces of the caller differ from the set that the upstream watches
// cover. That set is fixed while the stream runs, so a namespace that Rancher
// grants later reaches the client only through a new watch. The client re-lists
// and re-watches after the end of the stream, and the new merge covers the new
// set. The re-read of the allowed set goes through the cache of a plain list,
// so it adds no request beyond the one fetch per cache TTL.
func (s *Service) endWatchOnAllowedSetChange(ctx context.Context, cluster string, header http.Header, names []string, merge *watchMerge) {
	ticker := time.NewTicker(s.cache.ttl)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-merge.stop:
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
		if slices.Equal(set.names, names) {
			continue
		}
		merge.end("ended a merged watch, because the allowed namespaces of the caller changed")
		return
	}
}

// isWebsocketUpgrade reports whether the caller asks for a websocket.
func isWebsocketUpgrade(header http.Header) bool {
	if !strings.EqualFold(header.Get("Upgrade"), "websocket") {
		return false
	}
	for _, value := range header.Values("Connection") {
		for _, token := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), "upgrade") {
				return true
			}
		}
	}
	return false
}

// pickSubprotocol takes the first subprotocol of the offer that the merge
// encodes, in the order of the offer, as the API server does. An offer without
// such a subprotocol gives no subprotocol and binary frames.
func pickSubprotocol(offer string) (name string, base64Encode bool) {
	for _, entry := range strings.Split(offer, ",") {
		switch strings.TrimSpace(entry) {
		case binarySubprotocol:
			return binarySubprotocol, false
		case base64Subprotocol:
			return base64Subprotocol, true
		}
	}
	return "", false
}

// websocketAccept returns the Sec-WebSocket-Accept value of the handshake.
// RFC 6455 defines that value as the SHA-1 of the key and a constant. The hash
// carries no secret, and it proves no identity.
func websocketAccept(key string) string {
	sum := sha1.Sum([]byte(key + websocketGUID)) //nolint:gosec // RFC 6455 names SHA-1 for the handshake.
	return base64.StdEncoding.EncodeToString(sum[:])
}
