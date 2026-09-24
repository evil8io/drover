package filter

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"
)

const (
	defaultFanoutMaxNamespaces = 200
	defaultFanoutConcurrency   = 16
	defaultFanoutMaxInflight   = 64
)

// collectionTarget names one cluster-wide collection of a namespaced kind.
// apiPath is the path up to the version, for example
// /k8s/clusters/c-m-x/apis/apps/v1.
type collectionTarget struct {
	cluster  string
	apiPath  string
	resource string
}

func (t collectionTarget) namespacedPath(name string) string {
	return t.apiPath + "/namespaces/" + url.PathEscape(name) + "/" + t.resource
}

// roundTripCollection answers a cluster-wide list of a namespaced kind. A
// Rancher project member has the list permission inside the namespaces of its
// projects, and none at cluster scope, so the native request returns 403. The
// answer is then one request per allowed namespace, with the credentials of
// the caller, merged into one collection. The caller gets no permission that
// it does not have already, because every request carries its own
// credentials.
func (s *Service) roundTripCollection(req *http.Request, target collectionTarget) (*http.Response, error) {
	start := s.now()
	watch := watchRequested(req.URL.Query())
	result := listResult{cluster: target.cluster, watch: watch, path: pathCollection, resource: target.resource}

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

	denied, err := bufferBody(resp)
	if err != nil {
		return s.statusError(req, start, result, err), nil
	}

	set, callerDenied, err := s.allowed(req.Context(), target.cluster, req.Header)
	if callerDenied != nil || err != nil {
		_ = denied.Body.Close()
		if err != nil {
			return s.statusError(req, start, result, err), nil
		}
		result.outcome, result.status = outcomeDenied, callerDenied.StatusCode
		s.logList(req.Context(), start, result)
		return callerDenied, nil
	}
	result.user = set.user

	if watch {
		return s.mergedWatch(req, target, set, denied, start, result), nil
	}
	if len(set.names) == 0 {
		return s.emptyCollection(req, target, denied, start, result), nil
	}
	if len(set.names) > s.fanoutMaxNamespaces {
		_ = denied.Body.Close()
		s.metrics.fanoutCapped(req.Context(), target.cluster)
		result.outcome, result.status, result.count = outcomeCapped, http.StatusForbidden, len(set.names)
		s.logList(req.Context(), start, result)
		message := serviceName + ": the caller may see " + strconv.Itoa(len(set.names)) +
			" namespaces, above the fan-out limit of " + strconv.Itoa(s.fanoutMaxNamespaces)
		return statusResponse(req, http.StatusForbidden, reasonForbidden, message), nil
	}

	return s.fanout(req, target, set.names, denied, start, result), nil
}

// fanoutSlots has the local slots of one fan-out, and the global slots of
// every fan-out on the Service.
type fanoutSlots struct {
	local  chan struct{}
	global chan struct{}
}

func newFanoutSlots(concurrency int, global chan struct{}) *fanoutSlots {
	return &fanoutSlots{local: make(chan struct{}, concurrency), global: global}
}

// acquire takes the local slot, then the global slot, both against
// ctx.Done(). It reports whether it got both slots.
func (f *fanoutSlots) acquire(ctx context.Context) bool {
	select {
	case f.local <- struct{}{}:
	case <-ctx.Done():
		return false
	}
	select {
	case f.global <- struct{}{}:
		return true
	case <-ctx.Done():
		<-f.local
		return false
	}
}

// release returns the global slot, then the local slot.
func (f *fanoutSlots) release() {
	<-f.global
	<-f.local
}

// fanout requests the collection in each allowed namespace, and returns the
// merged answer. The answers stream out in the order of names, so the filter
// holds at most fanoutConcurrency answers at a time. A namespace that answers
// anything but 200 drops out of the merge, because the caller may hold the
// list permission in some of its namespaces only. The native 403 stands when
// no namespace answers 200.
func (s *Service) fanout(req *http.Request, target collectionTarget, names []string, denied *http.Response, start time.Time, result listResult) *http.Response {
	ctx := req.Context()
	slots := newFanoutSlots(s.fanoutConcurrency, s.fanoutGlobal)
	stopped := &atomic.Bool{}
	answers := make([]chan *http.Response, len(names))
	for i := range answers {
		answers[i] = make(chan *http.Response, 1)
	}

	// The dispatcher takes the local slot before the global slot, and it
	// starts the requests in the order of names. The reader needs the
	// answers in that same order, so a request it waits for always runs
	// already. The global wait blocks the dispatcher only. It never blocks
	// the reader, so no deadlock exists.
	go func() {
		for i, name := range names {
			if stopped.Load() {
				for _, answer := range answers[i:] {
					answer <- nil
				}
				return
			}
			if !slots.acquire(ctx) {
				for _, answer := range answers[i:] {
					answer <- nil
				}
				return
			}
			go s.fetchNamespaced(req, target, name, slots, answers[i], stopped)
		}
	}()

	for i, answer := range answers {
		resp := <-answer
		if resp == nil {
			continue
		}
		scanner, err := openCollection(resp.Body)
		if err != nil {
			s.skipNamespace(ctx, target, resp, slots, err)
			continue
		}

		_ = denied.Body.Close()
		reader, writer := io.Pipe()
		go s.streamFanout(req, writer, scanner, answers[i+1:], slots, target)

		result.outcome, result.status, result.count = outcomeFannedOut, http.StatusOK, len(names)
		s.logList(ctx, start, result)
		s.metrics.fanoutNamespaces(ctx, target.cluster, len(names))

		return &http.Response{
			Status:     "200 OK",
			StatusCode: http.StatusOK,
			Proto:      "HTTP/1.1",
			ProtoMajor: 1,
			ProtoMinor: 1,
			Header: http.Header{
				"Content-Type": []string{jsonContentType},
			},
			Body:          reader,
			ContentLength: -1,
			Request:       req,
		}
	}

	if stopped.Load() {
		result.outcome, result.status, result.count = outcomeNative, denied.StatusCode, len(names)
		s.logList(ctx, start, result)
		return denied
	}
	result.count = len(names)
	return s.emptyCollection(req, target, denied, start, result)
}

// emptyCollection answers a caller that may see no object of the kind: an
// allowed set without a namespace, or a fan-out where every namespace denied
// the list. The answer is an empty collection, not the native 403, because
// the namespace path answers an empty list in that same state and a client
// shows an empty view instead of an error.
//
// Discovery gives the kind and the scope of the resource, with the
// credentials of the caller, because the merge has no answer to take them
// from. A cluster-scoped kind keeps the native 403: the caller may see no
// namespace, and the object has none. The native answer also stands when
// discovery fails.
func (s *Service) emptyCollection(req *http.Request, target collectionTarget, denied *http.Response, start time.Time, result listResult) *http.Response {
	ctx := req.Context()
	kind, apiVersion, namespaced, ok := s.discoverResource(req, target)
	if !ok || !namespaced {
		result.outcome, result.status = outcomeNative, denied.StatusCode
		s.logList(ctx, start, result)
		return denied
	}
	_ = denied.Body.Close()

	body := emptyCollectionJSON(kind+"List", apiVersion)
	result.outcome, result.status = outcomeEmpty, http.StatusOK
	s.logList(ctx, start, result)
	return &http.Response{
		Status:     "200 OK",
		StatusCode: http.StatusOK,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header: http.Header{
			"Content-Type":   []string{jsonContentType},
			"Content-Length": []string{strconv.Itoa(len(body))},
		},
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       req,
	}
}

// discoverResource reads the kind, the group version and the scope of the
// resource from the discovery document of its api path.
func (s *Service) discoverResource(req *http.Request, target collectionTarget) (kind, apiVersion string, namespaced, ok bool) {
	ctx := req.Context()
	out := req.Clone(ctx)
	out.Body = nil
	out.ContentLength = 0
	out.GetBody = nil
	out.URL.Path = target.apiPath
	out.URL.RawPath = ""
	out.URL.RawQuery = ""
	out.Header.Set("Accept", jsonContentType)
	out.Header.Del("Accept-Encoding")
	stripUpgrade(out.Header)

	resp, err := s.base.RoundTrip(out)
	if err != nil {
		s.logger.DebugContext(ctx, "the discovery request failed",
			"cluster", target.cluster, "resource", target.resource, "error", err.Error())
		return "", "", false, false
	}
	body, tooLarge, err := readLimited(resp.Body, maxDrainBody)
	_ = resp.Body.Close()
	if err != nil || tooLarge || resp.StatusCode != http.StatusOK {
		s.logger.DebugContext(ctx, "the discovery request gave no document",
			"cluster", target.cluster, "resource", target.resource, "status", resp.StatusCode)
		return "", "", false, false
	}

	var document apiResourceList
	if err := json.Unmarshal(body, &document); err != nil {
		s.logger.DebugContext(ctx, "the discovery document does not parse",
			"cluster", target.cluster, "resource", target.resource, "error", err.Error())
		return "", "", false, false
	}
	for _, resource := range document.Resources {
		if resource.Name == target.resource && resource.Kind != "" {
			return resource.Kind, document.GroupVersion, resource.Namespaced, true
		}
	}
	return "", "", false, false
}

// fetchNamespaced requests the collection in one namespace, and sends the
// answer. It sends nil when the namespace gives no usable answer, and it
// returns its slot in that case. The reader of a usable answer returns the
// slot, so an unread answer keeps holding one. A 404 sets stopped, because a
// namespaced path of a kind that has no namespace scope, for example nodes,
// answers 404 in every namespace.
func (s *Service) fetchNamespaced(req *http.Request, target collectionTarget, name string, slots *fanoutSlots, answer chan<- *http.Response, stopped *atomic.Bool) {
	ctx := req.Context()
	resp, err := s.base.RoundTrip(namespacedRequest(req, target, name))
	if err != nil {
		s.metrics.fanoutSkipped(ctx, target.cluster)
		s.logger.DebugContext(ctx, "the namespaced request failed",
			"cluster", target.cluster, "resource", target.resource, "namespace", name, "error", err.Error())
		slots.release()
		answer <- nil
		return
	}
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrainBody))
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			stopped.Store(true)
		}
		s.metrics.fanoutSkipped(ctx, target.cluster)
		s.logger.DebugContext(ctx, "the namespaced request returned no collection",
			"cluster", target.cluster, "resource", target.resource, "namespace", name, "status", resp.StatusCode)
		slots.release()
		answer <- nil
		return
	}
	answer <- resp
}

// namespacedRequest returns the request of one namespace. It keeps the
// credentials, the selectors and the resourceVersion of the caller. It drops
// the paging parameters, because a continue token belongs to one namespace:
// the merged answer holds every element and it names no continue token, and a
// client stops after one page.
func namespacedRequest(req *http.Request, target collectionTarget, name string) *http.Request {
	out := req.Clone(req.Context())
	out.Body = nil
	out.ContentLength = 0
	out.GetBody = nil
	out.URL.Path = target.namespacedPath(name)
	out.URL.RawPath = ""

	query := out.URL.Query()
	query.Del("limit")
	query.Del("continue")
	out.URL.RawQuery = query.Encode()

	out.Header.Set("Accept", filterJSONAccept(req.Header.Get("Accept")))
	out.Header.Del("Accept-Encoding")
	out.Header.Del(extensionsHeader)
	return out
}

// streamFanout writes the merged collection: the header of the first answer,
// the elements of every answer, and the metadata last. A read error of an
// answer ends the body with that error, because a half-written collection is
// not valid JSON.
func (s *Service) streamFanout(req *http.Request, writer *io.PipeWriter, first *collectionScanner, rest []chan *http.Response, slots *fanoutSlots, target collectionTarget) {
	ctx := req.Context()
	defer func() { _ = writer.Close() }()

	merged := collectionHeader{arrayKey: first.header.arrayKey}
	written := 0
	// An answer whose tail does not arrive gives no resourceVersion, and a
	// merge without that value must name none, because a client that watches
	// from a value above the true lowest loses events.
	tailLost := false

	consume := func(scanner *collectionScanner) error {
		defer func() {
			if err := scanner.finish(); err != nil {
				tailLost = true
			}
			mergeHeader(&merged, scanner.header)
			scanner.close()
			slots.release()
		}()
		count, err := copyElements(writer, scanner, written)
		written = count
		return err
	}

	if err := writeCollectionHeader(writer, first.header.arrayKey); err != nil {
		first.close()
		slots.release()
		drainAnswers(rest, slots)
		return
	}
	if err := consume(first); err != nil {
		_ = writer.CloseWithError(err)
		drainAnswers(rest, slots)
		return
	}

	for i, answer := range rest {
		resp := <-answer
		if resp == nil {
			continue
		}
		scanner, err := openCollection(resp.Body)
		if err != nil {
			s.skipNamespace(ctx, target, resp, slots, err)
			continue
		}
		if scanner.header.arrayKey != merged.arrayKey {
			s.skipNamespace(ctx, target, resp, slots, errMixedCollection)
			continue
		}
		if err := consume(scanner); err != nil {
			_ = writer.CloseWithError(err)
			drainAnswers(rest[i+1:], slots)
			return
		}
	}

	if tailLost {
		merged.resourceVersion = ""
	}
	if merged.kind == "" {
		// An answer without a kind leaves the merge without one, and a client
		// that reads the kind then fails. Discovery is the last source.
		if kind, apiVersion, _, ok := s.discoverResource(req, target); ok {
			merged.kind, merged.apiVersion = kind+"List", apiVersion
		}
	}
	if err := writeCollectionFooter(writer, merged); err != nil {
		_ = writer.CloseWithError(err)
		return
	}
	s.logger.DebugContext(ctx, "merged a collection",
		"cluster", target.cluster, "resource", target.resource, "kind", merged.kind,
		"items", written, "resource_version", merged.resourceVersion)
}

// mergeHeader takes the fields of one answer that the merged answer still
// needs, and the lowest resourceVersion of the answers so far.
func mergeHeader(merged *collectionHeader, answer collectionHeader) {
	if merged.kind == "" {
		merged.kind = answer.kind
	}
	if merged.apiVersion == "" {
		merged.apiVersion = answer.apiVersion
	}
	if len(merged.columns) == 0 {
		merged.columns = answer.columns
	}
	merged.resourceVersion = lowestResourceVersion(merged.resourceVersion, answer.resourceVersion)
}

// skipNamespace drops one answer that the merge cannot read.
func (s *Service) skipNamespace(ctx context.Context, target collectionTarget, resp *http.Response, slots *fanoutSlots, err error) {
	_ = resp.Body.Close()
	slots.release()
	s.metrics.fanoutSkipped(ctx, target.cluster)
	s.logger.WarnContext(ctx, "the namespaced answer is not a collection",
		"cluster", target.cluster, "resource", target.resource, "error", err.Error())
}

// copyElements writes the elements of scanner, and returns the new count of
// written elements. written separates the first element of the merge from
// every later one with a comma.
func copyElements(w io.Writer, scanner *collectionScanner, written int) (int, error) {
	for {
		element, ok, err := scanner.next()
		if err != nil || !ok {
			return written, err
		}
		if written > 0 {
			if _, err := io.WriteString(w, ","); err != nil {
				return written, err
			}
		}
		if _, err := w.Write(element); err != nil {
			return written, err
		}
		written++
	}
}

// drainAnswers reads the answers that the merge no longer needs, and returns
// their slots, so every dispatched request ends.
func drainAnswers(answers []chan *http.Response, slots *fanoutSlots) {
	for _, answer := range answers {
		resp := <-answer
		if resp == nil {
			continue
		}
		_ = resp.Body.Close()
		slots.release()
	}
}

// bufferBody reads the body of resp into memory, so a caller can hand the
// answer back after it reads it.
func bufferBody(resp *http.Response) (*http.Response, error) {
	body, _, err := readLimited(resp.Body, maxDrainBody)
	_ = resp.Body.Close()
	if err != nil {
		return nil, err
	}
	setResponseBody(resp, body)
	return resp, nil
}
