package projectsync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const (
	// projectLabel is the label of Rancher on a namespace of a project. Its value
	// is the project name, the part of the project id after the colon.
	projectLabel = "field.cattle.io/projectId"

	projectsPath   = "/v3/projects"
	mergePatchType = "application/merge-patch+json"
	maxPages       = 100
	maxBody        = 32 << 20
	pageSize       = 500
	maxRecordValue = 4096
)

// errListExpired marks a namespace list whose continue token is too old.
var errListExpired = errors.New("the continue token of the namespace list is too old")

// project is the part of a Rancher project that the service reads.
type project struct {
	ID          string            `json:"id"`
	ClusterID   string            `json:"clusterId"`
	Name        string            `json:"name"`
	CreatorID   string            `json:"creatorId"`
	Created     string            `json:"created"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`

	// reserved marks the System and the Default project, and marked marks a
	// project with the label of the account project. pruneProject sets both
	// before it drops the labels that the sync does not copy.
	reserved bool
	marked   bool
}

// pagination is the part of the pagination of a Rancher collection that the
// service reads.
type pagination struct {
	Next string `json:"next"`
}

// namespace is the part of a Kubernetes namespace that the service reads.
type namespace struct {
	Metadata struct {
		Name              string            `json:"name"`
		UID               string            `json:"uid"`
		ResourceVersion   string            `json:"resourceVersion"`
		DeletionTimestamp string            `json:"deletionTimestamp"`
		Labels            map[string]string `json:"labels"`
		Annotations       map[string]string `json:"annotations"`
	} `json:"metadata"`
}

// listMeta is the part of the metadata of a Kubernetes list that the service
// reads.
type listMeta struct {
	Continue string `json:"continue"`
}

// pruneNamespace returns the name, the resource version, and the keys that the
// sync reads of item.
func (s *Syncer) pruneNamespace(item namespace) namespace {
	var out namespace
	out.Metadata.Name = item.Metadata.Name
	out.Metadata.UID = item.Metadata.UID
	out.Metadata.ResourceVersion = item.Metadata.ResourceVersion
	out.Metadata.DeletionTimestamp = item.Metadata.DeletionTimestamp
	out.Metadata.Labels = pick(item.Metadata.Labels, s.readLabels)
	out.Metadata.Annotations = pick(item.Metadata.Annotations, s.readAnnotations)
	// A tenant can fill a record up to the annotation limit of the API server,
	// and a record that the service writes is a short key list.
	for _, key := range []string{managedLabelsKey, managedAnnotationsKey} {
		if len(out.Metadata.Annotations[key]) > maxRecordValue {
			delete(out.Metadata.Annotations, key)
		}
	}
	return out
}

// pruneProject returns item with only the labels and the annotations that the
// sync copies.
func (s *Syncer) pruneProject(item project) project {
	item.reserved = item.Labels[systemProjectLabel] == "true" || item.Labels[defaultProjectLabel] == "true"
	item.marked = item.Labels[accountProjectLabel] == "true"
	item.Labels = pick(item.Labels, s.labels)
	item.Annotations = pick(item.Annotations, s.annotations)
	return item
}

// pick returns a new map with the entries of values at keys. It returns nil
// when values has none of the keys.
func pick(values map[string]string, keys []string) map[string]string {
	var out map[string]string
	for _, key := range keys {
		value, ok := values[key]
		if !ok {
			continue
		}
		if out == nil {
			out = make(map[string]string, len(keys))
		}
		out[key] = value
	}
	return out
}

// watchEvent is one event of a namespace watch stream. The object is a
// Namespace on every type but ERROR, which carries a Status.
type watchEvent struct {
	Type   string          `json:"type"`
	Object json.RawMessage `json:"object"`
}

// namespace returns the Namespace of the event.
func (e watchEvent) namespace() (namespace, error) {
	var item namespace
	if err := json.Unmarshal(e.Object, &item); err != nil {
		return namespace{}, err
	}
	return item, nil
}

// status returns the reason and the message of an ERROR event, as one line.
func (e watchEvent) status() string {
	var answer kubeStatus
	if err := json.Unmarshal(e.Object, &answer); err != nil {
		return "the watch returned an error event"
	}
	return fmt.Sprintf("the watch returned an error event: %s %s", answer.Reason, answer.Message)
}

// kubeStatus is the part of a Kubernetes Status object that the service reads.
type kubeStatus struct {
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

// statusError is an answer of Rancher with a status that the service does not expect.
type statusError struct {
	method  string
	path    string
	status  int
	reason  string
	message string
}

// newStatusError returns the error of an answer with status. It reads the
// reason and the message from a Kubernetes Status body, and it accepts a body
// that is not one.
func newStatusError(method, path string, status int, body []byte) statusError {
	err := statusError{method: method, path: path, status: status}
	var answer kubeStatus
	if json.Unmarshal(body, &answer) == nil {
		err.reason, err.message = answer.Reason, answer.Message
	}
	return err
}

func (e statusError) Error() string {
	if e.reason == "" {
		return fmt.Sprintf("%s %s returned status %d", e.method, e.path, e.status)
	}
	return fmt.Sprintf("%s %s returned status %d, reason %s", e.method, e.path, e.status, e.reason)
}

// skippable reports whether a failed namespace patch needs no error. A
// namespace that is gone answers 404. A namespace that another writer changed
// at the same time answers 409. A namespace in Terminating answers 403 with a
// message that names that state. The next reconcile run repeats the work.
func skippable(err error) bool {
	var status statusError
	if !errors.As(err, &status) {
		return false
	}
	switch status.status {
	case http.StatusNotFound, http.StatusConflict:
		return true
	case http.StatusForbidden:
		return strings.Contains(strings.ToLower(status.message), "terminat")
	}
	return false
}

// projects returns every project that the service user sees, over all pages.
func (s *Syncer) projects(ctx context.Context, token string) ([]project, error) {
	target := s.target(projectsPath, url.Values{"limit": []string{strconv.Itoa(pageSize)}})

	var projects []project
	for page := 0; page < maxPages; page++ {
		items, next, err := s.projectPage(ctx, token, target)
		if err != nil {
			return nil, err
		}
		projects = append(projects, items...)

		if next == "" {
			return projects, nil
		}
		target, err = s.nextTarget(next)
		if err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("the project list has more than %d pages", maxPages)
}

// projectPage reads one page of the project list. It returns the projects and
// the link to the next page.
func (s *Syncer) projectPage(ctx context.Context, token, target string) ([]project, string, error) {
	resp, err := s.getList(ctx, projectsPath, target, token)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = resp.Body.Close() }()

	var (
		items []project
		next  pagination
	)
	if err := decodePage(resp.Body, s.pageCap, "pagination", &next, "data", collect(&items, s.pruneProject)); err != nil {
		return nil, "", fmt.Errorf("decode the project list: %w", err)
	}
	return items, next.Next, nil
}

// namespaces returns the namespaces of a cluster that the label selector
// selects. An expired continue token starts the list again from the first
// page, once.
func (s *Syncer) namespaces(ctx context.Context, token, cluster, selector string) ([]namespace, error) {
	items, err := s.listNamespaces(ctx, token, cluster, selector)
	if errors.Is(err, errListExpired) {
		items, err = s.listNamespaces(ctx, token, cluster, selector)
	}
	return items, err
}

// listNamespaces reads the namespace list of a cluster, over all pages.
func (s *Syncer) listNamespaces(ctx context.Context, token, cluster, selector string) ([]namespace, error) {
	var (
		items []namespace
		next  string
	)
	for page := 0; page < maxPages; page++ {
		batch, more, err := s.namespacePage(ctx, token, cluster, selector, next)
		if err != nil {
			var status statusError
			if next != "" && errors.As(err, &status) && status.status == http.StatusGone {
				return nil, fmt.Errorf("%w: %w", errListExpired, err)
			}
			return nil, err
		}
		items = append(items, batch...)

		if more == "" {
			return items, nil
		}
		next = more
	}
	return nil, fmt.Errorf("the namespace list of cluster %s has more than %d pages", cluster, maxPages)
}

// namespacePage reads the page of the namespace list of a cluster that the
// continue token next selects. It returns the namespaces and the continue
// token of the next page.
func (s *Syncer) namespacePage(ctx context.Context, token, cluster, selector, next string) ([]namespace, string, error) {
	path := namespacesPath(cluster)
	query := url.Values{"limit": []string{strconv.Itoa(pageSize)}}
	if selector != "" {
		query.Set("labelSelector", selector)
	}
	if next != "" {
		query.Set("continue", next)
	}

	resp, err := s.getList(ctx, path, s.target(path, query), token)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = resp.Body.Close() }()

	var (
		items []namespace
		meta  listMeta
	)
	if err := decodePage(resp.Body, s.pageCap, "metadata", &meta, "items", collect(&items, s.pruneNamespace)); err != nil {
		return nil, "", fmt.Errorf("decode the namespace list of cluster %s: %w", cluster, err)
	}
	return items, meta.Continue, nil
}

// decodePage decodes one page of a list from body as a stream. It passes the
// decoder to item once per element of the array at itemsKey, decodes the
// value at metaKey into meta, and skips every other key. The keys can come in
// any order. A body larger than limit bytes returns an error that names the
// limit.
func decodePage(body io.Reader, limit int64, metaKey string, meta any, itemsKey string, item func(*json.Decoder) error) error {
	limited := &io.LimitedReader{R: body, N: limit + 1}
	err := walkPage(json.NewDecoder(limited), metaKey, meta, itemsKey, item)
	if err != nil && limited.N == 0 {
		return fmt.Errorf("the page is larger than %d bytes", limit)
	}
	return err
}

func walkPage(dec *json.Decoder, metaKey string, meta any, itemsKey string, item func(*json.Decoder) error) error {
	if err := expectDelim(dec, '{'); err != nil {
		return err
	}
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return err
		}
		switch key {
		case metaKey:
			err = dec.Decode(meta)
		case itemsKey:
			err = walkItems(dec, item)
		default:
			err = dec.Decode(new(json.RawMessage))
		}
		if err != nil {
			return err
		}
	}
	return expectDelim(dec, '}')
}

// walkItems passes the decoder to item once per element of the array that
// comes next. A null array has no element.
func walkItems(dec *json.Decoder, item func(*json.Decoder) error) error {
	token, err := dec.Token()
	if err != nil || token == nil {
		return err
	}
	if token != json.Delim('[') {
		return fmt.Errorf("the item list is %v, not an array", token)
	}
	for dec.More() {
		if err := item(dec); err != nil {
			return err
		}
	}
	return expectDelim(dec, ']')
}

func expectDelim(dec *json.Decoder, want json.Delim) error {
	token, err := dec.Token()
	if err != nil {
		return err
	}
	if token != want {
		return fmt.Errorf("the list has %v where %v is expected", token, want)
	}
	return nil
}

// collect returns an item function for decodePage. It decodes one item, and
// appends the part of it that prune returns to items.
func collect[T any](items *[]T, prune func(T) T) func(*json.Decoder) error {
	return func(dec *json.Decoder) error {
		var item T
		if err := dec.Decode(&item); err != nil {
			return err
		}
		*items = append(*items, prune(item))
		return nil
	}
}

// patchNamespace sends the patch of one namespace as a JSON merge patch.
func (s *Syncer) patchNamespace(ctx context.Context, token, cluster, name string, change patch) error {
	body, err := change.body()
	if err != nil {
		return fmt.Errorf("encode the patch of namespace %s: %w", name, err)
	}

	path := namespacesPath(cluster) + "/" + name
	status, answer, err := s.do(ctx, http.MethodPatch, s.target(path, nil), token, mergePatchType, body)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return newStatusError(http.MethodPatch, path, status, answer)
	}
	return nil
}

// getList sends a GET for one page of a list. On status 200 it returns the
// response with the body open, and the request timeout lasts until the caller
// closes the body. Another status returns a statusError.
func (s *Syncer) getList(ctx context.Context, path, target, token string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	resp, err := s.openStream(ctx, target, token)
	if err != nil {
		cancel()
		return nil, err
	}
	if resp.StatusCode == http.StatusOK {
		resp.Body = cancelOnClose{ReadCloser: resp.Body, cancel: cancel}
		return resp, nil
	}

	defer cancel()
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return nil, fmt.Errorf("read the answer of GET %s: %w", path, err)
	}
	return nil, newStatusError(http.MethodGet, path, resp.StatusCode, body)
}

// cancelOnClose is a response body that ends the context of its request on
// Close.
type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b cancelOnClose) Close() error {
	defer b.cancel()
	return b.ReadCloser.Close()
}

// openStream sends a GET and returns the response with the body open. Only
// ctx ends the request, so that a watch stream can outlive the request timeout
// of the service. The caller closes the body.
func (s *Syncer) openStream(ctx context.Context, target, token string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", s.userAgent)
	return s.client.Do(req)
}

func namespacesPath(cluster string) string {
	return "/k8s/clusters/" + cluster + "/api/v1/namespaces"
}

func (s *Syncer) target(path string, query url.Values) string {
	target := *s.rancher
	target.Path = path
	if query != nil {
		target.RawQuery = query.Encode()
	}
	return target.String()
}

// nextTarget returns the next page of Rancher on the configured host. Rancher
// builds the link from its own server URL, which is not always reachable here.
func (s *Syncer) nextTarget(next string) (string, error) {
	target, err := url.Parse(next)
	if err != nil {
		return "", fmt.Errorf("parse the next page link: %w", err)
	}
	target.Scheme = s.rancher.Scheme
	target.Host = s.rancher.Host
	return target.String(), nil
}

func (s *Syncer) do(ctx context.Context, method, target, token, contentType string, body []byte) (int, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", s.userAgent)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	answer, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return resp.StatusCode, nil, fmt.Errorf("read the answer of %s %s: %w", method, req.URL.Path, err)
	}
	return resp.StatusCode, answer, nil
}
