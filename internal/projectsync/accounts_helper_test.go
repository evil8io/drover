package projectsync

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	// acctCluster is the cluster of the accounts fake. acctSelf is the id that
	// the fake gives the service user.
	acctCluster = "c-1"
	acctSelf    = "u-sync"

	// acctRBACBase is the base path of the rbac.authorization.k8s.io/v1 group
	// of acctCluster.
	acctRBACBase = "/k8s/clusters/" + acctCluster + "/apis/" + rbacAPIVersion
)

// storedProject is the Norman fields of one project that the accounts fake
// keeps.
type storedProject struct {
	creatorID string
	created   string
	labels    map[string]string
}

// listEnvelope is the list shape that every Kubernetes collection endpoint of
// the fake answers with: a metadata object and an items array, the shape that
// decodePage reads.
type listEnvelope[T any] struct {
	Metadata listMeta `json:"metadata"`
	Items    []T      `json:"items"`
}

// normanCollection is the list shape of a Norman collection, the shape that
// the project list reads.
type normanCollection[T any] struct {
	Type       string     `json:"type"`
	Data       []T        `json:"data"`
	Pagination pagination `json:"pagination"`
}

// accountsFake serves the Kubernetes endpoints of one cluster, c-1, and the
// Norman project and user endpoints, for the service accounts feature. It
// keeps an in-memory store and records every request. A test seeds and
// mutates the store directly through its methods, all of which take the
// store lock.
type accountsFake struct {
	server *httptest.Server

	mu                  sync.Mutex
	namespaces          map[string]namespace
	serviceAccounts     map[string]map[string]object
	roleBindings        map[string]map[string]binding
	clusterRoleBindings map[string]binding
	projects            map[string]storedProject
	// unlisted are projects that answer 200 on the single-project lookup, but
	// that GET /v3/projects does not list. Test 10 uses this for a project
	// that the run cannot see, but that still exists.
	unlisted map[string]storedProject
	self     string
	seq      int
	requests []recorded
	// patchFailures is the status that a PATCH of a namespace answers,
	// instead of applying it, by namespace name.
	patchFailures map[string]int
}

func newAccountsFake(t *testing.T) *accountsFake {
	t.Helper()
	f := &accountsFake{
		namespaces:          make(map[string]namespace),
		serviceAccounts:     make(map[string]map[string]object),
		roleBindings:        make(map[string]map[string]binding),
		clusterRoleBindings: make(map[string]binding),
		projects:            make(map[string]storedProject),
		unlisted:            make(map[string]storedProject),
		self:                acctSelf,
	}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.server.Close)
	return f
}

// newAccountsSyncer returns a syncer with the service accounts on, pointed at
// fake. It configures no label or annotation key, so the reconcile of the
// original label copy never patches a namespace, and only the account writes
// show up in the fake.
func newAccountsSyncer(t *testing.T, fake *accountsFake, opts ...func(*Config)) (*Syncer, *syncBuffer) {
	t.Helper()
	target, err := url.Parse(fake.server.URL)
	if err != nil {
		t.Fatalf("parse the server URL: %v", err)
	}
	logs := &syncBuffer{}
	cfg := Config{
		RancherURL:      target,
		TokenFile:       tokenFile(t, serviceToken+"\n"),
		ServiceAccounts: true,
		Interval:        time.Second,
		Logger:          slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Version:         "test",
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	syncer, err := New(cfg)
	if err != nil {
		t.Fatalf("new syncer: %v", err)
	}
	return syncer, logs
}

// --- seed and mutate helpers, called by the tests ---

func (f *accountsFake) addProject(name string, edit func(*storedProject)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	p := storedProject{created: fmt.Sprintf("created-%03d", f.seq)}
	if edit != nil {
		edit(&p)
	}
	f.projects[name] = p
}

func (f *accountsFake) addUnlistedProject(name string, edit func(*storedProject)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	p := storedProject{created: fmt.Sprintf("created-%03d", f.seq)}
	if edit != nil {
		edit(&p)
	}
	f.unlisted[name] = p
}

func (f *accountsFake) addNamespace(ns namespace) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.namespaces[ns.Metadata.Name] = ns
}

func (f *accountsFake) namespaceUID(name string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.namespaces[name].Metadata.UID
}

func (f *accountsFake) mutateNamespace(name string, edit func(*namespace)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ns := f.namespaces[name]
	edit(&ns)
	f.namespaces[name] = ns
}

func (f *accountsFake) removeProject(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.projects, name)
}

// failPatch answers every PATCH of the namespace name with status, instead of
// applying it.
func (f *accountsFake) failPatch(name string, status int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.patchFailures == nil {
		f.patchFailures = make(map[string]int)
	}
	f.patchFailures[name] = status
}

func (f *accountsFake) addRoleBinding(ns string, b binding) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.roleBindings[ns] == nil {
		f.roleBindings[ns] = make(map[string]binding)
	}
	f.roleBindings[ns][b.Metadata.Name] = b
}

func (f *accountsFake) addClusterRoleBinding(b binding) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.clusterRoleBindings[b.Metadata.Name] = b
}

func (f *accountsFake) mutateRoleBinding(ns, name string, edit func(*binding)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b := f.roleBindings[ns][name]
	edit(&b)
	f.roleBindings[ns][name] = b
}

func (f *accountsFake) mutateClusterRoleBinding(name string, edit func(*binding)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b := f.clusterRoleBindings[name]
	edit(&b)
	f.clusterRoleBindings[name] = b
}

func (f *accountsFake) removeClusterRoleBinding(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.clusterRoleBindings, name)
}

func (f *accountsFake) resetRequests() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = nil
}

func (f *accountsFake) all() []recorded {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.requests)
}

func (f *accountsFake) requestsOfPath(path string) []recorded {
	var out []recorded
	for _, req := range f.all() {
		if req.path == path {
			out = append(out, req)
		}
	}
	return out
}

// writes returns every request of a method other than GET, in order.
func (f *accountsFake) writes() []recorded {
	var out []recorded
	for _, req := range f.all() {
		if req.method != http.MethodGet {
			out = append(out, req)
		}
	}
	return out
}

// --- the HTTP server ---

func (f *accountsFake) serve(w http.ResponseWriter, r *http.Request) {
	f.record(r)
	w.Header().Set("Content-Type", "application/json")

	path := r.URL.Path
	switch {
	case r.Method == http.MethodGet && path == usersPath:
		f.serveSelf(w)
	case r.Method == http.MethodGet && path == projectsPath:
		f.serveProjectList(w)
	case r.Method == http.MethodGet && strings.HasPrefix(path, projectsPath+"/"):
		f.serveProjectItem(w, strings.TrimPrefix(path, projectsPath+"/"))
	case r.Method == http.MethodPost && path == clusterPath(rancherCluster)+managementProjects+acctCluster+"/projects":
		f.createProject(w, r)
	case strings.HasPrefix(path, acctRBACBase+"/clusterrolebindings"):
		f.serveClusterRoleBindings(w, r, strings.TrimPrefix(path, acctRBACBase+"/clusterrolebindings"))
	case r.Method == http.MethodGet && path == acctRBACBase+"/rolebindings":
		f.serveAllRoleBindings(w, r)
	case strings.HasPrefix(path, acctRBACBase+"/namespaces/"):
		f.serveNamespacedRoleBindings(w, r, strings.TrimPrefix(path, acctRBACBase+"/namespaces/"))
	case r.Method == http.MethodGet && path == serviceAccountsPath(acctCluster, ""):
		f.serveAllServiceAccounts(w, r)
	case strings.HasPrefix(path, namespacesPath(acctCluster)):
		f.serveNamespaces(w, r, strings.TrimPrefix(strings.TrimPrefix(path, namespacesPath(acctCluster)), "/"))
	default:
		http.NotFound(w, r)
	}
}

// record keeps a copy of the request, and restores the body so the handler
// can read it again.
func (f *accountsFake) record(r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	r.Body = io.NopCloser(bytes.NewReader(body))
	f.mu.Lock()
	f.requests = append(f.requests, recorded{
		method: r.Method,
		path:   r.URL.Path,
		query:  r.URL.Query(),
		header: r.Header.Clone(),
		body:   string(body),
		at:     time.Now(),
	})
	f.mu.Unlock()
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeStatus(w http.ResponseWriter, status int, reason, message string) {
	writeJSON(w, status, kubeStatus{Reason: reason, Message: message})
}

func writeList[T any](w http.ResponseWriter, items []T) {
	writeJSON(w, http.StatusOK, listEnvelope[T]{Items: items})
}

// selectorMatches reports whether labels satisfies every comma-separated part
// of selector. A part of the form key checks presence, and key=value checks
// equality. An empty selector matches everything.
func selectorMatches(labels map[string]string, selector string) bool {
	if selector == "" {
		return true
	}
	for _, part := range strings.Split(selector, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, value, hasValue := strings.Cut(part, "=")
		if hasValue {
			if labels[key] != value {
				return false
			}
		} else if _, ok := labels[key]; !ok {
			return false
		}
	}
	return true
}

// decodeBody decodes a recorded request body into T.
func decodeBody[T any](t *testing.T, body string) T {
	t.Helper()
	var v T
	if err := json.Unmarshal([]byte(body), &v); err != nil {
		t.Fatalf("decode the body %s: %v", body, err)
	}
	return v
}

func sortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// --- users and projects ---

func (f *accountsFake) serveSelf(w http.ResponseWriter) {
	f.mu.Lock()
	self := f.self
	f.mu.Unlock()
	writeJSON(w, http.StatusOK, struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}{Data: []struct {
		ID string `json:"id"`
	}{{ID: self}}})
}

func (f *accountsFake) toProject(name string, p storedProject) project {
	return project{ID: acctCluster + ":" + name, ClusterID: acctCluster, Name: name, CreatorID: p.creatorID, Created: p.created, Labels: p.labels}
}

func (f *accountsFake) serveProjectList(w http.ResponseWriter) {
	f.mu.Lock()
	items := make([]project, 0, len(f.projects))
	for _, name := range sortedKeys(f.projects) {
		items = append(items, f.toProject(name, f.projects[name]))
	}
	f.mu.Unlock()
	writeJSON(w, http.StatusOK, normanCollection[project]{Type: "collection", Data: items})
}

func (f *accountsFake) serveProjectItem(w http.ResponseWriter, idSuffix string) {
	_, name, ok := strings.Cut(idSuffix, ":")
	if !ok {
		name = idSuffix
	}
	f.mu.Lock()
	p, exists := f.projects[name]
	if !exists {
		p, exists = f.unlisted[name]
	}
	f.mu.Unlock()
	if !exists {
		writeStatus(w, http.StatusNotFound, "NotFound", "project "+name+" not found")
		return
	}
	writeJSON(w, http.StatusOK, f.toProject(name, p))
}

// createProject serves the management.cattle.io create of the account
// project. It answers with the Kubernetes-shaped object that createAccountProject
// decodes, and it stores the project so a later GET /v3/projects lists it.
func (f *accountsFake) createProject(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	var incoming object
	if err := json.Unmarshal(body, &incoming); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	f.mu.Lock()
	name := "p-acct"
	for i := 2; ; i++ {
		if _, exists := f.projects[name]; !exists {
			break
		}
		name = fmt.Sprintf("p-acct-%d", i)
	}
	f.seq++
	f.projects[name] = storedProject{
		creatorID: incoming.Metadata.Annotations[creatorAnnotation],
		created:   fmt.Sprintf("created-%03d", f.seq),
		labels:    incoming.Metadata.Labels,
	}
	f.mu.Unlock()

	writeJSON(w, http.StatusCreated, object{
		APIVersion: incoming.APIVersion,
		Kind:       incoming.Kind,
		Metadata: objectMeta{
			Name:            name,
			Namespace:       acctCluster,
			UID:             "uid-" + name,
			ResourceVersion: "1",
			Labels:          incoming.Metadata.Labels,
			Annotations:     incoming.Metadata.Annotations,
		},
		Spec: incoming.Spec,
	})
}

// --- namespaces, and the service accounts nested under one ---

func (f *accountsFake) serveNamespaces(w http.ResponseWriter, r *http.Request, rest string) {
	if rest == "" {
		switch r.Method {
		case http.MethodGet:
			f.listNamespaces(w, r)
		case http.MethodPost:
			f.createNamespace(w, r)
		default:
			http.NotFound(w, r)
		}
		return
	}

	parts := strings.SplitN(rest, "/", 2)
	if len(parts) == 2 && parts[1] == "serviceaccounts" {
		ns := parts[0]
		switch r.Method {
		case http.MethodGet:
			f.listServiceAccounts(w, r, ns)
		case http.MethodPost:
			f.createServiceAccount(w, r, ns)
		default:
			http.NotFound(w, r)
		}
		return
	}
	if len(parts) == 1 {
		name := parts[0]
		switch r.Method {
		case http.MethodGet:
			f.getNamespace(w, name)
		case http.MethodPatch:
			f.patchNamespace(w, r, name)
		case http.MethodDelete:
			f.deleteNamespace(w, name)
		default:
			http.NotFound(w, r)
		}
		return
	}
	http.NotFound(w, r)
}

func (f *accountsFake) listNamespaces(w http.ResponseWriter, r *http.Request) {
	selector := r.URL.Query().Get("labelSelector")
	f.mu.Lock()
	items := make([]namespace, 0, len(f.namespaces))
	for _, name := range sortedKeys(f.namespaces) {
		ns := f.namespaces[name]
		if selectorMatches(ns.Metadata.Labels, selector) {
			items = append(items, ns)
		}
	}
	f.mu.Unlock()
	writeList(w, items)
}

func (f *accountsFake) createNamespace(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	var ns namespace
	if err := json.Unmarshal(body, &ns); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.namespaces[ns.Metadata.Name]; exists {
		writeStatus(w, http.StatusConflict, "AlreadyExists", "namespace "+ns.Metadata.Name+" exists")
		return
	}
	if ns.Metadata.UID == "" {
		ns.Metadata.UID = "uid-" + ns.Metadata.Name
	}
	ns.Metadata.ResourceVersion = "1"
	f.namespaces[ns.Metadata.Name] = ns
	writeJSON(w, http.StatusCreated, ns)
}

func (f *accountsFake) getNamespace(w http.ResponseWriter, name string) {
	f.mu.Lock()
	ns, ok := f.namespaces[name]
	f.mu.Unlock()
	if !ok {
		writeStatus(w, http.StatusNotFound, "NotFound", "namespace "+name+" not found")
		return
	}
	writeJSON(w, http.StatusOK, ns)
}

// patchNamespace applies a merge patch of the annotations only, the shape
// that the account namespace move sends.
func (f *accountsFake) patchNamespace(w http.ResponseWriter, r *http.Request, name string) {
	body, _ := io.ReadAll(r.Body)
	var patch struct {
		Metadata struct {
			Annotations map[string]string `json:"annotations"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(body, &patch); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if status, ok := f.patchFailures[name]; ok {
		writeStatus(w, status, "InternalError", "the patch failed")
		return
	}
	ns, ok := f.namespaces[name]
	if !ok {
		writeStatus(w, http.StatusNotFound, "NotFound", "namespace "+name+" not found")
		return
	}
	if ns.Metadata.Annotations == nil {
		ns.Metadata.Annotations = make(map[string]string)
	}
	for k, v := range patch.Metadata.Annotations {
		ns.Metadata.Annotations[k] = v
	}
	f.namespaces[name] = ns
	writeJSON(w, http.StatusOK, ns)
}

func (f *accountsFake) deleteNamespace(w http.ResponseWriter, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.namespaces[name]; !ok {
		writeStatus(w, http.StatusNotFound, "NotFound", "namespace "+name+" not found")
		return
	}
	delete(f.namespaces, name)
	writeJSON(w, http.StatusOK, kubeStatus{})
}

func (f *accountsFake) listServiceAccounts(w http.ResponseWriter, r *http.Request, ns string) {
	selector := r.URL.Query().Get("labelSelector")
	f.mu.Lock()
	var items []object
	for _, name := range sortedKeys(f.serviceAccounts[ns]) {
		obj := f.serviceAccounts[ns][name]
		if selectorMatches(obj.Metadata.Labels, selector) {
			items = append(items, obj)
		}
	}
	f.mu.Unlock()
	writeList(w, items)
}

func (f *accountsFake) serveAllServiceAccounts(w http.ResponseWriter, r *http.Request) {
	selector := r.URL.Query().Get("labelSelector")
	f.mu.Lock()
	var items []object
	for _, ns := range sortedKeys(f.serviceAccounts) {
		for _, name := range sortedKeys(f.serviceAccounts[ns]) {
			obj := f.serviceAccounts[ns][name]
			if selectorMatches(obj.Metadata.Labels, selector) {
				items = append(items, obj)
			}
		}
	}
	f.mu.Unlock()
	writeList(w, items)
}

func (f *accountsFake) createServiceAccount(w http.ResponseWriter, r *http.Request, ns string) {
	body, _ := io.ReadAll(r.Body)
	var obj object
	if err := json.Unmarshal(body, &obj); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.serviceAccounts[ns] == nil {
		f.serviceAccounts[ns] = make(map[string]object)
	}
	if _, exists := f.serviceAccounts[ns][obj.Metadata.Name]; exists {
		writeStatus(w, http.StatusConflict, "AlreadyExists", "service account "+obj.Metadata.Name+" exists")
		return
	}
	obj.Metadata.Namespace = ns
	obj.Metadata.UID = "uid-" + ns + "-" + obj.Metadata.Name
	obj.Metadata.ResourceVersion = "1"
	f.serviceAccounts[ns][obj.Metadata.Name] = obj
	writeJSON(w, http.StatusCreated, obj)
}

// --- role bindings and cluster role bindings ---

func (f *accountsFake) serveAllRoleBindings(w http.ResponseWriter, r *http.Request) {
	selector := r.URL.Query().Get("labelSelector")
	f.mu.Lock()
	var items []binding
	for _, ns := range sortedKeys(f.roleBindings) {
		for _, name := range sortedKeys(f.roleBindings[ns]) {
			b := f.roleBindings[ns][name]
			if selectorMatches(b.Metadata.Labels, selector) {
				items = append(items, b)
			}
		}
	}
	f.mu.Unlock()
	writeList(w, items)
}

func (f *accountsFake) serveNamespacedRoleBindings(w http.ResponseWriter, r *http.Request, rest string) {
	parts := strings.SplitN(rest, "/", 3)
	if len(parts) < 2 || parts[1] != "rolebindings" {
		http.NotFound(w, r)
		return
	}
	ns := parts[0]
	if len(parts) == 2 {
		switch r.Method {
		case http.MethodGet:
			f.listRoleBindings(w, r, ns)
		case http.MethodPost:
			f.createBinding(w, r, ns, f.roleBindings)
		default:
			http.NotFound(w, r)
		}
		return
	}
	name := parts[2]
	switch r.Method {
	case http.MethodGet:
		f.getBinding(w, ns, name, f.roleBindings)
	case http.MethodPut:
		f.putBinding(w, r, ns, name, f.roleBindings)
	case http.MethodDelete:
		f.deleteBinding(w, ns, name, f.roleBindings)
	default:
		http.NotFound(w, r)
	}
}

func (f *accountsFake) listRoleBindings(w http.ResponseWriter, r *http.Request, ns string) {
	selector := r.URL.Query().Get("labelSelector")
	f.mu.Lock()
	var items []binding
	for _, name := range sortedKeys(f.roleBindings[ns]) {
		b := f.roleBindings[ns][name]
		if selectorMatches(b.Metadata.Labels, selector) {
			items = append(items, b)
		}
	}
	f.mu.Unlock()
	writeList(w, items)
}

func (f *accountsFake) serveClusterRoleBindings(w http.ResponseWriter, r *http.Request, rest string) {
	if rest == "" {
		switch r.Method {
		case http.MethodGet:
			f.listClusterRoleBindings(w, r)
		case http.MethodPost:
			f.createClusterRoleBinding(w, r)
		default:
			http.NotFound(w, r)
		}
		return
	}
	name := strings.TrimPrefix(rest, "/")
	switch r.Method {
	case http.MethodGet:
		f.getClusterRoleBinding(w, name)
	case http.MethodPut:
		f.putClusterRoleBinding(w, r, name)
	case http.MethodDelete:
		f.deleteClusterRoleBinding(w, name)
	default:
		http.NotFound(w, r)
	}
}

func (f *accountsFake) listClusterRoleBindings(w http.ResponseWriter, r *http.Request) {
	selector := r.URL.Query().Get("labelSelector")
	f.mu.Lock()
	var items []binding
	for _, name := range sortedKeys(f.clusterRoleBindings) {
		b := f.clusterRoleBindings[name]
		if selectorMatches(b.Metadata.Labels, selector) {
			items = append(items, b)
		}
	}
	f.mu.Unlock()
	writeList(w, items)
}

func (f *accountsFake) createClusterRoleBinding(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	var b binding
	if err := json.Unmarshal(body, &b); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.clusterRoleBindings[b.Metadata.Name]; exists {
		writeStatus(w, http.StatusConflict, "AlreadyExists", "cluster role binding "+b.Metadata.Name+" exists")
		return
	}
	b.Metadata.ResourceVersion = "1"
	f.clusterRoleBindings[b.Metadata.Name] = b
	writeJSON(w, http.StatusCreated, b)
}

func (f *accountsFake) getClusterRoleBinding(w http.ResponseWriter, name string) {
	f.mu.Lock()
	b, ok := f.clusterRoleBindings[name]
	f.mu.Unlock()
	if !ok {
		writeStatus(w, http.StatusNotFound, "NotFound", "cluster role binding "+name+" not found")
		return
	}
	writeJSON(w, http.StatusOK, b)
}

func (f *accountsFake) putClusterRoleBinding(w http.ResponseWriter, r *http.Request, name string) {
	body, _ := io.ReadAll(r.Body)
	var b binding
	if err := json.Unmarshal(body, &b); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	have, ok := f.clusterRoleBindings[name]
	if !ok {
		writeStatus(w, http.StatusNotFound, "NotFound", "cluster role binding "+name+" not found")
		return
	}
	b.Metadata.Name = name
	b.Metadata.ResourceVersion = have.Metadata.ResourceVersion + "+1"
	f.clusterRoleBindings[name] = b
	writeJSON(w, http.StatusOK, b)
}

func (f *accountsFake) deleteClusterRoleBinding(w http.ResponseWriter, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.clusterRoleBindings[name]; !ok {
		writeStatus(w, http.StatusNotFound, "NotFound", "cluster role binding "+name+" not found")
		return
	}
	delete(f.clusterRoleBindings, name)
	writeJSON(w, http.StatusOK, kubeStatus{})
}

// createBinding, getBinding, putBinding, and deleteBinding serve one
// namespace-scoped role binding collection. store is keyed by namespace, then
// by name.
func (f *accountsFake) createBinding(w http.ResponseWriter, r *http.Request, ns string, store map[string]map[string]binding) {
	body, _ := io.ReadAll(r.Body)
	var b binding
	if err := json.Unmarshal(body, &b); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	b.Metadata.Namespace = ns
	if store[ns] == nil {
		store[ns] = make(map[string]binding)
	}
	if _, exists := store[ns][b.Metadata.Name]; exists {
		writeStatus(w, http.StatusConflict, "AlreadyExists", "binding "+b.Metadata.Name+" exists")
		return
	}
	b.Metadata.ResourceVersion = "1"
	store[ns][b.Metadata.Name] = b
	writeJSON(w, http.StatusCreated, b)
}

func (f *accountsFake) getBinding(w http.ResponseWriter, ns, name string, store map[string]map[string]binding) {
	f.mu.Lock()
	b, ok := store[ns][name]
	f.mu.Unlock()
	if !ok {
		writeStatus(w, http.StatusNotFound, "NotFound", "binding "+name+" not found")
		return
	}
	writeJSON(w, http.StatusOK, b)
}

func (f *accountsFake) putBinding(w http.ResponseWriter, r *http.Request, ns, name string, store map[string]map[string]binding) {
	body, _ := io.ReadAll(r.Body)
	var b binding
	if err := json.Unmarshal(body, &b); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	have, ok := store[ns][name]
	if !ok {
		writeStatus(w, http.StatusNotFound, "NotFound", "binding "+name+" not found")
		return
	}
	b.Metadata.Namespace = ns
	b.Metadata.Name = name
	b.Metadata.ResourceVersion = have.Metadata.ResourceVersion + "+1"
	store[ns][name] = b
	writeJSON(w, http.StatusOK, b)
}

func (f *accountsFake) deleteBinding(w http.ResponseWriter, ns, name string, store map[string]map[string]binding) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := store[ns][name]; !ok {
		writeStatus(w, http.StatusNotFound, "NotFound", "binding "+name+" not found")
		return
	}
	delete(store[ns], name)
	writeJSON(w, http.StatusOK, kubeStatus{})
}
