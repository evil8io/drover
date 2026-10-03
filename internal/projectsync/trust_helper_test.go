package projectsync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"strings"
	"testing"
)

const (
	// trustRulesPath is the rules file of the fixture set.
	trustRulesPath = "testdata/trust/rules.json"
	trustKey       = "drover-trust"
	trustStatusKey = "drover-trust-status"
	githubMount    = "auth/jwt/github"
)

// projectPath is the path of the Project object name of acctCluster in the
// Rancher cluster.
func projectPath(name string) string {
	return clusterPath(rancherCluster) + managementProjects + acctCluster + "/projects/" + name
}

func withTrust(path string) func(*Config) {
	return func(cfg *Config) { cfg.TrustFile = path }
}

// newTrustSetup returns an OpenBao setup with the fixture rules, the JWT
// mount of the issuer github, and the AWS mount.
func newTrustSetup(t *testing.T, fake *accountsFake, opts ...func(*Config)) *openbaoSetup {
	t.Helper()
	setup := newOpenBaoSetup(t, fake, append([]func(*Config){withTrust(trustRulesPath)}, opts...)...)
	setup.bao.addAuthMount(githubMount)
	setup.bao.addAuthMount(awsAuthPath)
	return setup
}

// trusted sets the trust annotation of a stored project to document.
func trusted(document string) func(*storedProject) {
	return func(p *storedProject) { p.annotations = map[string]string{trustKey: document} }
}

func trustHash(document string) string {
	sum := sha256.Sum256([]byte(document))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// editProject changes the stored project name.
func (f *accountsFake) editProject(name string, edit func(*storedProject)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := f.projects[name]
	p.annotations = maps.Clone(p.annotations)
	edit(&p)
	f.projects[name] = p
}

// projectAnnotation returns the annotation key of the stored project name.
func (f *accountsFake) projectAnnotation(name, key string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	value, ok := f.projects[name].annotations[key]
	return value, ok
}

// patchProject applies a merge patch of the annotations of a project, the
// shape of the trust status write. A null deletes the key.
func (f *accountsFake) patchProject(w http.ResponseWriter, r *http.Request, name string) {
	body, _ := io.ReadAll(r.Body)
	var patch struct {
		Metadata struct {
			Annotations map[string]*string `json:"annotations"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(body, &patch); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.projects[name]
	if !ok {
		writeStatus(w, http.StatusNotFound, "NotFound", "project "+name+" not found")
		return
	}
	annotations := maps.Clone(p.annotations)
	if annotations == nil {
		annotations = make(map[string]string)
	}
	for key, value := range patch.Metadata.Annotations {
		if value == nil {
			delete(annotations, key)
		} else {
			annotations[key] = *value
		}
	}
	p.annotations = annotations
	f.projects[name] = p
	writeJSON(w, http.StatusOK, object{APIVersion: "management.cattle.io/v3", Kind: "Project", Metadata: objectMeta{Name: name, Namespace: acctCluster, Annotations: annotations}})
}

// watchProject puts the stored project name into the snapshot of the syncer,
// as the project watch does after an event.
func watchProject(t *testing.T, setup *openbaoSetup, name string) {
	t.Helper()
	setup.fake.mu.Lock()
	stored := setup.fake.projects[name]
	setup.fake.mu.Unlock()
	item := setup.syncer.pruneProject(setup.fake.toProject(name, stored))
	if !setup.syncer.setProject(acctCluster, name, item) {
		t.Fatalf("the snapshot has no cluster %s", acctCluster)
	}
}

// listTrustProject runs the lister for the project name, as after a project
// event.
func listTrustProject(t *testing.T, setup *openbaoSetup, name string) {
	t.Helper()
	watchProject(t, setup, name)
	setup.syncer.listProject(context.Background(), acctCluster, name, newClusterWatch(10).patches)
}

// splitAuthRoute returns the auth mount of route and the rest of the path.
func splitAuthRoute(route string) (string, string) {
	if rest, ok := strings.CutPrefix(route, jwtAuthPrefix); ok {
		issuer, sub, _ := strings.Cut(rest, "/")
		return jwtAuthPrefix + issuer, sub
	}
	if rest, ok := strings.CutPrefix(route, awsAuthPath+"/"); ok {
		return awsAuthPath, rest
	}
	return route, ""
}

// serveLogin serves the roles of the JWT mounts under auth/jwt/ and of the
// AWS mount at auth/aws. The AWS mount lists at roles.
func (f *fakeOpenBao) serveLogin(w http.ResponseWriter, r *http.Request, route string, list bool, body []byte) {
	mount, rest := splitAuthRoute(route)
	if !f.authMounts[mount] {
		f.noHandler(w, route)
		return
	}
	switch {
	case list && mount+"/"+rest == listPath(mount):
		f.writeKeys(w, sortedKeys(f.loginRoles[mount]))
	case strings.HasPrefix(rest, "role/"):
		name := strings.TrimPrefix(rest, "role/")
		switch r.Method {
		case http.MethodGet:
			data, ok := f.loginRoles[mount][name]
			if !ok {
				writeJSON(w, http.StatusNotFound, map[string][]string{"errors": {}})
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"data": data})
		case http.MethodPost, http.MethodPut:
			var data map[string]any
			_ = json.Unmarshal(body, &data)
			if f.loginRoles[mount] == nil {
				f.loginRoles[mount] = make(map[string]map[string]any)
			}
			f.loginRoles[mount][name] = data
			w.WriteHeader(http.StatusNoContent)
		case http.MethodDelete:
			delete(f.loginRoles[mount], name)
			w.WriteHeader(http.StatusNoContent)
		}
	default:
		f.noHandler(w, route)
	}
}

func (f *fakeOpenBao) addAuthMount(mount string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.authMounts[mount] = true
}

func (f *fakeOpenBao) login(mount, name string) (map[string]any, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.loginRoles[mount][name]
	return data, ok
}

func (f *fakeOpenBao) setLogin(mount, name string, data map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.loginRoles[mount] == nil {
		f.loginRoles[mount] = make(map[string]map[string]any)
	}
	f.loginRoles[mount][name] = data
}

func (f *fakeOpenBao) loginNames(mount string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return sortedKeys(f.loginRoles[mount])
}

// loginRequests returns the requests of the auth mounts of the login roles,
// without the login of the service.
func (f *fakeOpenBao) loginRequests() []recorded {
	var out []recorded
	for _, req := range f.all() {
		if strings.HasPrefix(req.path, "/v1/auth/") && req.path != "/v1/auth/kubernetes/login" {
			out = append(out, req)
		}
	}
	return out
}
