package projectsync

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

const (
	// openbaoVerifyAfter is the time after a read or a write of a role or a
	// policy before the reconcile run reads it again. It bounds the reads to
	// one per object per 10 minutes.
	openbaoVerifyAfter = 10 * time.Minute

	openbaoEngineType = "kubernetes"
	openbaoMountsPath = "sys/mounts/"
	openbaoACLPath    = "sys/policies/acl"

	// openbaoConfigKey is the key of the config of a cluster in verified.
	openbaoConfigKey = "config"

	managementClusters = "/apis/management.cattle.io/v3/clusters/"

	// credsParameters restricts a credential request to the namespace and
	// the lifetime. The endpoint also accepts audiences, and a token with
	// another audience is valid for other services than the API server.
	credsParameters = "  allowed_parameters = {\n    \"kubernetes_namespace\" = []\n    \"ttl\" = []\n  }\n"
)

// credentialRole is a role of the Kubernetes secrets engine. The service
// writes every field that can widen or change a credential, so that a write
// resets a changed role.
type credentialRole struct {
	ServiceAccountName string   `json:"service_account_name"`
	Namespaces         []string `json:"allowed_kubernetes_namespaces"`
	NamespaceSelector  string   `json:"allowed_kubernetes_namespace_selector"`
	KubernetesRoleName string   `json:"kubernetes_role_name"`
	GeneratedRules     string   `json:"generated_role_rules"`
	Audiences          []string `json:"token_default_audiences"`
	DefaultTTL         int64    `json:"token_default_ttl"`
	MaxTTL             int64    `json:"token_max_ttl"`
}

func (w *openbaoWriter) mount(cluster string) string {
	return w.mountPrefix + "/" + cluster
}

// roleName returns the name of the role of project and role in the mount of a
// cluster.
func roleName(project string, role accountRole) string {
	return project + "-" + role.name
}

// parseRoleName returns the project and the role of a role name of the
// service, and false for another name. The project must be a label value,
// because it goes into the path of a Rancher request.
func parseRoleName(name string) (string, accountRole, bool) {
	for _, role := range accountRoles {
		project, ok := strings.CutSuffix(name, "-"+role.name)
		if ok && len(project) <= maxLabelValueLength && qualifiedName.MatchString(project) {
			return project, role, true
		}
	}
	return "", accountRole{}, false
}

// policyName returns the name of the ACL policy of project and role in
// cluster. It starts with the mount prefix, with a dash for each slash.
func (w *openbaoWriter) policyName(cluster, project string, role accountRole) string {
	return strings.ReplaceAll(w.mountPrefix, "/", "-") + "-" + cluster + "-" + project + "-" + role.name
}

// policyText returns the ACL policy that grants the credentials of project
// and role in cluster.
func (w *openbaoWriter) policyText(cluster, project string, role accountRole) string {
	return w.policyHead(cluster, project, role) + credsParameters + "}\n"
}

func (w *openbaoWriter) policyHead(cluster, project string, role accountRole) string {
	return "path \"" + w.mount(cluster) + "/creds/" + roleName(project, role) + "\" {\n  capabilities = [\"update\"]\n"
}

// ownPolicy reports whether text is the policy of project and role in
// cluster. It also accepts that policy without credsParameters, as releases
// before the restriction wrote it. The compare ignores the space around the
// text.
func (w *openbaoWriter) ownPolicy(text, cluster, project string, role accountRole) bool {
	text = strings.TrimSpace(text)
	head := w.policyHead(cluster, project, role)
	return text == strings.TrimSpace(head+credsParameters+"}") || text == strings.TrimSpace(head+"}")
}

func (w *openbaoWriter) wantRole(project string, role accountRole) credentialRole {
	return credentialRole{
		ServiceAccountName: role.name,
		Namespaces:         []string{accountNamespace(project)},
		Audiences:          []string{},
		DefaultTTL:         int64(w.credentialTTL / time.Second),
		MaxTTL:             int64(w.credentialMaxTTL / time.Second),
	}
}

func sameCredentialRole(have, want credentialRole) bool {
	return have.ServiceAccountName == want.ServiceAccountName &&
		slices.Equal(have.Namespaces, want.Namespaces) &&
		have.NamespaceSelector == want.NamespaceSelector &&
		have.KubernetesRoleName == want.KubernetesRoleName &&
		have.GeneratedRules == want.GeneratedRules &&
		len(have.Audiences) == 0 &&
		have.DefaultTTL == want.DefaultTTL && have.MaxTTL == want.MaxTTL
}

// verifyDue reports whether the object key of cluster needs a read, because
// the service did not read or write it within openbaoVerifyAfter.
func (w *openbaoWriter) verifyDue(cluster, key string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	at, ok := w.verified[cluster][key]
	return !ok || w.now().Sub(at) >= openbaoVerifyAfter
}

func (w *openbaoWriter) markVerified(cluster, key string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.verified[cluster] == nil {
		w.verified[cluster] = make(map[string]time.Time)
	}
	w.verified[cluster][key] = w.now()
}

// keepVerified forgets every object of cluster that wanted does not have.
func (w *openbaoWriter) keepVerified(cluster string, wanted map[string]bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for key := range w.verified[cluster] {
		if !wanted[key] {
			delete(w.verified[cluster], key)
		}
	}
}

func roleKey(name string) string   { return "role/" + name }
func policyKey(name string) string { return "policy/" + name }

// list returns the keys of a LIST of path. OpenBao answers 404 without a
// message to a LIST without keys.
func (w *openbaoWriter) list(ctx context.Context, path string) ([]string, error) {
	var answer struct {
		Data struct {
			Keys []string `json:"keys"`
		} `json:"data"`
	}
	err := w.call(ctx, http.MethodGet, path, url.Values{"list": []string{"true"}}, nil, &answer)
	var failure openbaoError
	if errors.As(err, &failure) && failure.status == http.StatusNotFound && len(failure.messages) == 0 {
		return nil, nil
	}
	return answer.Data.Keys, err
}

// listPolicies returns the names of the ACL policies.
func (w *openbaoWriter) listPolicies(ctx context.Context) (map[string]bool, error) {
	names, err := w.list(ctx, openbaoACLPath)
	if err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(names))
	for _, name := range names {
		out[name] = true
	}
	return out, nil
}

// ensureMount enables the Kubernetes secrets engine at the mount of cluster
// when OpenBao has no mount there. It returns true when it enabled it.
// OpenBao answers 400 with "No secret engine mount at" for a missing mount.
func (w *openbaoWriter) ensureMount(ctx context.Context, cluster string) (bool, error) {
	var answer struct {
		Data struct {
			Type string `json:"type"`
		} `json:"data"`
	}
	path := openbaoMountsPath + w.mount(cluster)
	err := w.call(ctx, http.MethodGet, path, nil, nil, &answer)
	var failure openbaoError
	switch {
	case err == nil && answer.Data.Type == openbaoEngineType:
		return false, nil
	case err == nil:
		return false, &mountTypeError{path: w.mount(cluster), kind: answer.Data.Type}
	case !errors.As(err, &failure) || failure.status != http.StatusBadRequest ||
		!slices.ContainsFunc(failure.messages, func(m string) bool { return strings.HasPrefix(m, "No secret engine mount at") }):
		return false, err
	}
	if err := w.call(ctx, http.MethodPost, path, nil, map[string]string{"type": openbaoEngineType}, nil); err != nil {
		return false, err
	}
	return true, nil
}

type mountTypeError struct{ path, kind string }

func (e *mountTypeError) Error() string {
	return "the OpenBao mount " + e.path + " has the type " + e.kind + ", not " + openbaoEngineType
}

func (w *openbaoWriter) readRole(ctx context.Context, cluster, name string) (credentialRole, error) {
	var answer struct {
		Data struct {
			ServiceAccountName string   `json:"service_account_name"`
			Namespaces         []string `json:"allowed_kubernetes_namespaces"`
			NamespaceSelector  string   `json:"allowed_kubernetes_namespace_selector"`
			KubernetesRoleName string   `json:"kubernetes_role_name"`
			GeneratedRules     string   `json:"generated_role_rules"`
			Audiences          []string `json:"token_default_audiences"`
			DefaultTTL         float64  `json:"token_default_ttl"`
			MaxTTL             float64  `json:"token_max_ttl"`
		} `json:"data"`
	}
	err := w.call(ctx, http.MethodGet, w.mount(cluster)+"/roles/"+name, nil, nil, &answer)
	d := answer.Data
	return credentialRole{
		ServiceAccountName: d.ServiceAccountName, Namespaces: d.Namespaces, NamespaceSelector: d.NamespaceSelector,
		KubernetesRoleName: d.KubernetesRoleName, GeneratedRules: d.GeneratedRules, Audiences: d.Audiences,
		DefaultTTL: int64(d.DefaultTTL), MaxTTL: int64(d.MaxTTL),
	}, err
}

func (w *openbaoWriter) writeRole(ctx context.Context, cluster, name string, role credentialRole) error {
	return w.call(ctx, http.MethodPost, w.mount(cluster)+"/roles/"+name, nil, role, nil)
}

func (w *openbaoWriter) readPolicy(ctx context.Context, name string) (string, error) {
	var answer struct {
		Data struct {
			Policy string `json:"policy"`
		} `json:"data"`
	}
	err := w.call(ctx, http.MethodGet, openbaoACLPath+"/"+name, nil, nil, &answer)
	return answer.Data.Policy, err
}

func (w *openbaoWriter) writePolicy(ctx context.Context, name, text string) error {
	return w.call(ctx, http.MethodPut, openbaoACLPath+"/"+name, nil, map[string]string{"policy": text}, nil)
}

// openbaoChanged logs and counts one write of the OpenBao state.
func (s *Syncer) openbaoChanged(ctx context.Context, cluster, kind, action, name string) {
	s.metrics.openbaoChanged(ctx, kind, action)
	s.logger.InfoContext(ctx, "OpenBao object changed", "cluster", cluster, "kind", kind, "action", action, "name", name)
}

// openbaoObjectFailure logs one failed request of the OpenBao state.
func (s *Syncer) openbaoObjectFailure(ctx context.Context, cluster, kind, verb, name string, err error) {
	s.logFailure(ctx, slog.LevelError, "the OpenBao "+kind+" "+verb+" failed", err, "cluster", cluster, "name", name)
}

// keepOpenBaoCluster keeps the OpenBao state of cluster: the mount, a role
// and a policy per project role of every tenant of target, and the config
// when due is true. known are the projects of the cluster in this run.
// policies are the names of the ACL policies, and nil when the list failed.
// It returns the count of errors.
func (s *Syncer) keepOpenBaoCluster(ctx context.Context, token, cluster string, target openbaoTarget, known map[string]project, policies map[string]bool, due bool, ca string) int {
	w := s.openbao
	created, err := w.ensureMount(ctx, cluster)
	if err != nil {
		s.openbaoObjectFailure(ctx, cluster, "mount", "request", w.mount(cluster), err)
		if due {
			s.metrics.openbaoWritten(ctx, cluster, outcomeError)
		}
		return 1
	}
	if created {
		s.openbaoChanged(ctx, cluster, "mount", "create", w.mount(cluster))
	}

	errs := 0
	roles, err := w.list(ctx, w.mount(cluster)+"/roles")
	listed := err == nil
	if err != nil {
		errs++
		s.openbaoObjectFailure(ctx, cluster, "role", "list request", w.mount(cluster), err)
	}
	wanted := make(map[string]bool)
	for _, project := range target.tenants {
		for _, role := range accountRoles {
			name := roleName(project, role)
			wanted[roleKey(name)] = true
			if listed && !s.keepRole(ctx, cluster, name, w.wantRole(project, role), slices.Contains(roles, name)) {
				errs++
			}
			policy := w.policyName(cluster, project, role)
			wanted[policyKey(policy)] = true
			if policies != nil && !s.keepPolicy(ctx, cluster, policy, w.policyText(cluster, project, role), policies[policy]) {
				errs++
			}
		}
	}
	if listed && policies != nil {
		errs += s.dropGoneCredentials(ctx, token, cluster, roles, known, policies)
	}
	wanted[openbaoConfigKey] = true
	w.keepVerified(cluster, wanted)

	if created {
		due = true
	}
	if !due && w.verifyDue(cluster, openbaoConfigKey) {
		stale, err := s.configStale(ctx, cluster, ca)
		switch {
		case err != nil:
			errs++
			s.openbaoObjectFailure(ctx, cluster, "config", "read", w.mount(cluster), err)
		case stale:
			due = true
			s.logger.InfoContext(ctx, "the OpenBao config is missing or differs", "cluster", cluster)
		default:
			w.markVerified(cluster, openbaoConfigKey)
		}
	}
	if due && !s.writeOpenBao(ctx, token, cluster, target.account, ca) {
		errs++
	}
	return errs
}

// keepRole creates the role name of cluster when the list did not have it,
// and reads and corrects it when its last read or write is older than
// openbaoVerifyAfter. It returns false on an error.
func (s *Syncer) keepRole(ctx context.Context, cluster, name string, want credentialRole, exists bool) bool {
	w := s.openbao
	action := "create"
	if exists {
		if !w.verifyDue(cluster, roleKey(name)) {
			return true
		}
		have, err := w.readRole(ctx, cluster, name)
		if err != nil {
			s.openbaoObjectFailure(ctx, cluster, "role", "read", name, err)
			return false
		}
		if sameCredentialRole(have, want) {
			w.markVerified(cluster, roleKey(name))
			return true
		}
		action = "update"
	}
	if err := w.writeRole(ctx, cluster, name, want); err != nil {
		s.openbaoObjectFailure(ctx, cluster, "role", "write", name, err)
		return false
	}
	w.markVerified(cluster, roleKey(name))
	s.openbaoChanged(ctx, cluster, "role", action, name)
	return true
}

// keepPolicy is keepRole for the ACL policy name. The compare ignores the
// space around the text.
func (s *Syncer) keepPolicy(ctx context.Context, cluster, name, text string, exists bool) bool {
	w := s.openbao
	action := "create"
	if exists {
		if !w.verifyDue(cluster, policyKey(name)) {
			return true
		}
		have, err := w.readPolicy(ctx, name)
		if err != nil {
			s.openbaoObjectFailure(ctx, cluster, "policy", "read", name, err)
			return false
		}
		if strings.TrimSpace(have) == strings.TrimSpace(text) {
			w.markVerified(cluster, policyKey(name))
			return true
		}
		action = "update"
	}
	if err := w.writePolicy(ctx, name, text); err != nil {
		s.openbaoObjectFailure(ctx, cluster, "policy", "write", name, err)
		return false
	}
	w.markVerified(cluster, policyKey(name))
	s.openbaoChanged(ctx, cluster, "policy", action, name)
	return true
}

// dropGoneCredentials deletes the role and the policy of every role name of
// roles whose project is gone. A project that the run and the project watch
// do not know counts as gone only after Rancher answers 404 for it, because
// it can be newer than the project list of the run. The policy goes first, so
// that a failed delete leaves the role, and the next run finds the pair
// again. It returns the count of errors.
func (s *Syncer) dropGoneCredentials(ctx context.Context, token, cluster string, roles []string, known map[string]project, policies map[string]bool) int {
	w := s.openbao
	errs := 0
	gone := make(map[string]bool)
	live := s.projectsOf(cluster)
	for _, name := range slices.Sorted(slices.Values(roles)) {
		project, role, ok := parseRoleName(name)
		if !ok {
			continue
		}
		if _, ok := known[project]; ok {
			continue
		}
		if _, ok := live[project]; ok {
			continue
		}
		isGone, checked := gone[project]
		if !checked {
			var err error
			if isGone, err = s.projectGone(ctx, token, cluster, project); err != nil {
				errs++
				s.logFailure(ctx, slog.LevelError, "the project request failed", err, "cluster", cluster, "project", project)
				continue
			}
			gone[project] = isGone
		}
		if !isGone {
			continue
		}

		policy := w.policyName(cluster, project, role)
		if policies[policy] {
			own, err := s.isOwnPolicy(ctx, cluster, policy, project, role)
			if err != nil {
				errs++
				continue
			}
			if own && !s.deletePolicy(ctx, cluster, policy) {
				errs++
				continue
			}
		}
		if err := w.call(ctx, http.MethodDelete, w.mount(cluster)+"/roles/"+name, nil, nil, nil); err != nil {
			errs++
			s.openbaoObjectFailure(ctx, cluster, "role", "delete", name, err)
			continue
		}
		s.openbaoChanged(ctx, cluster, "role", "delete", name)
	}
	return errs
}

// parsePolicyName returns the cluster, the project, and the role of an ACL
// policy name of the service. The cluster is the longest id of clusters that
// fits, because one cluster id can start with another. It returns false for a
// name that does not parse.
func (w *openbaoWriter) parsePolicyName(name string, clusters []string) (string, string, accountRole, bool) {
	rest, ok := strings.CutPrefix(name, strings.ReplaceAll(w.mountPrefix, "/", "-")+"-")
	if !ok {
		return "", "", accountRole{}, false
	}
	cluster := ""
	for _, candidate := range clusters {
		if len(candidate) > len(cluster) && strings.HasPrefix(rest, candidate+"-") {
			cluster = candidate
		}
	}
	if cluster == "" {
		return "", "", accountRole{}, false
	}
	project, role, ok := parseRoleName(strings.TrimPrefix(rest, cluster+"-"))
	if !ok {
		return "", "", accountRole{}, false
	}
	return cluster, project, role, true
}

// dropOrphanPolicies deletes every ACL policy of the service whose project is
// gone, also when its role is gone already. A failed write or an earlier
// writer can leave such a policy. names are the clusters of the run, and a
// policy of another cluster stays. projects are the projects of the run by
// cluster. A project that the run and the project watch do not know counts as
// gone only after Rancher answers 404 for it. dropOrphanPolicies removes each
// deleted name from policies, and returns the count of errors.
func (s *Syncer) dropOrphanPolicies(ctx context.Context, token string, names []string, projects map[string]map[string]project, policies map[string]bool) int {
	w := s.openbao
	errs := 0
	gone := make(map[string]bool)
	for _, name := range slices.Sorted(maps.Keys(policies)) {
		cluster, project, role, ok := w.parsePolicyName(name, names)
		if !ok {
			continue
		}
		if _, known := projects[cluster][project]; known {
			continue
		}
		if _, live := s.projectsOf(cluster)[project]; live {
			continue
		}
		own, err := s.isOwnPolicy(ctx, cluster, name, project, role)
		if err != nil {
			errs++
			continue
		}
		if !own {
			continue
		}
		key := cluster + ":" + project
		isGone, checked := gone[key]
		if !checked {
			var err error
			if isGone, err = s.projectGone(ctx, token, cluster, project); err != nil {
				errs++
				s.logFailure(ctx, slog.LevelError, "the project request failed", err, "cluster", cluster, "project", project)
				continue
			}
			gone[key] = isGone
		}
		if !isGone {
			continue
		}
		if !s.deletePolicy(ctx, cluster, name) {
			errs++
			continue
		}
		delete(policies, name)
	}
	return errs
}

// isOwnPolicy reads the ACL policy name, and reports whether its text is the
// policy of project and role in cluster. A name alone does not prove the
// writer, because another writer can use a name of the same form. A policy
// that OpenBao does not have is not own. isOwnPolicy logs a failed read.
func (s *Syncer) isOwnPolicy(ctx context.Context, cluster, name, project string, role accountRole) (bool, error) {
	text, err := s.openbao.readPolicy(ctx, name)
	if hasOpenBaoStatus(err, http.StatusNotFound) {
		return false, nil
	}
	if err != nil {
		s.openbaoObjectFailure(ctx, cluster, "policy", "read", name, err)
		return false, err
	}
	if !s.openbao.ownPolicy(text, cluster, project, role) {
		s.logger.DebugContext(ctx, "the OpenBao policy has another text, so the service keeps it", "cluster", cluster, "name", name)
		return false, nil
	}
	return true, nil
}

// deletePolicy deletes the ACL policy name of cluster, and logs the result.
// It returns false on an error.
func (s *Syncer) deletePolicy(ctx context.Context, cluster, name string) bool {
	if err := s.openbao.call(ctx, http.MethodDelete, openbaoACLPath+"/"+name, nil, nil, nil); err != nil {
		s.openbaoObjectFailure(ctx, cluster, "policy", "delete", name, err)
		return false
	}
	s.openbaoChanged(ctx, cluster, "policy", "delete", name)
	return true
}

// listMounts returns the cluster ids of the mounts of the Kubernetes secrets
// engine under the mount prefix, sorted. It skips an id that is not a label
// value, because the id goes into the path of a Rancher request.
func (w *openbaoWriter) listMounts(ctx context.Context) ([]string, error) {
	var answer struct {
		Data map[string]struct {
			Type string `json:"type"`
		} `json:"data"`
	}
	if err := w.call(ctx, http.MethodGet, strings.TrimSuffix(openbaoMountsPath, "/"), nil, nil, &answer); err != nil {
		return nil, err
	}
	var out []string
	for path, mount := range answer.Data {
		id, ok := strings.CutPrefix(strings.TrimSuffix(path, "/"), w.mountPrefix+"/")
		if ok && mount.Type == openbaoEngineType && len(id) <= maxLabelValueLength && qualifiedName.MatchString(id) {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out, nil
}

// clusterGone reports whether the Rancher cluster has no Cluster object with
// the id cluster. The Kubernetes API checks the right before it looks for the
// object, so a missing right gives an error, never a gone cluster.
func (s *Syncer) clusterGone(ctx context.Context, token, cluster string) (bool, error) {
	var item object
	found, err := s.getObject(ctx, token, clusterPath(rancherCluster)+managementClusters+cluster, &item)
	return !found && err == nil, err
}

// dropGoneClusters deletes the mount and the ACL policies of each cluster
// outside names after Rancher answers 404 for that cluster. names are the
// clusters of the run. A mount counts only when its config has the Rancher
// URL of that cluster, so that the service deletes no mount of another
// writer. The policies go first, so that the mount stays as the key for a
// retry. The mount delete also deletes the roles and the config of the mount.
// dropGoneClusters removes each deleted name from policies, and returns the
// count of errors.
func (s *Syncer) dropGoneClusters(ctx context.Context, token string, names []string, policies map[string]bool) int {
	w := s.openbao
	mounts, err := w.listMounts(ctx)
	if err != nil {
		s.logFailure(ctx, slog.LevelError, "the OpenBao mount list request failed", err)
		return 1
	}
	errs := 0
	for _, cluster := range mounts {
		if slices.Contains(names, cluster) {
			continue
		}
		have, found, err := w.readConfig(ctx, cluster)
		if err != nil {
			errs++
			s.openbaoObjectFailure(ctx, cluster, "config", "read", w.mount(cluster), err)
			continue
		}
		if !found || have.Host != w.wantConfig(cluster, "").Host {
			s.logger.DebugContext(ctx, "the OpenBao mount has no config of the service, so the service keeps it", "cluster", cluster, "mount", w.mount(cluster))
			continue
		}
		gone, err := s.clusterGone(ctx, token, cluster)
		if err != nil {
			errs++
			s.logFailure(ctx, slog.LevelError, "the cluster request failed", err, "cluster", cluster)
			continue
		}
		if !gone {
			continue
		}

		failed := false
		clusters := append(slices.Clone(names), cluster)
		for _, name := range slices.Sorted(maps.Keys(policies)) {
			owner, project, role, ok := w.parsePolicyName(name, clusters)
			if !ok || owner != cluster {
				continue
			}
			own, err := s.isOwnPolicy(ctx, cluster, name, project, role)
			switch {
			case err != nil || (own && !s.deletePolicy(ctx, cluster, name)):
				errs++
				failed = true
			case own:
				delete(policies, name)
			}
		}
		if failed {
			continue
		}
		if err := w.call(ctx, http.MethodDelete, openbaoMountsPath+w.mount(cluster), nil, nil, nil); err != nil {
			errs++
			s.openbaoObjectFailure(ctx, cluster, "mount", "delete", w.mount(cluster), err)
			continue
		}
		s.openbaoChanged(ctx, cluster, "mount", "delete", w.mount(cluster))
	}
	return errs
}

// writeOpenBaoProject writes the roles and the policies of project name in
// cluster, for the project watch. It writes without a read, and it skips an
// object that the service read or wrote within openbaoVerifyAfter. On the
// first failure it gives up, and the reconcile run repeats the work. A
// missing mount also waits for the reconcile run, which creates it.
func (s *Syncer) writeOpenBaoProject(ctx context.Context, cluster, name string) {
	w := s.openbao
	for _, role := range accountRoles {
		steps := []struct {
			kind, name, key string
			write           func() error
		}{
			{"role", roleName(name, role), roleKey(roleName(name, role)), func() error {
				return w.writeRole(ctx, cluster, roleName(name, role), w.wantRole(name, role))
			}},
			{"policy", w.policyName(cluster, name, role), policyKey(w.policyName(cluster, name, role)), func() error {
				return w.writePolicy(ctx, w.policyName(cluster, name, role), w.policyText(cluster, name, role))
			}},
		}
		for _, step := range steps {
			if !w.verifyDue(cluster, step.key) {
				continue
			}
			err := step.write()
			if missingMount(err) {
				s.logger.InfoContext(ctx, "the OpenBao mount does not exist yet", "cluster", cluster, "mount", w.mount(cluster))
				return
			}
			if err != nil {
				s.openbaoObjectFailure(ctx, cluster, step.kind, "write", step.name, err)
				return
			}
			w.markVerified(cluster, step.key)
			s.openbaoChanged(ctx, cluster, step.kind, "write", step.name)
		}
	}
}
