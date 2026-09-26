package projectsync

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/evil8io/drover/internal/rancherclient"
)

const (
	// accountProjectLabel marks the Project that holds the account namespaces
	// of a cluster. A project owner can set it on its own project, so the
	// service trusts it only together with the creator annotation, see
	// isAccountProject.
	accountProjectLabel = "drover-service-accounts"
	// accountProjectName is the display name of a new account project.
	accountProjectName = "drover"

	// accountProjectKey and accountRoleKey label every object of the
	// accounts: the tenant project name, and the role.
	accountProjectKey = "drover-project"
	accountRoleKey    = "drover-role"

	// accountPrefix starts the name of an account namespace, a role binding,
	// and a cluster role binding of the service.
	accountPrefix = "drover-"

	creatorAnnotation   = "field.cattle.io/creatorId"
	projectAnnotation   = "field.cattle.io/projectId"
	systemProjectLabel  = "authz.management.cattle.io/system-project"
	defaultProjectLabel = "authz.management.cattle.io/default-project"

	// createNamespaceRole is the ClusterRole of Rancher that grants the
	// namespace create.
	createNamespaceRole = "create-ns"

	usersPath          = "/v3/users"
	managementProjects = "/apis/management.cattle.io/v3/namespaces/"
)

// accountRole is one role of a project, with one ServiceAccount.
type accountRole struct {
	// name is the name of the role template of Rancher, and of the
	// ServiceAccount.
	name string
	// clusterRole is the ClusterRole that the role binding in a project
	// namespace grants.
	clusterRole string
	// namespaces is the suffix of the ClusterRole <project>-<suffix> that
	// Rancher keeps for the namespaces of a project.
	namespaces string
	// createNamespaces adds a binding to the ClusterRole create-ns.
	createNamespaces bool
}

var accountRoles = []accountRole{
	{name: "project-owner", clusterRole: "admin", namespaces: "namespaces-edit", createNamespaces: true},
	{name: "project-member", clusterRole: "edit", namespaces: "namespaces-edit", createNamespaces: true},
	{name: "read-only", clusterRole: "view", namespaces: "namespaces-readonly"},
}

// accountState is the account view of one cluster.
type accountState struct {
	// projects are the account projects of the cluster, oldest first. A new
	// account namespace goes into the first one.
	projects []string
	// namespaces maps the name of a tenant project to the uid of its account
	// namespace.
	namespaces map[string]string
	// settled maps a namespace name to the key that the worker compares: the
	// project and the uid of its account namespace, from the last run. The
	// worker skips a namespace whose key is unchanged since that run, so a
	// watch replay after a restart costs no request.
	settled map[string]string
}

// trust is the outcome of the check of an account namespace.
type trust int

const (
	// trustUnknown skips the project in this pass, and a later pass checks it
	// again.
	trustUnknown trust = iota
	trustYes
	trustNo
)

func accountNamespace(project string) string {
	return accountPrefix + project
}

func roleBindingName(role accountRole) string {
	return accountPrefix + role.name
}

func namespacesBindingName(project string, role accountRole) string {
	return accountPrefix + project + "-" + role.name + "-namespaces"
}

func createBindingName(project string, role accountRole) string {
	return accountPrefix + project + "-" + role.name + "-create-ns"
}

// isAccountProject reports whether item is an account project of the service
// user self. The webhook of Rancher makes the creator annotation immutable. A
// cluster member can create a project with the service user as creator, but
// Rancher then binds only the service user to that project, so the member
// gets no role in it.
func isAccountProject(item project, self string) bool {
	return item.marked && self != "" && item.CreatorID == self
}

// isTenant reports whether the project named name gets accounts: not the
// System project, not the Default project, and not an account project.
func isTenant(item project, name, self string) bool {
	if item.reserved || isAccountProject(item, self) {
		return false
	}
	return len(name) <= maxLabelValueLength && qualifiedName.MatchString(name)
}

// accountProjects returns the names of the account projects in projects,
// oldest first.
func accountProjects(projects map[string]project, self string) []string {
	var names []string
	for name, item := range projects {
		if isAccountProject(item, self) {
			names = append(names, name)
		}
	}
	slices.SortFunc(names, func(a, b string) int {
		return cmp.Or(cmp.Compare(projects[a].Created, projects[b].Created), cmp.Compare(a, b))
	})
	return names
}

// projectOf returns the project name of a namespace of cluster, from the
// project annotation. Rancher sets the project label from the annotation, but
// it keeps the label when the annotation goes, and it grants the project roles
// by the annotation.
func projectOf(item namespace, cluster string) string {
	owner, name, ok := strings.Cut(item.Metadata.Annotations[projectAnnotation], ":")
	if !ok || owner != cluster {
		return ""
	}
	return name
}

// selfID returns the id of the service user. It asks Rancher once, and the
// answer stays for the life of the process.
func (s *Syncer) selfID(ctx context.Context, token string) (string, error) {
	s.selfMu.Lock()
	defer s.selfMu.Unlock()
	if s.self != "" {
		return s.self, nil
	}

	status, body, err := s.do(ctx, http.MethodGet, s.target(usersPath, url.Values{"me": []string{"true"}}), token, "", nil)
	if err != nil {
		return "", err
	}
	if status != http.StatusOK {
		return "", newStatusError(http.MethodGet, usersPath, status, body)
	}
	var answer struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &answer); err != nil {
		return "", fmt.Errorf("decode the user list: %w", err)
	}
	if len(answer.Data) != 1 || answer.Data[0].ID == "" {
		return "", fmt.Errorf("the user list has %d users, not the service user only", len(answer.Data))
	}
	s.self = answer.Data[0].ID
	return s.self, nil
}

// cachedSelf returns the id of the service user, or "" before the first
// reconcile run.
func (s *Syncer) cachedSelf() string {
	s.selfMu.Lock()
	defer s.selfMu.Unlock()
	return s.self
}

// accountsOf returns the account view of cluster, and false before the first
// run of that cluster.
func (s *Syncer) accountsOf(cluster string) (accountState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state, ok := s.accounts[cluster]
	return state, ok
}

// setAccounts replaces the account view of cluster.
func (s *Syncer) setAccounts(cluster string, state accountState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := maps.Clone(s.accounts)
	if next == nil {
		next = make(map[string]accountState)
	}
	next[cluster] = state
	s.accounts = next
}

// setAccountNamespace stores the uid of the account namespace of a project in
// the view of cluster. An empty uid removes the project. A cluster without a
// view keeps none.
func (s *Syncer) setAccountNamespace(cluster, name, uid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.accounts[cluster]
	if !ok || state.namespaces[name] == uid {
		return
	}
	namespaces := maps.Clone(state.namespaces)
	if namespaces == nil {
		namespaces = make(map[string]string)
	}
	if uid == "" {
		delete(namespaces, name)
	} else {
		namespaces[name] = uid
	}
	next := maps.Clone(s.accounts)
	next[cluster] = accountState{projects: state.projects, namespaces: namespaces, settled: state.settled}
	s.accounts = next
}

// settledKey is the key of the worker cache for a namespace of project name,
// whose account namespace has uid.
func settledKey(name, uid string) string {
	return name + "/" + uid
}

// accountFailure logs a failure of the accounts, and counts it in run when
// run is not nil. A failure that skippable accepts is a debug line.
func (s *Syncer) accountFailure(ctx context.Context, run *counters, message string, err error, attrs ...any) {
	if skippable(err) {
		s.logger.DebugContext(ctx, message, append(attrs, "error", err.Error())...)
		return
	}
	if run != nil {
		run.errors++
	}
	s.logFailure(ctx, slog.LevelWarn, message, err, attrs...)
}

// accountChanged logs and counts one write of the accounts.
func (s *Syncer) accountChanged(ctx context.Context, run *counters, cluster, kind, action, namespace, name, project string) {
	if run != nil {
		run.accounts++
	}
	s.metrics.accountChanged(ctx, kind, action)
	s.logger.InfoContext(ctx, "account object changed",
		"cluster", cluster, "kind", kind, "action", action,
		"namespace", namespace, "name", name, "project", project)
}

// syncAccounts reconciles the accounts of every project of one cluster.
// namespaces are the namespaces of the cluster with a project label.
func (s *Syncer) syncAccounts(ctx context.Context, token, cluster string, projects map[string]project, namespaces []namespace, run *counters) {
	self, err := s.selfID(ctx, token)
	if err != nil {
		s.accountFailure(ctx, run, "the user request failed", err, "cluster", cluster)
		return
	}

	owners := accountProjects(projects, self)
	if len(owners) == 0 {
		name, err := s.createAccountProject(ctx, token, cluster, self)
		if err != nil {
			s.accountFailure(ctx, run, "the account project create failed", err, "cluster", cluster)
			return
		}
		s.accountChanged(ctx, run, cluster, "project", "create", "", name, "")
		owners = []string{name}
	}

	accounts, err := listAll(ctx, s, token, serviceAccountsPath(cluster, ""), accountProjectKey, pruneObject)
	if err != nil {
		s.accountFailure(ctx, run, "the service account list request failed", err, "cluster", cluster)
		return
	}
	clusterBindings, err := listAll(ctx, s, token, clusterRoleBindingsPath(cluster), accountProjectKey, pruneBinding)
	if err != nil {
		s.accountFailure(ctx, run, "the cluster role binding list request failed", err, "cluster", cluster)
		return
	}
	roleBindings, err := listAll(ctx, s, token, roleBindingsPath(cluster, ""), accountRoleKey, pruneBinding)
	if err != nil {
		s.accountFailure(ctx, run, "the role binding list request failed", err, "cluster", cluster)
		return
	}

	byName := make(map[string]namespace, len(namespaces))
	members := make(map[string][]namespace)
	for _, item := range namespaces {
		byName[item.Metadata.Name] = item
		name := projectOf(item, cluster)
		members[name] = append(members[name], item)
	}
	accountsIn := groupBy(accounts, func(item object) string { return item.Metadata.Namespace })
	clusterBindingsOf := groupBy(clusterBindings, func(item binding) string { return item.Metadata.Labels[accountProjectKey] })
	roleBindingsIn := groupBy(roleBindings, func(item binding) string { return item.Metadata.Namespace })
	roleBindingsOf := groupBy(roleBindings, func(item binding) string { return item.Metadata.Labels[accountProjectKey] })

	trusted := make(map[string]string)
	pending := make(map[string]bool)
	for _, name := range slices.Sorted(maps.Keys(projects)) {
		if !isTenant(projects[name], name, self) {
			continue
		}
		item, listed := byName[accountNamespace(name)]
		uid, outcome := s.checkAccountNamespace(ctx, token, cluster, name, owners, item, listed, clusterBindingsOf[name], true, run)
		switch outcome {
		case trustUnknown:
			pending[name] = true
			continue
		case trustNo:
			s.dropAccountBindings(ctx, token, cluster, name, clusterBindingsOf[name], roleBindingsOf[name], run)
			continue
		}
		trusted[name] = uid
		s.ensureAccounts(ctx, token, cluster, name, accountsIn[accountNamespace(name)], run)
		s.ensureClusterBindings(ctx, token, cluster, name, uid, clusterBindingsOf[name], run)
		for _, member := range members[name] {
			if member.Metadata.DeletionTimestamp != "" {
				continue
			}
			s.ensureRoleBindings(ctx, token, cluster, member.Metadata.Name, name, uid, roleBindingsIn[member.Metadata.Name], run)
		}
	}
	// The lister can reach a project that is newer than the project list of
	// this run. The project watch removes a deleted project from the snapshot,
	// so the sweep still finds its account namespace.
	if previous, ok := s.accountsOf(cluster); ok {
		live := s.projectsOf(cluster)
		for name, uid := range previous.namespaces {
			if _, known := projects[name]; known {
				continue
			}
			if _, ok := live[name]; ok {
				trusted[name] = uid
			}
		}
	}

	s.dropStrayRoleBindings(ctx, token, cluster, projects, byName, trusted, pending, roleBindings, run)

	settled := make(map[string]string, len(namespaces))
	for _, item := range namespaces {
		name := projectOf(item, cluster)
		source, known := projects[name]
		if !known {
			continue
		}
		if uid, ok := trusted[name]; ok {
			settled[item.Metadata.Name] = settledKey(name, uid)
		} else if !isTenant(source, name, self) {
			settled[item.Metadata.Name] = settledKey(name, "")
		}
	}
	s.setAccounts(cluster, accountState{projects: owners, namespaces: trusted, settled: settled})

	s.sweepAccountNamespaces(ctx, token, cluster, projects, self, owners, namespaces, trusted, clusterBindingsOf, roleBindingsOf, run)
}

// groupBy returns items by the key that key returns.
func groupBy[T any](items []T, key func(T) string) map[string][]T {
	out := make(map[string][]T)
	for _, item := range items {
		k := key(item)
		out[k] = append(out[k], item)
	}
	return out
}

// createAccountProject creates the account project of cluster, with the
// service user as creator. Rancher then binds the service user as its owner,
// and no tenant.
func (s *Syncer) createAccountProject(ctx context.Context, token, cluster, self string) (string, error) {
	body := object{
		APIVersion: "management.cattle.io/v3",
		Kind:       "Project",
		Metadata: objectMeta{
			GenerateName: "p-",
			Namespace:    cluster,
			Labels:       map[string]string{accountProjectLabel: "true"},
			Annotations:  map[string]string{creatorAnnotation: self},
		},
		Spec: map[string]string{
			"clusterName": cluster,
			"displayName": accountProjectName,
			"description": "The ServiceAccounts that drover keeps for every project.",
		},
	}
	var created object
	path := clusterPath(rancherCluster) + managementProjects + cluster + "/projects"
	if err := s.send(ctx, token, http.MethodPost, path, body, &created); err != nil {
		return "", err
	}
	return created.Metadata.Name, nil
}

// checkAccountNamespace returns the uid of the account namespace of project
// name, and whether the service may use it. item is that namespace from the
// list, when listed is true. The service uses the namespace only in an account
// project, because a tenant can create the name first in its own project. A
// missing namespace is created. With adopt, a namespace in no project moves
// back into the first account project when a cluster role binding of the
// project names it as owner. Only the service and an admin write cluster role
// bindings, so that owner reference proves that the namespace was an account
// namespace before.
func (s *Syncer) checkAccountNamespace(ctx context.Context, token, cluster, name string, owners []string, item namespace, listed bool, clusterBindings []binding, adopt bool, run *counters) (string, trust) {
	nsName := accountNamespace(name)
	if !listed {
		found, err := s.getObject(ctx, token, namespacePath(cluster, nsName), &item)
		if err != nil {
			s.accountFailure(ctx, run, "the account namespace request failed", err, "cluster", cluster, "project", name)
			return "", trustUnknown
		}
		if !found {
			return s.createAccountNamespace(ctx, token, cluster, name, owners[0], run)
		}
	}
	if item.Metadata.DeletionTimestamp != "" {
		return "", trustUnknown
	}

	owner := projectOf(item, cluster)
	if slices.Contains(owners, owner) {
		return item.Metadata.UID, trustYes
	}
	if owner != "" {
		s.logger.WarnContext(ctx, "the account namespace is in another project, and the project gets no accounts",
			"cluster", cluster, "project", name, "namespace", nsName, "namespace_project", owner)
		return "", trustNo
	}
	if !adopt {
		return "", trustUnknown
	}
	if !ownsBindings(item, clusterBindings) {
		s.logger.WarnContext(ctx, "the account namespace has no project and no binding of the service, and the project gets no accounts",
			"cluster", cluster, "project", name, "namespace", nsName)
		return "", trustNo
	}

	body := map[string]any{"metadata": map[string]any{"annotations": map[string]string{projectAnnotation: cluster + ":" + owners[0]}}}
	if err := s.send(ctx, token, http.MethodPatch, namespacePath(cluster, nsName), body, nil); err != nil {
		s.accountFailure(ctx, run, "the account namespace move failed", err, "cluster", cluster, "project", name)
		return "", trustUnknown
	}
	s.accountChanged(ctx, run, cluster, "namespace", "move", "", nsName, name)
	return item.Metadata.UID, trustYes
}

// ownsBindings reports whether a cluster role binding names item as owner, by
// uid.
func ownsBindings(item namespace, clusterBindings []binding) bool {
	for _, b := range clusterBindings {
		for _, ref := range b.Metadata.OwnerReferences {
			if ref.Kind == "Namespace" && ref.Name == item.Metadata.Name && ref.UID == item.Metadata.UID {
				return true
			}
		}
	}
	return false
}

func (s *Syncer) createAccountNamespace(ctx context.Context, token, cluster, name, owner string, run *counters) (string, trust) {
	nsName := accountNamespace(name)
	body := object{
		APIVersion: "v1",
		Kind:       "Namespace",
		Metadata: objectMeta{
			Name:        nsName,
			Labels:      map[string]string{accountProjectKey: name},
			Annotations: map[string]string{projectAnnotation: cluster + ":" + owner},
		},
	}
	var created object
	if err := s.send(ctx, token, http.MethodPost, namespacesPath(cluster), body, &created); err != nil {
		s.accountFailure(ctx, run, "the account namespace create failed", err, "cluster", cluster, "project", name)
		return "", trustUnknown
	}
	s.accountChanged(ctx, run, cluster, "namespace", "create", "", nsName, name)
	return created.Metadata.UID, trustYes
}

// ensureAccounts creates the ServiceAccounts of project name that existing
// does not have.
func (s *Syncer) ensureAccounts(ctx context.Context, token, cluster, name string, existing []object, run *counters) {
	nsName := accountNamespace(name)
	for _, role := range accountRoles {
		if slices.ContainsFunc(existing, func(item object) bool { return item.Metadata.Name == role.name }) {
			continue
		}
		body := object{
			APIVersion: "v1",
			Kind:       "ServiceAccount",
			Metadata: objectMeta{
				Name:      role.name,
				Namespace: nsName,
				Labels:    accountLabels(name, role),
			},
		}
		err := s.send(ctx, token, http.MethodPost, serviceAccountsPath(cluster, nsName), body, nil)
		if hasStatus(err, http.StatusConflict) {
			continue
		}
		if err != nil {
			s.accountFailure(ctx, run, "the service account create failed", err, "cluster", cluster, "project", name, "name", role.name)
			continue
		}
		s.accountChanged(ctx, run, cluster, "serviceaccount", "create", nsName, role.name, name)
	}
}

func accountLabels(project string, role accountRole) map[string]string {
	return map[string]string{accountProjectKey: project, accountRoleKey: role.name}
}

// accountBinding returns a binding of kind to the ClusterRole clusterRole, for
// the ServiceAccount of role in the account namespace of project. The account
// namespace with uid owns it, so the garbage collector deletes it with that
// namespace.
func accountBinding(kind, name, namespace, clusterRole, project, uid string, role accountRole) binding {
	nsName := accountNamespace(project)
	return binding{
		APIVersion: rbacAPIVersion,
		Kind:       kind,
		Metadata: objectMeta{
			Name:            name,
			Namespace:       namespace,
			Labels:          accountLabels(project, role),
			OwnerReferences: []ownerReference{{APIVersion: "v1", Kind: "Namespace", Name: nsName, UID: uid}},
		},
		RoleRef:  roleRef{APIGroup: rbacGroup, Kind: "ClusterRole", Name: clusterRole},
		Subjects: []subject{{Kind: "ServiceAccount", Name: role.name, Namespace: nsName}},
	}
}

// ensureClusterBindings reconciles the cluster role bindings of project name.
func (s *Syncer) ensureClusterBindings(ctx context.Context, token, cluster, name, uid string, existing []binding, run *counters) {
	collection := clusterRoleBindingsPath(cluster)
	for _, role := range accountRoles {
		wants := []binding{accountBinding("ClusterRoleBinding", namespacesBindingName(name, role), "", name+"-"+role.namespaces, name, uid, role)}
		if role.createNamespaces {
			wants = append(wants, accountBinding("ClusterRoleBinding", createBindingName(name, role), "", createNamespaceRole, name, uid, role))
		}
		for _, want := range wants {
			s.reconcileBinding(ctx, token, cluster, collection, want, findBinding(existing, want.Metadata.Name), name, run)
		}
	}
}

// ensureRoleBindings reconciles the role bindings of project name in the
// namespace nsName. existing are the bindings with the role label in it.
func (s *Syncer) ensureRoleBindings(ctx context.Context, token, cluster, nsName, name, uid string, existing []binding, run *counters) {
	collection := roleBindingsPath(cluster, nsName)
	for _, role := range accountRoles {
		want := accountBinding("RoleBinding", roleBindingName(role), nsName, role.clusterRole, name, uid, role)
		s.reconcileBinding(ctx, token, cluster, collection, want, findBinding(existing, want.Metadata.Name), name, run)
	}
}

func findBinding(items []binding, name string) *binding {
	for i := range items {
		if items[i].Metadata.Name == name {
			return &items[i]
		}
	}
	return nil
}

// reconcileBinding makes the binding at collection equal to want. have is the
// listed binding of that name, or nil. A binding with another role reference
// is deleted and created again, because the role reference is immutable. A
// name that another writer took without the label of the service answers 409
// on the create, and the service then reads and corrects it.
func (s *Syncer) reconcileBinding(ctx context.Context, token, cluster, collection string, want binding, have *binding, project string, run *counters) {
	kind := strings.ToLower(want.Kind)
	path := collection + "/" + want.Metadata.Name
	attrs := []any{"cluster", cluster, "project", project, "namespace", want.Metadata.Namespace, "name", want.Metadata.Name}

	if have == nil {
		err := s.send(ctx, token, http.MethodPost, collection, want, nil)
		if err == nil {
			s.accountChanged(ctx, run, cluster, kind, "create", want.Metadata.Namespace, want.Metadata.Name, project)
			return
		}
		if !hasStatus(err, http.StatusConflict) {
			s.accountFailure(ctx, run, "the "+kind+" create failed", err, attrs...)
			return
		}
		var got binding
		found, err := s.getObject(ctx, token, path, &got)
		if err != nil || !found {
			if err != nil {
				s.accountFailure(ctx, run, "the "+kind+" request failed", err, attrs...)
			}
			return
		}
		got = pruneBinding(got)
		have = &got
	}

	if have.RoleRef != want.RoleRef {
		if err := s.deleteObject(ctx, token, path); err != nil {
			s.accountFailure(ctx, run, "the "+kind+" delete failed", err, attrs...)
			return
		}
		s.accountChanged(ctx, run, cluster, kind, "delete", want.Metadata.Namespace, want.Metadata.Name, project)
		if err := s.send(ctx, token, http.MethodPost, collection, want, nil); err != nil {
			s.accountFailure(ctx, run, "the "+kind+" create failed", err, attrs...)
			return
		}
		s.accountChanged(ctx, run, cluster, kind, "create", want.Metadata.Namespace, want.Metadata.Name, project)
		return
	}
	if sameBinding(*have, want) {
		return
	}
	want.Metadata.ResourceVersion = have.Metadata.ResourceVersion
	if err := s.send(ctx, token, http.MethodPut, path, want, nil); err != nil {
		s.accountFailure(ctx, run, "the "+kind+" update failed", err, attrs...)
		return
	}
	s.accountChanged(ctx, run, cluster, kind, "update", want.Metadata.Namespace, want.Metadata.Name, project)
}

// sameBinding reports whether have has the subjects, the owner references,
// and the labels of want.
func sameBinding(have, want binding) bool {
	return slices.Equal(have.Subjects, want.Subjects) &&
		slices.Equal(have.Metadata.OwnerReferences, want.Metadata.OwnerReferences) &&
		maps.Equal(have.Metadata.Labels, want.Metadata.Labels)
}

// managedRoleBinding reports whether item is a role binding of the service:
// its name follows from the role in its label.
func managedRoleBinding(item binding) bool {
	name := item.Metadata.Labels[accountRoleKey]
	return slices.ContainsFunc(accountRoles, func(role accountRole) bool {
		return role.name == name && item.Metadata.Name == roleBindingName(role)
	})
}

// managedClusterBinding reports whether item is a cluster role binding of the
// service for project name.
func managedClusterBinding(item binding, name string) bool {
	return slices.ContainsFunc(accountRoles, func(role accountRole) bool {
		return item.Metadata.Name == namespacesBindingName(name, role) ||
			(role.createNamespaces && item.Metadata.Name == createBindingName(name, role))
	})
}

// dropAccountBindings deletes the bindings of project name. The service calls
// it when the project may not use its account namespace, so that no binding
// grants a right to a ServiceAccount of that name in a namespace of a tenant.
func (s *Syncer) dropAccountBindings(ctx context.Context, token, cluster, name string, clusterBindings, roleBindings []binding, run *counters) {
	for _, item := range clusterBindings {
		if managedClusterBinding(item, name) {
			s.dropBinding(ctx, token, cluster, clusterRoleBindingsPath(cluster), item, name, run)
		}
	}
	for _, item := range roleBindings {
		if managedRoleBinding(item) {
			s.dropBinding(ctx, token, cluster, roleBindingsPath(cluster, item.Metadata.Namespace), item, name, run)
		}
	}
}

func (s *Syncer) dropBinding(ctx context.Context, token, cluster, collection string, item binding, project string, run *counters) {
	kind := "clusterrolebinding"
	if item.Metadata.Namespace != "" {
		kind = "rolebinding"
	}
	if err := s.deleteObject(ctx, token, collection+"/"+item.Metadata.Name); err != nil {
		s.accountFailure(ctx, run, "the "+kind+" delete failed", err,
			"cluster", cluster, "project", project, "namespace", item.Metadata.Namespace, "name", item.Metadata.Name)
		return
	}
	s.accountChanged(ctx, run, cluster, kind, "delete", item.Metadata.Namespace, item.Metadata.Name, project)
}

// dropStrayRoleBindings deletes the role bindings of the service in a
// namespace that left its project, or whose project gets no accounts. A
// namespace of a project that the run does not know stays, because a project
// can be newer than the project list of the run. A namespace of a pending
// project stays too, because the check of its account namespace failed in
// this run.
func (s *Syncer) dropStrayRoleBindings(ctx context.Context, token, cluster string, projects map[string]project, byName map[string]namespace, trusted map[string]string, pending map[string]bool, roleBindings []binding, run *counters) {
	lost := make(map[string]bool)
	for _, item := range roleBindings {
		if !managedRoleBinding(item) {
			continue
		}
		nsName := item.Metadata.Namespace
		if member, listed := byName[nsName]; listed {
			name := projectOf(member, cluster)
			if _, ok := trusted[name]; ok || pending[name] {
				continue
			}
			if _, known := projects[name]; !known && name != "" {
				continue
			}
		} else {
			gone, checked := lost[nsName]
			if !checked {
				gone = s.lostProject(ctx, token, cluster, nsName, run)
				lost[nsName] = gone
			}
			if !gone {
				continue
			}
		}
		s.dropBinding(ctx, token, cluster, roleBindingsPath(cluster, nsName), item, item.Metadata.Labels[accountProjectKey], run)
	}
}

// lostProject reports whether the namespace nsName exists, is not in
// Terminating, and has no project.
func (s *Syncer) lostProject(ctx context.Context, token, cluster, nsName string, run *counters) bool {
	var item namespace
	found, err := s.getObject(ctx, token, namespacePath(cluster, nsName), &item)
	if err != nil {
		s.accountFailure(ctx, run, "the namespace request failed", err, "cluster", cluster, "namespace", nsName)
		return false
	}
	return found && item.Metadata.DeletionTimestamp == "" && projectOf(item, cluster) == ""
}

// sweepAccountNamespaces deletes an account namespace whose project gets no
// accounts or no longer exists. A project that the run does not know counts
// as gone only after Rancher answers 404 for it, because it can be newer than
// the project list of the run. The bindings go first, so that no binding
// names a ServiceAccount of a namespace that a tenant could create next.
func (s *Syncer) sweepAccountNamespaces(ctx context.Context, token, cluster string, projects map[string]project, self string, owners []string, namespaces []namespace, trusted map[string]string, clusterBindingsOf, roleBindingsOf map[string][]binding, run *counters) {
	for _, item := range namespaces {
		if !slices.Contains(owners, projectOf(item, cluster)) || item.Metadata.DeletionTimestamp != "" {
			continue
		}
		name, ok := strings.CutPrefix(item.Metadata.Name, accountPrefix)
		if !ok || name == "" {
			continue
		}
		if _, ok := trusted[name]; ok {
			continue
		}
		if source, known := projects[name]; known {
			if isTenant(source, name, self) {
				continue
			}
		} else {
			gone, err := s.projectGone(ctx, token, cluster, name)
			if err != nil {
				s.accountFailure(ctx, run, "the project request failed", err, "cluster", cluster, "project", name)
				continue
			}
			if !gone {
				continue
			}
		}

		s.dropAccountBindings(ctx, token, cluster, name, clusterBindingsOf[name], roleBindingsOf[name], run)
		if err := s.deleteObject(ctx, token, namespacePath(cluster, item.Metadata.Name)); err != nil {
			s.accountFailure(ctx, run, "the account namespace delete failed", err, "cluster", cluster, "project", name)
			continue
		}
		s.accountChanged(ctx, run, cluster, "namespace", "delete", "", item.Metadata.Name, name)
	}
}

// projectGone reports whether Rancher answers 404 for the project name of
// cluster.
func (s *Syncer) projectGone(ctx context.Context, token, cluster, name string) (bool, error) {
	path := projectsPath + "/" + cluster + ":" + name
	status, body, err := s.do(ctx, http.MethodGet, s.target(path, nil), token, "", nil)
	if err != nil {
		return false, err
	}
	switch status {
	case http.StatusOK:
		return false, nil
	case http.StatusNotFound:
		return true, nil
	}
	return false, newStatusError(http.MethodGet, path, status, body)
}

// ensureProjectAccounts brings the accounts of one project up to date after a
// project event: the account namespace, the ServiceAccounts, and the cluster
// role bindings. The worker then handles the role bindings of each namespace
// of the project. A namespace in no project waits for the reconcile run,
// because only the run knows the account projects for certain.
func (s *Syncer) ensureProjectAccounts(ctx context.Context, token, cluster, name string) {
	state, ok := s.accountsOf(cluster)
	if !ok || len(state.projects) == 0 {
		return
	}
	source, known := s.projectsOf(cluster)[name]
	if !known || !isTenant(source, name, s.cachedSelf()) {
		return
	}

	clusterBindings, err := listAll(ctx, s, token, clusterRoleBindingsPath(cluster), accountProjectKey+"="+name, pruneBinding)
	if err != nil {
		s.accountFailure(ctx, nil, "the cluster role binding list request failed", err, "cluster", cluster, "project", name)
		return
	}
	uid, outcome := s.checkAccountNamespace(ctx, token, cluster, name, state.projects, namespace{}, false, clusterBindings, false, nil)
	switch outcome {
	case trustUnknown:
		return
	case trustNo:
		roleBindings, err := listAll(ctx, s, token, roleBindingsPath(cluster, ""), accountProjectKey+"="+name, pruneBinding)
		if err != nil {
			s.accountFailure(ctx, nil, "the role binding list request failed", err, "cluster", cluster, "project", name)
			return
		}
		s.dropAccountBindings(ctx, token, cluster, name, clusterBindings, roleBindings, nil)
		s.setAccountNamespace(cluster, name, "")
		return
	}

	accounts, err := listAll(ctx, s, token, serviceAccountsPath(cluster, accountNamespace(name)), accountProjectKey, pruneObject)
	if err != nil {
		s.accountFailure(ctx, nil, "the service account list request failed", err, "cluster", cluster, "project", name)
		return
	}
	s.ensureAccounts(ctx, token, cluster, name, accounts, nil)
	s.ensureClusterBindings(ctx, token, cluster, name, uid, clusterBindings, nil)
	s.setAccountNamespace(cluster, name, uid)
}

// applyAccounts brings the role bindings of one namespace of the queue up to
// date. seen is the cache of the worker: the project and the account
// namespace uid that the role bindings of a namespace follow. A namespace with
// the name of an account namespace outside the account projects also puts
// that project on the project queue, so that the lister checks it.
func (s *Syncer) applyAccounts(ctx context.Context, cluster string, watch *clusterWatch, item patchItem, seen map[string]string) {
	nsName := item.target.Metadata.Name
	if item.deleted {
		delete(seen, nsName)
		return
	}
	state, ok := s.accountsOf(cluster)
	if !ok {
		return
	}
	projects := s.projectsOf(cluster)
	name := projectOf(item.target, cluster)
	uid := state.namespaces[name]
	key := settledKey(name, uid)
	if cached, ok := seen[nsName]; ok && cached == key {
		return
	}
	if slices.Contains(state.projects, name) {
		seen[nsName] = key
		return
	}
	if target, ok := strings.CutPrefix(nsName, accountPrefix); ok && watch != nil {
		if _, known := projects[target]; known {
			// A namespace with the name of an account namespace, outside
			// the account projects.
			watch.projects.put(target)
		}
	}
	if state.settled[nsName] == key {
		seen[nsName] = key
		return
	}
	if uid == "" && name != "" {
		source, known := projects[name]
		if !known || isTenant(source, name, s.cachedSelf()) {
			// A project that the run or the lister did not reach yet.
			return
		}
	}

	token, err := rancherclient.ReadToken(s.tokenFile)
	if err != nil {
		s.accountFailure(ctx, nil, "the role binding list request failed", err, "cluster", cluster, "namespace", nsName)
		return
	}
	existing, err := listAll(ctx, s, token, roleBindingsPath(cluster, nsName), accountRoleKey, pruneBinding)
	if err != nil {
		s.accountFailure(ctx, nil, "the role binding list request failed", err, "cluster", cluster, "namespace", nsName)
		return
	}
	if uid != "" {
		s.ensureRoleBindings(ctx, token, cluster, nsName, name, uid, existing, nil)
	} else {
		for _, b := range existing {
			if managedRoleBinding(b) {
				s.dropBinding(ctx, token, cluster, roleBindingsPath(cluster, nsName), b, b.Metadata.Labels[accountProjectKey], nil)
			}
		}
	}
	seen[nsName] = key
}
