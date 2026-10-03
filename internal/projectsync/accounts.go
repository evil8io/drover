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
	// project, the uid of its account namespace, and the uid of the namespace,
	// from the last run. The worker skips a namespace whose key is unchanged
	// since that run, so a watch replay after a restart costs no request.
	settled map[string]string
	// openbao is the uid of the OpenBao namespace when the last run trusted
	// it, and "" otherwise. The lister gives it to the OpenBao RoleBinding of
	// a project as owner.
	openbao string
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
// System project, not the Default project, not an account project, and not a
// project whose account namespace name is the OpenBao namespace.
func isTenant(item project, name, self string) bool {
	if item.reserved || isAccountProject(item, self) || accountNamespace(name) == openbaoNamespace {
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

// storeAccounts replaces the account view of cluster with the view of a run.
// before is the view at the start of the run. The lister can trust an account
// namespace while the run is busy, so the store keeps such a trust when the
// run did not check the project and the snapshot still has it. denied are the
// projects whose account namespace the run did not trust.
func (s *Syncer) storeAccounts(cluster string, state, before accountState, denied map[string]bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	live := s.clusters[cluster]
	for name, uid := range s.accounts[cluster].namespaces {
		if before.namespaces[name] == uid || denied[name] {
			continue
		}
		if _, ok := state.namespaces[name]; ok {
			continue
		}
		if _, ok := live[name]; ok {
			state.namespaces[name] = uid
		}
	}
	next := maps.Clone(s.accounts)
	if next == nil {
		next = make(map[string]accountState)
	}
	next[cluster] = state
	s.accounts = next
}

// clearOpenBaoNamespace clears the uid of the OpenBao namespace in the view of
// cluster. A non-empty uid clears only that uid, so that a newer uid of a run
// stays.
func (s *Syncer) clearOpenBaoNamespace(cluster, uid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.accounts[cluster]
	if !ok || state.openbao == "" || (uid != "" && state.openbao != uid) {
		return
	}
	next := maps.Clone(s.accounts)
	state.openbao = ""
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
	state.namespaces = namespaces
	next[cluster] = state
	s.accounts = next
}

// settledKey is the key of the worker cache for the namespace with the uid
// member in project name, whose account namespace has the uid account.
func settledKey(name, account, member string) string {
	return name + "/" + account + "/" + member
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
// namespaces are every namespace of the cluster. It returns the OpenBao target
// of the cluster when the run keeps the OpenBao objects, and nil otherwise.
func (s *Syncer) syncAccounts(ctx context.Context, token, cluster string, projects map[string]project, namespaces []namespace, run *counters) *openbaoTarget {
	before, _ := s.accountsOf(cluster)
	// The lister writes the OpenBao RoleBinding with the uid of the view, so a
	// run that ends before its own check must not keep that uid.
	stored := false
	defer func() {
		if !stored {
			s.clearOpenBaoNamespace(cluster, "")
		}
	}()

	self, err := s.selfID(ctx, token)
	if err != nil {
		s.accountFailure(ctx, run, "the user request failed", err, "cluster", cluster)
		return nil
	}

	owners := accountProjects(projects, self)
	if len(owners) == 0 {
		name, err := s.createAccountProject(ctx, token, cluster, self)
		if err != nil {
			s.accountFailure(ctx, run, "the account project create failed", err, "cluster", cluster)
			return nil
		}
		s.accountChanged(ctx, run, cluster, "project", "create", "", name, "")
		owners = []string{name}
	}

	accounts, err := listAll(ctx, s, token, serviceAccountsPath(cluster, ""), accountProjectKey, pruneObject)
	if err != nil {
		s.accountFailure(ctx, run, "the service account list request failed", err, "cluster", cluster)
		return nil
	}
	clusterBindings, err := listAll(ctx, s, token, clusterRoleBindingsPath(cluster), accountProjectKey, pruneBinding)
	if err != nil {
		s.accountFailure(ctx, run, "the cluster role binding list request failed", err, "cluster", cluster)
		return nil
	}
	roleBindings, err := listAll(ctx, s, token, roleBindingsPath(cluster, ""), accountRoleKey, pruneBinding)
	if err != nil {
		s.accountFailure(ctx, run, "the role binding list request failed", err, "cluster", cluster)
		return nil
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
	denied := make(map[string]bool)
	for _, name := range slices.Sorted(maps.Keys(projects)) {
		if !isTenant(projects[name], name, self) {
			continue
		}
		item, listed := byName[accountNamespace(name)]
		uid, outcome := s.checkAccountNamespace(ctx, token, cluster, tenantSpace(name), owners, item, listed, clusterBindingsOf[name], true, run)
		switch outcome {
		case trustUnknown:
			pending[name] = true
			continue
		case trustNo:
			denied[name] = true
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

	var space, account string
	if s.openbao != nil {
		space, account = s.syncOpenBao(ctx, token, cluster, owners, byName, trusted, roleBindings, roleBindingsIn, run)
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
			settled[item.Metadata.Name] = settledKey(name, uid, item.Metadata.UID)
		} else if !isTenant(source, name, self) {
			settled[item.Metadata.Name] = settledKey(name, "", item.Metadata.UID)
		}
	}
	s.storeAccounts(cluster, accountState{projects: owners, namespaces: trusted, settled: settled, openbao: space}, before, denied)
	stored = true

	s.sweepAccountNamespaces(ctx, token, cluster, projects, self, owners, namespaces, trusted, clusterBindingsOf, roleBindingsOf, run)
	if account == "" {
		return nil
	}
	return &openbaoTarget{account: account, tenants: slices.Sorted(maps.Keys(trusted))}
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

// accountSpace is a namespace that the service keeps in an account project:
// the account namespace of a tenant project, or the OpenBao namespace.
type accountSpace struct {
	name string
	// project is the tenant project of an account namespace, and empty for
	// the OpenBao namespace.
	project string
	// labels are the labels of a new namespace.
	labels map[string]string
	// what names the namespace in a log line, and loss names what the service
	// gives up when it may not use the namespace.
	what, loss string
}

func tenantSpace(project string) accountSpace {
	return accountSpace{
		name:    accountNamespace(project),
		project: project,
		labels:  map[string]string{accountProjectKey: project},
		what:    "account namespace",
		loss:    "the project gets no accounts",
	}
}

// attrs returns the log attributes of the namespace in cluster.
func (a accountSpace) attrs(cluster string) []any {
	if a.project == "" {
		return []any{"cluster", cluster}
	}
	return []any{"cluster", cluster, "project", a.project}
}

// checkAccountNamespace returns the uid of the namespace of space, and whether
// the service may use it. item is that namespace from the list, when listed is
// true. The service uses the namespace only in an account project, because a
// tenant can create the name first in its own project. A missing namespace is
// created. With adopt, a namespace in no project moves back into the first
// account project when a binding of proof names it as owner. The caller passes
// only bindings that no tenant can write, so that owner reference proves that
// the namespace was in an account project before.
func (s *Syncer) checkAccountNamespace(ctx context.Context, token, cluster string, space accountSpace, owners []string, item namespace, listed bool, proof []binding, adopt bool, run *counters) (string, trust) {
	if !listed {
		found, err := s.getObject(ctx, token, namespacePath(cluster, space.name), &item)
		if err != nil {
			s.accountFailure(ctx, run, "the "+space.what+" request failed", err, space.attrs(cluster)...)
			return "", trustUnknown
		}
		if !found {
			return s.createAccountNamespace(ctx, token, cluster, space, owners[0], run)
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
		s.logger.WarnContext(ctx, "the "+space.what+" is in another project, and "+space.loss,
			append(space.attrs(cluster), "namespace", space.name, "namespace_project", owner)...)
		return "", trustNo
	}
	if !adopt {
		return "", trustUnknown
	}
	if !ownsBindings(item, proof) {
		s.logger.WarnContext(ctx, "the "+space.what+" has no project and no binding of the service, and "+space.loss,
			append(space.attrs(cluster), "namespace", space.name)...)
		return "", trustNo
	}

	body := map[string]any{"metadata": map[string]any{"annotations": map[string]string{projectAnnotation: cluster + ":" + owners[0]}}}
	if err := s.send(ctx, token, http.MethodPatch, namespacePath(cluster, space.name), body, nil); err != nil {
		s.accountFailure(ctx, run, "the "+space.what+" move failed", err, space.attrs(cluster)...)
		return "", trustUnknown
	}
	s.accountChanged(ctx, run, cluster, "namespace", "move", "", space.name, space.project)
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

func (s *Syncer) createAccountNamespace(ctx context.Context, token, cluster string, space accountSpace, owner string, run *counters) (string, trust) {
	body := object{
		APIVersion: "v1",
		Kind:       "Namespace",
		Metadata: objectMeta{
			Name:        space.name,
			Labels:      space.labels,
			Annotations: map[string]string{projectAnnotation: cluster + ":" + owner},
		},
	}
	var created object
	if err := s.send(ctx, token, http.MethodPost, namespacesPath(cluster), body, &created); err != nil {
		s.accountFailure(ctx, run, "the "+space.what+" create failed", err, space.attrs(cluster)...)
		return "", trustUnknown
	}
	s.accountChanged(ctx, run, cluster, "namespace", "create", "", space.name, space.project)
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
		if have = createOrRead(ctx, s, token, cluster, collection, kind, project, want.Metadata, want, pruneBinding, run); have == nil {
			return
		}
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

// createOrRead creates want at collection. It returns nil when the create
// succeeds, and when it fails. A name that another writer took without the
// label of the service answers 409, and createOrRead then returns that object,
// pruned, so that the caller corrects it. kind names the object in the logs
// and the metrics, and meta is the metadata of want.
func createOrRead[T any](ctx context.Context, s *Syncer, token, cluster, collection, kind, project string, meta objectMeta, want T, prune func(T) T, run *counters) *T {
	err := s.send(ctx, token, http.MethodPost, collection, want, nil)
	if err == nil {
		s.accountChanged(ctx, run, cluster, kind, "create", meta.Namespace, meta.Name, project)
		return nil
	}
	attrs := []any{"cluster", cluster, "project", project, "namespace", meta.Namespace, "name", meta.Name}
	if !hasStatus(err, http.StatusConflict) {
		s.accountFailure(ctx, run, "the "+kind+" create failed", err, attrs...)
		return nil
	}
	var got T
	found, err := s.getObject(ctx, token, collection+"/"+meta.Name, &got)
	if err != nil || !found {
		if err != nil {
			s.accountFailure(ctx, run, "the "+kind+" request failed", err, attrs...)
		}
		return nil
	}
	got = prune(got)
	return &got
}

// reconcileRole makes the Role at collection equal to want. have is the
// listed Role of that name, or nil. A Role has no immutable field, so a
// difference is an update.
func (s *Syncer) reconcileRole(ctx context.Context, token, cluster string, want role, have *role, project string, run *counters) {
	collection := rolesPath(cluster, want.Metadata.Namespace)
	if have == nil {
		if have = createOrRead(ctx, s, token, cluster, collection, "role", project, want.Metadata, want, pruneRole, run); have == nil {
			return
		}
	}
	if sameRole(*have, want) {
		return
	}
	want.Metadata.ResourceVersion = have.Metadata.ResourceVersion
	if err := s.send(ctx, token, http.MethodPut, collection+"/"+want.Metadata.Name, want, nil); err != nil {
		s.accountFailure(ctx, run, "the role update failed", err,
			"cluster", cluster, "project", project, "namespace", want.Metadata.Namespace, "name", want.Metadata.Name)
		return
	}
	s.accountChanged(ctx, run, cluster, "role", "update", want.Metadata.Namespace, want.Metadata.Name, project)
}

// sameRole reports whether have has the rules, the owner references, and the
// labels of want.
func sameRole(have, want role) bool {
	return slices.EqualFunc(have.Rules, want.Rules, func(a, b policyRule) bool {
		return slices.Equal(a.APIGroups, b.APIGroups) && slices.Equal(a.Resources, b.Resources) &&
			slices.Equal(a.Verbs, b.Verbs) && slices.Equal(a.ResourceNames, b.ResourceNames)
	}) &&
		slices.Equal(have.Metadata.OwnerReferences, want.Metadata.OwnerReferences) &&
		maps.Equal(have.Metadata.Labels, want.Metadata.Labels)
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

// dropAccountBindings deletes the bindings of project name, the OpenBao
// RoleBinding in its account namespace included. The service calls it when
// the project may not use its account namespace, so that no binding grants a
// right to a ServiceAccount of that name in a namespace of a tenant.
// It returns the count of the deletes that failed.
func (s *Syncer) dropAccountBindings(ctx context.Context, token, cluster, name string, clusterBindings, roleBindings []binding, run *counters) int {
	failed := 0
	for _, item := range clusterBindings {
		if managedClusterBinding(item, name) && !s.dropBinding(ctx, token, cluster, clusterRoleBindingsPath(cluster), item, name, run) {
			failed++
		}
	}
	for _, item := range roleBindings {
		if (managedRoleBinding(item) || isOpenBaoBinding(item)) &&
			!s.dropBinding(ctx, token, cluster, roleBindingsPath(cluster, item.Metadata.Namespace), item, name, run) {
			failed++
		}
	}
	return failed
}

// dropBinding deletes item at collection. It returns false when the delete
// fails.
func (s *Syncer) dropBinding(ctx context.Context, token, cluster, collection string, item binding, project string, run *counters) bool {
	kind := "clusterrolebinding"
	if item.Metadata.Namespace != "" {
		kind = "rolebinding"
	}
	if err := s.deleteObject(ctx, token, collection+"/"+item.Metadata.Name); err != nil {
		s.accountFailure(ctx, run, "the "+kind+" delete failed", err,
			"cluster", cluster, "project", project, "namespace", item.Metadata.Namespace, "name", item.Metadata.Name)
		return false
	}
	s.accountChanged(ctx, run, cluster, kind, "delete", item.Metadata.Namespace, item.Metadata.Name, project)
	return true
}

// dropStrayRoleBindings deletes the role bindings of the service in a
// namespace in no project, or whose project gets no accounts. A namespace that
// the list of the run does not have keeps its bindings. A namespace of a
// project that the run does not know keeps the bindings of that project,
// because a project can be newer than the project list of the run. A
// namespace of a pending project keeps them too, because the check of its
// account namespace failed in this run. The bindings of another project go in
// both cases, because they grant rights in a namespace that the project left.
func (s *Syncer) dropStrayRoleBindings(ctx context.Context, token, cluster string, projects map[string]project, byName map[string]namespace, trusted map[string]string, pending map[string]bool, roleBindings []binding, run *counters) {
	for _, item := range roleBindings {
		if !managedRoleBinding(item) {
			continue
		}
		nsName := item.Metadata.Namespace
		member, listed := byName[nsName]
		if !listed {
			continue
		}
		name := projectOf(member, cluster)
		if _, ok := trusted[name]; ok {
			continue
		}
		_, known := projects[name]
		waiting := pending[name] || (!known && name != "")
		if waiting && item.Metadata.Labels[accountProjectKey] == name {
			continue
		}
		s.dropBinding(ctx, token, cluster, roleBindingsPath(cluster, nsName), item, item.Metadata.Labels[accountProjectKey], run)
	}
}

// sweepAccountNamespaces deletes an account namespace whose project gets no
// accounts or no longer exists. A project that the run does not know counts
// as gone only after Rancher answers 404 for it, because it can be newer than
// the project list of the run. The bindings go first, so that no binding
// names a ServiceAccount of a namespace that a tenant could create next. A
// namespace whose bindings do not all go stays until the next run.
func (s *Syncer) sweepAccountNamespaces(ctx context.Context, token, cluster string, projects map[string]project, self string, owners []string, namespaces []namespace, trusted map[string]string, clusterBindingsOf, roleBindingsOf map[string][]binding, run *counters) {
	for _, item := range namespaces {
		if item.Metadata.DeletionTimestamp != "" {
			continue
		}
		// The OpenBao namespace has the prefix of an account namespace, and no
		// project.
		if item.Metadata.Name == openbaoNamespace {
			continue
		}
		name, ok := strings.CutPrefix(item.Metadata.Name, accountPrefix)
		if !ok || name == "" {
			continue
		}
		if _, ok := trusted[name]; ok {
			continue
		}
		// A deleted account project leaves its namespaces in no project. A
		// tenant can put its own namespace of that name into no project, so
		// only a cluster role binding of the service is proof.
		owner := projectOf(item, cluster)
		if !slices.Contains(owners, owner) && (owner != "" || !ownsBindings(item, clusterBindingsOf[name])) {
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

		if s.dropAccountBindings(ctx, token, cluster, name, clusterBindingsOf[name], roleBindingsOf[name], run) > 0 {
			continue
		}
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
// project event: the account namespace, the ServiceAccounts, the cluster role
// bindings, and, when the last run trusted the OpenBao namespace, the OpenBao
// Role and RoleBinding, the roles and policies in OpenBao, and the login roles
// and the status of the trust annotation. The worker then
// handles the role bindings of each namespace of the project. A namespace in
// no project waits for the reconcile run, because only the run knows the
// account projects for certain.
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
	uid, outcome := s.checkAccountNamespace(ctx, token, cluster, tenantSpace(name), state.projects, namespace{}, false, clusterBindings, false, nil)
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
	if s.openbao != nil && state.openbao != "" && s.openbaoNamespaceLive(ctx, token, cluster, state) {
		s.ensureOpenBaoAccess(ctx, token, cluster, name, state.openbao, nil, nil, nil)
		s.writeOpenBaoProject(ctx, cluster, name)
		s.keepProjectTrust(ctx, token, cluster, name)
	}
	s.setAccountNamespace(cluster, name, uid)
}

// openbaoNamespaceLive reports whether the OpenBao namespace of cluster still
// has the uid that the last run trusted, in an account project of that run.
// A tenant can delete the namespace and create it again in its own project
// within one interval. Another uid clears the uid of the view.
func (s *Syncer) openbaoNamespaceLive(ctx context.Context, token, cluster string, state accountState) bool {
	var item namespace
	found, err := s.getObject(ctx, token, namespacePath(cluster, openbaoNamespace), &item)
	if err != nil {
		s.accountFailure(ctx, nil, "the OpenBao namespace request failed", err, "cluster", cluster)
		return false
	}
	if found && item.Metadata.UID == state.openbao && item.Metadata.DeletionTimestamp == "" &&
		slices.Contains(state.projects, projectOf(item, cluster)) {
		return true
	}
	s.logger.InfoContext(ctx, "the OpenBao namespace changed after the last reconcile run", "cluster", cluster)
	s.clearOpenBaoNamespace(cluster, state.openbao)
	return false
}

// applyAccounts brings the role bindings of one namespace of the queue up to
// date. seen is the cache of the worker: the project, the account namespace
// uid, and the namespace uid that the role bindings of a namespace follow. A
// namespace with the name of an account namespace outside the account
// projects also puts that project on the project queue, so that the lister
// checks it.
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
	key := settledKey(name, uid, item.target.Metadata.UID)
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
	// A project that the run or the lister did not reach yet keeps its own
	// bindings.
	waiting := false
	if uid == "" && name != "" {
		source, known := projects[name]
		waiting = !known || isTenant(source, name, s.cachedSelf())
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
			if managedRoleBinding(b) && (!waiting || b.Metadata.Labels[accountProjectKey] != name) {
				s.dropBinding(ctx, token, cluster, roleBindingsPath(cluster, nsName), b, b.Metadata.Labels[accountProjectKey], nil)
			}
		}
	}
	seen[nsName] = key
}
