package projectsync

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// publicRead is the bound role set of most tests: a ClusterRole, and a Role of
// the same name in kube-public.
var publicRead = []BoundRole{
	{Name: "drover:public-read"},
	{Namespace: "kube-public", Name: "drover:public-read"},
}

func withBoundRoles(roles []BoundRole) func(*Config) {
	return func(cfg *Config) { cfg.BoundRoles = roles }
}

// checkBoundPosts checks the create requests posts of the bindings of bound
// for project: one per project role, with the role reference, one
// ServiceAccount subject, the owner reference to the account namespace, and
// the labels of the service. kind is the role kind that the bindings
// reference.
func checkBoundPosts(t *testing.T, posts []recorded, project, uid, kind string, bound BoundRole) {
	t.Helper()
	got := make(map[string]binding)
	for _, req := range posts {
		b := decodeBody[binding](t, req.body)
		got[b.Metadata.Name] = b
	}
	for _, role := range accountRoles {
		name := boundBindingName(project, role, bound)
		b, ok := got[name]
		if !ok {
			t.Errorf("no create request of %s", name)
			continue
		}
		if want := (roleRef{APIGroup: rbacGroup, Kind: kind, Name: bound.Name}); b.RoleRef != want {
			t.Errorf("role ref of %s = %+v, want %+v", name, b.RoleRef, want)
		}
		if want := []subject{{Kind: "ServiceAccount", Name: role.name, Namespace: accountNamespace(project)}}; len(b.Subjects) != 1 || b.Subjects[0] != want[0] {
			t.Errorf("subjects of %s = %v, want %v", name, b.Subjects, want)
		}
		if len(b.Metadata.OwnerReferences) != 1 || b.Metadata.OwnerReferences[0].UID != uid {
			t.Errorf("owner references of %s = %v, want the account namespace %s", name, b.Metadata.OwnerReferences, uid)
		}
		if b.Metadata.Labels[accountProjectKey] != project || b.Metadata.Labels[accountRoleKey] != role.name {
			t.Errorf("labels of %s = %v, want the project and the role", name, b.Metadata.Labels)
		}
		if b.Metadata.Namespace != bound.Namespace {
			t.Errorf("namespace of %s = %q, want %q", name, b.Metadata.Namespace, bound.Namespace)
		}
	}
}

func TestReconcileBindsTheBoundRolesAndSweepsARemovedOne(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", nil)
	fake.addNamespace(tenantNamespace("ns-a", "p-alpha"))
	syncer, _ := newAccountsSyncer(t, fake, withBoundRoles(publicRead))

	syncer.reconcile(context.Background())

	uid := fake.namespaceUID(accountNamespace("p-alpha"))
	crbPosts := filterMethod(fake.requestsOfPath(clusterRoleBindingsPath(acctCluster)), http.MethodPost)
	if len(crbPosts) != 8 {
		t.Fatalf("cluster role binding create requests = %d, want 5 of the project roles and 3 of the bound ClusterRole", len(crbPosts))
	}
	checkBoundPosts(t, crbPosts, "p-alpha", uid, "ClusterRole", publicRead[0])
	rbPosts := filterMethod(fake.requestsOfPath(roleBindingsPath(acctCluster, "kube-public")), http.MethodPost)
	if len(rbPosts) != 3 {
		t.Fatalf("role binding create requests in kube-public = %d, want 3", len(rbPosts))
	}
	checkBoundPosts(t, rbPosts, "p-alpha", uid, "Role", publicRead[1])

	fake.resetRequests()
	syncer.reconcile(context.Background())
	if got := fake.writes(); len(got) != 0 {
		t.Errorf("writes of the second run = %d, want 0:\n%v", len(got), got)
	}

	// A config without the Role keeps the ClusterRoleBindings, and deletes the
	// RoleBindings in kube-public.
	fake.resetRequests()
	narrower, _ := newAccountsSyncer(t, fake, withBoundRoles(publicRead[:1]))
	narrower.reconcile(context.Background())
	for _, role := range accountRoles {
		path := roleBindingsPath(acctCluster, "kube-public") + "/" + boundBindingName("p-alpha", role, publicRead[1])
		if got := filterMethod(fake.requestsOfPath(path), http.MethodDelete); len(got) != 1 {
			t.Errorf("DELETE requests of %s = %d, want 1", path, len(got))
		}
	}
	var deletes []string
	for _, req := range fake.writes() {
		if req.method == http.MethodDelete && !strings.Contains(req.path, "/namespaces/kube-public/") {
			deletes = append(deletes, req.path)
		}
	}
	if len(deletes) != 0 {
		t.Errorf("other DELETE requests = %v, want none", deletes)
	}
}

func TestReconcileDropsTheBoundBindingsOfAProjectWithoutAccounts(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	markedProject(fake, "p-acct")
	fake.addProject("p-alpha", nil)

	colliding := accountNamespace("p-alpha")
	var ns namespace
	ns.Metadata.Name = colliding
	ns.Metadata.UID = "uid-" + colliding
	ns.Metadata.Labels = map[string]string{projectLabel: "p-beta"}
	ns.Metadata.Annotations = map[string]string{projectAnnotation: acctCluster + ":p-beta"}
	fake.addNamespace(ns)

	crb := boundBinding("p-alpha", "uid-old", accountRoles[0], publicRead[0])
	fake.addClusterRoleBinding(crb)
	rb := boundBinding("p-alpha", "uid-old", accountRoles[2], publicRead[1])
	fake.addRoleBinding("kube-public", rb)

	syncer, _ := newAccountsSyncer(t, fake, withBoundRoles(publicRead))
	syncer.reconcile(context.Background())

	crbPath := clusterRoleBindingsPath(acctCluster) + "/" + crb.Metadata.Name
	if got := filterMethod(fake.requestsOfPath(crbPath), http.MethodDelete); len(got) != 1 {
		t.Errorf("DELETE requests of the bound cluster role binding = %d, want 1", len(got))
	}
	rbPath := roleBindingsPath(acctCluster, "kube-public") + "/" + rb.Metadata.Name
	if got := filterMethod(fake.requestsOfPath(rbPath), http.MethodDelete); len(got) != 1 {
		t.Errorf("DELETE requests of the bound role binding = %d, want 1", len(got))
	}
}

func TestListProjectBindsTheBoundRolesThroughTheLister(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	markedProject(fake, "p-acct")
	syncer, _ := newAccountsSyncer(t, fake, withBoundRoles(publicRead))
	syncer.setAccounts(acctCluster, accountState{projects: []string{"p-acct"}})
	syncer.setClusters(map[string]map[string]project{
		acctCluster: {"p-new": {ID: acctCluster + ":p-new", ClusterID: acctCluster, Name: "p-new"}},
	})

	watch := newClusterWatch(10)
	syncer.listProject(context.Background(), acctCluster, "p-new", watch.patches)

	uid := fake.namespaceUID(accountNamespace("p-new"))
	crbPosts := filterMethod(fake.requestsOfPath(clusterRoleBindingsPath(acctCluster)), http.MethodPost)
	if len(crbPosts) != 8 {
		t.Fatalf("cluster role binding create requests = %d, want 8", len(crbPosts))
	}
	checkBoundPosts(t, crbPosts, "p-new", uid, "ClusterRole", publicRead[0])
	rbPosts := filterMethod(fake.requestsOfPath(roleBindingsPath(acctCluster, "kube-public")), http.MethodPost)
	if len(rbPosts) != 3 {
		t.Fatalf("role binding create requests in kube-public = %d, want 3", len(rbPosts))
	}
	checkBoundPosts(t, rbPosts, "p-new", uid, "Role", publicRead[1])

	// A second pass of the lister writes nothing.
	fake.resetRequests()
	syncer.listProject(context.Background(), acctCluster, "p-new", watch.patches)
	if got := fake.writes(); len(got) != 0 {
		t.Errorf("writes of the second pass = %d, want 0:\n%v", len(got), got)
	}
}

func TestNewRejectsAnInvalidBoundRole(t *testing.T) {
	t.Parallel()
	rancher := newFakeRancher(t)
	target, err := url.Parse(rancher.server.URL)
	if err != nil {
		t.Fatalf("parse the server URL: %v", err)
	}

	tests := []struct {
		name            string
		serviceAccounts bool
		roles           []BoundRole
		want            string
	}{
		{"without the service accounts", false, publicRead[:1], "the bound roles need the service accounts"},
		{"a name with a slash", true, []BoundRole{{Name: "a/b"}}, `the bound role name "a/b" is not a path segment`},
		{"an empty name", true, []BoundRole{{Namespace: "kube-public"}}, `the bound role name "" is not a path segment`},
		{"a namespace in upper case", true, []BoundRole{{Namespace: "Kube-Public", Name: "x"}}, `the namespace "Kube-Public" of the bound role x is not a DNS label`},
		{"a role twice", true, []BoundRole{publicRead[1], publicRead[1]}, "the bound role kube-public/drover:public-read is listed twice"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := Config{RancherURL: target, TokenFile: tokenFile(t, serviceToken), ServiceAccounts: tt.serviceAccounts, BoundRoles: tt.roles}
			if !tt.serviceAccounts {
				cfg.Labels = []string{"team"}
			}
			_, err := New(cfg)
			if err == nil || err.Error() != tt.want {
				t.Errorf("New = %v, want %q", err, tt.want)
			}
		})
	}

	if _, err := New(Config{RancherURL: target, TokenFile: tokenFile(t, serviceToken), ServiceAccounts: true, BoundRoles: publicRead}); err != nil {
		t.Errorf("New with the bound roles = %v, want no error", err)
	}
}
