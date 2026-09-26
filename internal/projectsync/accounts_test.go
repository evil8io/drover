package projectsync

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
)

// tenantNamespace returns a settled namespace of a tenant project: labelled,
// so the bulk namespace list finds it, and with the project annotation, the
// field that projectOf reads.
func tenantNamespace(name, project string) namespace {
	var ns namespace
	ns.Metadata.Name = name
	ns.Metadata.UID = "uid-" + name
	ns.Metadata.Labels = map[string]string{projectLabel: project}
	ns.Metadata.Annotations = map[string]string{projectAnnotation: acctCluster + ":" + project}
	return ns
}

// markedProject seeds fake with an account project that the service owns:
// marked, and created by the self user, so accountProjects finds it without a
// create.
func markedProject(fake *accountsFake, name string) {
	fake.addProject(name, func(p *storedProject) {
		p.creatorID = acctSelf
		p.labels = map[string]string{accountProjectLabel: "true"}
	})
}

// crbWant returns the role references that the service keeps for the cluster
// role bindings of one project, by name.
func crbWant(project string) map[string]roleRef {
	return map[string]roleRef{
		namespacesBindingName(project, accountRoles[0]): {APIGroup: rbacGroup, Kind: "ClusterRole", Name: project + "-namespaces-edit"},
		createBindingName(project, accountRoles[0]):     {APIGroup: rbacGroup, Kind: "ClusterRole", Name: createNamespaceRole},
		namespacesBindingName(project, accountRoles[1]): {APIGroup: rbacGroup, Kind: "ClusterRole", Name: project + "-namespaces-edit"},
		createBindingName(project, accountRoles[1]):     {APIGroup: rbacGroup, Kind: "ClusterRole", Name: createNamespaceRole},
		namespacesBindingName(project, accountRoles[2]): {APIGroup: rbacGroup, Kind: "ClusterRole", Name: project + "-namespaces-readonly"},
	}
}

// rbWant returns the ClusterRole of the namespace role bindings, by name.
func rbWant() map[string]string {
	return map[string]string{
		roleBindingName(accountRoles[0]): "admin",
		roleBindingName(accountRoles[1]): "edit",
		roleBindingName(accountRoles[2]): "view",
	}
}

func filterMethod(requests []recorded, method string) []recorded {
	var out []recorded
	for _, r := range requests {
		if r.method == method {
			out = append(out, r)
		}
	}
	return out
}

// createProjectPath is the path of the management.cattle.io create of the
// account project, of acctCluster.
var createProjectPath = clusterPath(rancherCluster) + managementProjects + acctCluster + "/projects"

func TestReconcileCreatesTheAccountsOfANewProject(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", nil)
	fake.addNamespace(tenantNamespace("ns-a", "p-alpha"))
	fake.addNamespace(tenantNamespace("ns-b", "p-alpha"))
	syncer, logs := newAccountsSyncer(t, fake)

	syncer.reconcile(context.Background())

	projectPosts := fake.requestsOfPath(createProjectPath)
	if len(projectPosts) != 1 {
		t.Fatalf("account project create requests = %d, want 1", len(projectPosts))
	}
	created := decodeBody[object](t, projectPosts[0].body)
	if got := created.Metadata.Labels[accountProjectLabel]; got != "true" {
		t.Errorf("label %s = %q, want true", accountProjectLabel, got)
	}
	if got := created.Metadata.Annotations[creatorAnnotation]; got != acctSelf {
		t.Errorf("annotation %s = %q, want %s", creatorAnnotation, got, acctSelf)
	}
	if got := created.Metadata.Namespace; got != acctCluster {
		t.Errorf("namespace of the account project = %q, want %s", got, acctCluster)
	}
	if spec, ok := created.Spec.(map[string]any); !ok || spec["displayName"] != accountProjectName {
		t.Errorf("spec.displayName of the account project = %v, want %s", created.Spec, accountProjectName)
	}

	nsPosts := filterMethod(fake.requestsOfPath(namespacesPath(acctCluster)), http.MethodPost)
	if len(nsPosts) != 1 {
		t.Fatalf("namespace create requests = %d, want 1", len(nsPosts))
	}
	accountNS := decodeBody[object](t, nsPosts[0].body)
	if got, want := accountNS.Metadata.Name, accountNamespace("p-alpha"); got != want {
		t.Errorf("name of the account namespace = %q, want %q", got, want)
	}
	if got, want := accountNS.Metadata.Annotations[projectAnnotation], acctCluster+":p-acct"; got != want {
		t.Errorf("project annotation of the account namespace = %q, want %q", got, want)
	}
	accountNSUID := fake.namespaceUID(accountNamespace("p-alpha"))

	saPosts := filterMethod(fake.requestsOfPath(serviceAccountsPath(acctCluster, "drover-p-alpha")), http.MethodPost)
	if len(saPosts) != 3 {
		t.Fatalf("service account create requests = %d, want 3", len(saPosts))
	}
	var saNames []string
	for _, req := range saPosts {
		saNames = append(saNames, decodeBody[object](t, req.body).Metadata.Name)
	}
	slices.Sort(saNames)
	if want := []string{"project-member", "project-owner", "read-only"}; !slices.Equal(saNames, want) {
		t.Errorf("service account names = %v, want %v", saNames, want)
	}

	crbPosts := filterMethod(fake.requestsOfPath(clusterRoleBindingsPath(acctCluster)), http.MethodPost)
	if len(crbPosts) != 5 {
		t.Fatalf("cluster role binding create requests = %d, want 5", len(crbPosts))
	}
	wantCRB := crbWant("p-alpha")
	for _, req := range crbPosts {
		b := decodeBody[binding](t, req.body)
		want, ok := wantCRB[b.Metadata.Name]
		if !ok {
			t.Errorf("unexpected cluster role binding %s", b.Metadata.Name)
			continue
		}
		if b.RoleRef != want {
			t.Errorf("role ref of %s = %+v, want %+v", b.Metadata.Name, b.RoleRef, want)
		}
		if len(b.Subjects) != 1 || b.Subjects[0].Kind != "ServiceAccount" || b.Subjects[0].Namespace != "drover-p-alpha" {
			t.Errorf("subjects of %s = %v, want one ServiceAccount subject in drover-p-alpha", b.Metadata.Name, b.Subjects)
		}
		if len(b.Metadata.OwnerReferences) != 1 || b.Metadata.OwnerReferences[0].Name != "drover-p-alpha" ||
			b.Metadata.OwnerReferences[0].UID != accountNSUID {
			t.Errorf("owner references of %s = %v, want the account namespace", b.Metadata.Name, b.Metadata.OwnerReferences)
		}
		if got := b.Metadata.Labels[accountProjectKey]; got != "p-alpha" {
			t.Errorf("project label of %s = %q, want p-alpha", b.Metadata.Name, got)
		}
	}

	wantRB := rbWant()
	for _, ns := range []string{"ns-a", "ns-b"} {
		posts := filterMethod(fake.requestsOfPath(roleBindingsPath(acctCluster, ns)), http.MethodPost)
		if len(posts) != 3 {
			t.Fatalf("role binding create requests of %s = %d, want 3", ns, len(posts))
		}
		for _, req := range posts {
			b := decodeBody[binding](t, req.body)
			want, ok := wantRB[b.Metadata.Name]
			if !ok {
				t.Errorf("unexpected role binding %s in %s", b.Metadata.Name, ns)
				continue
			}
			if b.RoleRef.Name != want {
				t.Errorf("role ref of %s in %s = %s, want %s", b.Metadata.Name, ns, b.RoleRef.Name, want)
			}
		}
	}

	if !strings.Contains(logs.String(), "msg=reconcile clusters=1 projects=1 namespaces=2 patched=0 errors=0 accounts_changed=16") {
		t.Errorf("no summary line with 16 account changes:\n%s", logs.String())
	}
}

func TestReconcileGivesNoAccountsToTheSystemOrTheDefaultProject(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	markedProject(fake, "p-acct")
	fake.addProject("p-system", func(p *storedProject) { p.labels = map[string]string{systemProjectLabel: "true"} })
	fake.addProject("p-default", func(p *storedProject) { p.labels = map[string]string{defaultProjectLabel: "true"} })
	syncer, _ := newAccountsSyncer(t, fake)

	syncer.reconcile(context.Background())

	for _, name := range []string{"p-system", "p-default"} {
		nsName := accountNamespace(name)
		if got := fake.requestsOfPath(namespacePath(acctCluster, nsName)); len(got) != 0 {
			t.Errorf("requests of %s = %d, want 0", nsName, len(got))
		}
		if got := filterMethod(fake.requestsOfPath(serviceAccountsPath(acctCluster, nsName)), http.MethodPost); len(got) != 0 {
			t.Errorf("service account create requests of %s = %d, want 0", nsName, len(got))
		}
	}
	if got := filterMethod(fake.requestsOfPath(clusterRoleBindingsPath(acctCluster)), http.MethodPost); len(got) != 0 {
		t.Errorf("cluster role binding create requests = %d, want 0", len(got))
	}
	if got := filterMethod(fake.requestsOfPath(createProjectPath), http.MethodPost); len(got) != 0 {
		t.Errorf("account project create requests = %d, want 0", len(got))
	}
}

func TestReconcileTreatsAProjectWithAnotherCreatorAsATenant(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-marked", func(p *storedProject) {
		p.creatorID = "someone-else"
		p.labels = map[string]string{accountProjectLabel: "true"}
	})
	syncer, _ := newAccountsSyncer(t, fake)

	syncer.reconcile(context.Background())

	if got := filterMethod(fake.requestsOfPath(createProjectPath), http.MethodPost); len(got) != 1 {
		t.Fatalf("account project create requests = %d, want 1", len(got))
	}
	nsPosts := filterMethod(fake.requestsOfPath(namespacesPath(acctCluster)), http.MethodPost)
	if len(nsPosts) != 1 {
		t.Fatalf("namespace create requests = %d, want 1", len(nsPosts))
	}
	if got, want := decodeBody[object](t, nsPosts[0].body).Metadata.Name, accountNamespace("p-marked"); got != want {
		t.Errorf("name of the account namespace = %q, want %q", got, want)
	}
	if got := filterMethod(fake.requestsOfPath(serviceAccountsPath(acctCluster, "drover-p-marked")), http.MethodPost); len(got) != 3 {
		t.Errorf("service account create requests = %d, want 3", len(got))
	}
}

func TestReconcileReusesAnExistingAccountProjectAndTheOlderOneWins(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-old", func(p *storedProject) {
		p.creatorID = acctSelf
		p.labels = map[string]string{accountProjectLabel: "true"}
		p.created = "2024-01-01T00:00:00Z"
	})
	fake.addProject("p-new-acct", func(p *storedProject) {
		p.creatorID = acctSelf
		p.labels = map[string]string{accountProjectLabel: "true"}
		p.created = "2024-06-01T00:00:00Z"
	})
	fake.addProject("p-alpha", nil)
	syncer, _ := newAccountsSyncer(t, fake)

	syncer.reconcile(context.Background())

	if got := filterMethod(fake.requestsOfPath(createProjectPath), http.MethodPost); len(got) != 0 {
		t.Fatalf("account project create requests = %d, want 0", len(got))
	}
	nsPosts := filterMethod(fake.requestsOfPath(namespacesPath(acctCluster)), http.MethodPost)
	if len(nsPosts) != 1 {
		t.Fatalf("namespace create requests = %d, want 1", len(nsPosts))
	}
	if got, want := decodeBody[object](t, nsPosts[0].body).Metadata.Annotations[projectAnnotation], acctCluster+":p-old"; got != want {
		t.Errorf("project annotation = %q, want %q (the older account project)", got, want)
	}
}

func TestReconcileWritesNothingOnASecondRun(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", nil)
	fake.addNamespace(tenantNamespace("ns-a", "p-alpha"))
	fake.addNamespace(tenantNamespace("ns-b", "p-alpha"))
	syncer, _ := newAccountsSyncer(t, fake)

	syncer.reconcile(context.Background())
	fake.resetRequests()
	syncer.reconcile(context.Background())

	if got := fake.writes(); len(got) != 0 {
		t.Errorf("writes of the second run = %d, want 0:\n%v", len(got), got)
	}
}

func TestReconcileCorrectsAWrongBindingAndCreatesAMissingOne(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", nil)
	fake.addNamespace(tenantNamespace("ns-a", "p-alpha"))
	fake.addNamespace(tenantNamespace("ns-b", "p-alpha"))
	syncer, _ := newAccountsSyncer(t, fake)
	syncer.reconcile(context.Background())

	// A wrong subject on the role binding of ns-a: an update.
	fake.mutateRoleBinding("ns-a", roleBindingName(accountRoles[0]), func(b *binding) {
		b.Subjects = []subject{{Kind: "ServiceAccount", Name: "someone-else", Namespace: accountNamespace("p-alpha")}}
	})
	// A wrong role reference on a cluster role binding: a delete and a
	// create, because the role reference is immutable.
	wrongName := namespacesBindingName("p-alpha", accountRoles[0])
	fake.mutateClusterRoleBinding(wrongName, func(b *binding) {
		b.RoleRef.Name = "wrong-role"
	})
	// A missing cluster role binding: a create.
	missingName := createBindingName("p-alpha", accountRoles[0])
	fake.removeClusterRoleBinding(missingName)

	fake.resetRequests()
	syncer.reconcile(context.Background())

	rbPath := roleBindingsPath(acctCluster, "ns-a") + "/" + roleBindingName(accountRoles[0])
	puts := filterMethod(fake.requestsOfPath(rbPath), http.MethodPut)
	if len(puts) != 1 {
		t.Fatalf("PUT requests of %s = %d, want 1", rbPath, len(puts))
	}
	if fixed := decodeBody[binding](t, puts[0].body); len(fixed.Subjects) != 1 || fixed.Subjects[0].Name != accountRoles[0].name {
		t.Errorf("subjects after the PUT of %s = %v, want %s", rbPath, fixed.Subjects, accountRoles[0].name)
	}

	crbPath := clusterRoleBindingsPath(acctCluster) + "/" + wrongName
	if got := filterMethod(fake.requestsOfPath(crbPath), http.MethodDelete); len(got) != 1 {
		t.Fatalf("DELETE requests of %s = %d, want 1", crbPath, len(got))
	}

	recreated := filterMethod(fake.requestsOfPath(clusterRoleBindingsPath(acctCluster)), http.MethodPost)
	var recreatedNames []string
	for _, req := range recreated {
		recreatedNames = append(recreatedNames, decodeBody[binding](t, req.body).Metadata.Name)
	}
	slices.Sort(recreatedNames)
	wantRecreated := []string{missingName, wrongName}
	slices.Sort(wantRecreated)
	if !slices.Equal(recreatedNames, wantRecreated) {
		t.Errorf("created cluster role bindings = %v, want %v", recreatedNames, wantRecreated)
	}

	if writes := fake.writes(); len(writes) != 4 {
		t.Errorf("writes of the second run = %d, want 4 (one PUT, one DELETE, two POST):\n%v", len(writes), writes)
	}
}

func TestReconcileSkipsAnAccountNamespaceNameTakenByAnotherProject(t *testing.T) {
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

	// A stray cluster role binding and a stray role binding of p-alpha, left
	// over from before the name collision.
	fake.addClusterRoleBinding(accountBinding("ClusterRoleBinding", namespacesBindingName("p-alpha", accountRoles[0]),
		"", "p-alpha-namespaces-edit", "p-alpha", "uid-old", accountRoles[0]))
	fake.addRoleBinding("junk-ns", accountBinding("RoleBinding", roleBindingName(accountRoles[0]),
		"junk-ns", accountRoles[0].clusterRole, "p-alpha", "uid-old", accountRoles[0]))

	syncer, logs := newAccountsSyncer(t, fake)
	syncer.reconcile(context.Background())

	if got := filterMethod(fake.requestsOfPath(serviceAccountsPath(acctCluster, colliding)), http.MethodPost); len(got) != 0 {
		t.Errorf("service account create requests in %s = %d, want 0", colliding, len(got))
	}
	nsWrites := fake.requestsOfPath(namespacePath(acctCluster, colliding))
	if len(filterMethod(nsWrites, http.MethodPatch))+len(filterMethod(nsWrites, http.MethodDelete)) != 0 {
		t.Errorf("writes of the colliding namespace = %v, want none", nsWrites)
	}
	want := `msg="the account namespace is in another project, and the project gets no accounts" ` +
		`cluster=c-1 project=p-alpha namespace=` + colliding + ` namespace_project=p-beta`
	if !strings.Contains(logs.String(), want) {
		t.Errorf("no warning line:\n%s", logs.String())
	}

	crbPath := clusterRoleBindingsPath(acctCluster) + "/" + namespacesBindingName("p-alpha", accountRoles[0])
	if got := filterMethod(fake.requestsOfPath(crbPath), http.MethodDelete); len(got) != 1 {
		t.Errorf("DELETE requests of the stray cluster role binding = %d, want 1", len(got))
	}
	rbPath := roleBindingsPath(acctCluster, "junk-ns") + "/" + roleBindingName(accountRoles[0])
	if got := filterMethod(fake.requestsOfPath(rbPath), http.MethodDelete); len(got) != 1 {
		t.Errorf("DELETE requests of the stray role binding = %d, want 1", len(got))
	}
}

func TestReconcileMovesAnOrphanedAccountNamespaceOnlyWithProof(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T, withProof bool) (*accountsFake, *syncBuffer) {
		t.Helper()
		fake := newAccountsFake(t)
		markedProject(fake, "p-acct")
		fake.addProject("p-alpha", nil)

		orphan := accountNamespace("p-alpha")
		var ns namespace
		ns.Metadata.Name = orphan
		ns.Metadata.UID = "uid-" + orphan
		// No label, no annotation: the namespace is in no project.
		fake.addNamespace(ns)

		ownerUID := ""
		if withProof {
			ownerUID = ns.Metadata.UID
		}
		fake.addClusterRoleBinding(accountBinding("ClusterRoleBinding", namespacesBindingName("p-alpha", accountRoles[0]),
			"", "p-alpha-namespaces-edit", "p-alpha", ownerUID, accountRoles[0]))

		syncer, logs := newAccountsSyncer(t, fake)
		syncer.reconcile(context.Background())
		return fake, logs
	}

	t.Run("with the proof of a matching owner reference", func(t *testing.T) {
		t.Parallel()
		fake, logs := setup(t, true)
		orphan := accountNamespace("p-alpha")

		patches := filterMethod(fake.requestsOfPath(namespacePath(acctCluster, orphan)), http.MethodPatch)
		if len(patches) != 1 {
			t.Fatalf("PATCH requests of %s = %d, want 1", orphan, len(patches))
		}
		var body struct {
			Metadata struct {
				Annotations map[string]string `json:"annotations"`
			} `json:"metadata"`
		}
		if err := json.Unmarshal([]byte(patches[0].body), &body); err != nil {
			t.Fatalf("decode the patch body: %v", err)
		}
		if got, want := body.Metadata.Annotations[projectAnnotation], acctCluster+":p-acct"; got != want {
			t.Errorf("project annotation of the patch = %q, want %q", got, want)
		}
		// A merge patch needs application/merge-patch+json, the content type
		// that the sibling patchNamespace (rancher.go) sends for the label
		// copy feature. A real Kubernetes-API-compatible server rejects any
		// other content type on a PATCH with 415.
		if got := patches[0].header.Get("Content-Type"); got != mergePatchType {
			t.Errorf("content type of the move patch = %q, want %q", got, mergePatchType)
		}
		want := `msg="account object changed" cluster=c-1 kind=namespace action=move namespace="" name=` + orphan + ` project=p-alpha`
		if !strings.Contains(logs.String(), want) {
			t.Errorf("no move line:\n%s", logs.String())
		}
	})

	t.Run("without a matching owner reference", func(t *testing.T) {
		t.Parallel()
		fake, logs := setup(t, false)
		orphan := accountNamespace("p-alpha")

		if got := filterMethod(fake.requestsOfPath(namespacePath(acctCluster, orphan)), http.MethodPatch); len(got) != 0 {
			t.Errorf("PATCH requests of %s = %d, want 0", orphan, len(got))
		}
		want := `msg="the account namespace has no project and no binding of the service, and the project gets no accounts" ` +
			`cluster=c-1 project=p-alpha namespace=` + orphan
		if !strings.Contains(logs.String(), want) {
			t.Errorf("no warning line:\n%s", logs.String())
		}
	})
}

func TestReconcileDeletesAStrayBindingOnlyWhenTheNamespaceHasNoProject(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	markedProject(fake, "p-acct")

	var lostNS namespace
	lostNS.Metadata.Name = "ns-x"
	// No label, no annotation: the namespace has no project.
	fake.addNamespace(lostNS)
	fake.addRoleBinding("ns-x", accountBinding("RoleBinding", roleBindingName(accountRoles[2]),
		"ns-x", accountRoles[2].clusterRole, "p-gone", "uid-gone", accountRoles[2]))

	var unknownNS namespace
	unknownNS.Metadata.Name = "ns-y"
	unknownNS.Metadata.Labels = map[string]string{projectLabel: "p-vanished"}
	unknownNS.Metadata.Annotations = map[string]string{projectAnnotation: acctCluster + ":p-vanished"}
	fake.addNamespace(unknownNS)
	fake.addRoleBinding("ns-y", accountBinding("RoleBinding", roleBindingName(accountRoles[0]),
		"ns-y", accountRoles[0].clusterRole, "p-vanished", "uid-vanished", accountRoles[0]))

	syncer, _ := newAccountsSyncer(t, fake)
	syncer.reconcile(context.Background())

	lostPath := roleBindingsPath(acctCluster, "ns-x") + "/" + roleBindingName(accountRoles[2])
	if got := filterMethod(fake.requestsOfPath(lostPath), http.MethodDelete); len(got) != 1 {
		t.Errorf("DELETE requests of the binding without a project = %d, want 1", len(got))
	}
	if got := filterMethod(fake.requestsOfPath(namespacePath(acctCluster, "ns-x")), http.MethodGet); len(got) != 1 {
		t.Errorf("GET requests of ns-x = %d, want 1", len(got))
	}

	unknownPath := roleBindingsPath(acctCluster, "ns-y") + "/" + roleBindingName(accountRoles[0])
	if got := filterMethod(fake.requestsOfPath(unknownPath), http.MethodDelete); len(got) != 0 {
		t.Errorf("DELETE requests of the binding of an unknown project = %d, want 0", len(got))
	}
	if got := filterMethod(fake.requestsOfPath(namespacePath(acctCluster, "ns-y")), http.MethodGet); len(got) != 0 {
		t.Errorf("GET requests of ns-y = %d, want 0 (kept without an extra request)", len(got))
	}
}

func TestReconcileDeletesAnAccountNamespaceOnlyAfterA404OfItsProject(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", nil)
	syncer, _ := newAccountsSyncer(t, fake)
	syncer.reconcile(context.Background()) // establishes the account project p-acct

	goneNS := accountNamespace("p-gone-a")
	var ns1 namespace
	ns1.Metadata.Name = goneNS
	ns1.Metadata.UID = "uid-" + goneNS
	ns1.Metadata.Labels = map[string]string{accountProjectKey: "p-gone-a", projectLabel: "p-acct"}
	ns1.Metadata.Annotations = map[string]string{projectAnnotation: acctCluster + ":p-acct"}
	fake.addNamespace(ns1)
	fake.addClusterRoleBinding(accountBinding("ClusterRoleBinding", namespacesBindingName("p-gone-a", accountRoles[0]),
		"", "p-gone-a-namespaces-edit", "p-gone-a", ns1.Metadata.UID, accountRoles[0]))

	keptNS := accountNamespace("p-gone-b")
	var ns2 namespace
	ns2.Metadata.Name = keptNS
	ns2.Metadata.UID = "uid-" + keptNS
	ns2.Metadata.Labels = map[string]string{accountProjectKey: "p-gone-b", projectLabel: "p-acct"}
	ns2.Metadata.Annotations = map[string]string{projectAnnotation: acctCluster + ":p-acct"}
	fake.addNamespace(ns2)
	fake.addUnlistedProject("p-gone-b", nil)

	fake.resetRequests()
	syncer.reconcile(context.Background())

	crbPath := clusterRoleBindingsPath(acctCluster) + "/" + namespacesBindingName("p-gone-a", accountRoles[0])
	nsPath := namespacePath(acctCluster, goneNS)
	all := fake.all()
	crbIdx, nsIdx := -1, -1
	for i, r := range all {
		if r.method == http.MethodDelete && r.path == crbPath && crbIdx == -1 {
			crbIdx = i
		}
		if r.method == http.MethodDelete && r.path == nsPath && nsIdx == -1 {
			nsIdx = i
		}
	}
	if crbIdx == -1 {
		t.Fatalf("no DELETE of the binding of p-gone-a")
	}
	if nsIdx == -1 {
		t.Fatalf("no DELETE of %s", goneNS)
	}
	if crbIdx > nsIdx {
		t.Errorf("the binding delete (index %d) came after the namespace delete (index %d)", crbIdx, nsIdx)
	}

	if got := filterMethod(fake.requestsOfPath(namespacePath(acctCluster, keptNS)), http.MethodDelete); len(got) != 0 {
		t.Errorf("DELETE requests of %s = %d, want 0 (its project answers 200)", keptNS, len(got))
	}
}

func TestReconcileSweepsAnAccountNamespaceOfAProjectGoneFromThePreviousTrustToo(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", nil)
	syncer, _ := newAccountsSyncer(t, fake)
	syncer.reconcile(context.Background())

	state, ok := syncer.accountsOf(acctCluster)
	if !ok {
		t.Fatal("no account state after the first run")
	}
	if _, ok := state.namespaces["p-alpha"]; !ok {
		t.Fatal("p-alpha is not trusted after the first run")
	}

	goneNS := accountNamespace("p-alpha")
	// Rancher settles the label some time after the create, so the second
	// run's bulk namespace list can find the account namespace.
	fake.mutateNamespace(goneNS, func(ns *namespace) {
		ns.Metadata.Labels = map[string]string{projectLabel: "p-acct"}
	})
	// p-alpha leaves both the project list of the run and the live snapshot,
	// because both come from the same list.
	fake.removeProject("p-alpha")

	fake.resetRequests()
	syncer.reconcile(context.Background())

	if got := filterMethod(fake.requestsOfPath(namespacePath(acctCluster, goneNS)), http.MethodDelete); len(got) != 1 {
		t.Errorf("DELETE requests of %s = %d, want 1", goneNS, len(got))
	}
	state, _ = syncer.accountsOf(acctCluster)
	if _, ok := state.namespaces["p-alpha"]; ok {
		t.Error("p-alpha is still trusted after the sweep, want the previous trust dropped")
	}
}

func TestReconcileKeepsTheRoleBindingsOfAProjectWhoseAccountNamespaceCheckFailed(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	markedProject(fake, "p-acct")
	fake.addProject("p-alpha", nil)
	fake.addNamespace(tenantNamespace("ns-a", "p-alpha"))
	for _, role := range accountRoles {
		fake.addRoleBinding("ns-a", accountBinding("RoleBinding", roleBindingName(role), "ns-a",
			role.clusterRole, "p-alpha", "uid-old", role))
	}

	orphan := accountNamespace("p-alpha")
	var ns namespace
	ns.Metadata.Name = orphan
	ns.Metadata.UID = "uid-" + orphan
	// No project: eligible for the adopt check.
	fake.addNamespace(ns)
	fake.addClusterRoleBinding(accountBinding("ClusterRoleBinding", namespacesBindingName("p-alpha", accountRoles[0]),
		"", "p-alpha-namespaces-edit", "p-alpha", ns.Metadata.UID, accountRoles[0]))
	fake.failPatch(orphan, http.StatusInternalServerError)

	syncer, logs := newAccountsSyncer(t, fake)
	syncer.reconcile(context.Background())

	for _, role := range accountRoles {
		path := roleBindingsPath(acctCluster, "ns-a") + "/" + roleBindingName(role)
		if got := filterMethod(fake.requestsOfPath(path), http.MethodDelete); len(got) != 0 {
			t.Errorf("DELETE requests of %s = %d, want 0 (the project is pending, neither trusted nor untrusted)", path, len(got))
		}
	}
	if got := filterMethod(fake.requestsOfPath(namespacePath(acctCluster, orphan)), http.MethodPatch); len(got) != 1 {
		t.Errorf("PATCH requests of %s = %d, want 1", orphan, len(got))
	}
	if !strings.Contains(logs.String(), `msg="the account namespace move failed"`) {
		t.Errorf("no failure line for the move:\n%s", logs.String())
	}
}

func TestApplyAccountsHandlesTheNamespaceCases(t *testing.T) {
	t.Parallel()

	t.Run("skips a namespace already in the settled map", func(t *testing.T) {
		t.Parallel()
		fake := newAccountsFake(t)
		syncer, _ := newAccountsSyncer(t, fake)
		syncer.setAccounts(acctCluster, accountState{
			namespaces: map[string]string{"p-alpha": "uid-1"},
			settled:    map[string]string{"ns-a": settledKey("p-alpha", "uid-1")},
		})
		watch := newClusterWatch(10)
		seen := make(map[string]string)

		syncer.applyAccounts(context.Background(), acctCluster, watch, patchItem{target: tenantNamespace("ns-a", "p-alpha")}, seen)

		if got := fake.all(); len(got) != 0 {
			t.Errorf("requests = %d, want 0", len(got))
		}
		if got, want := seen["ns-a"], settledKey("p-alpha", "uid-1"); got != want {
			t.Errorf("seen[ns-a] = %q, want %q", got, want)
		}
	})

	t.Run("updates the role bindings of a namespace moved to another trusted project", func(t *testing.T) {
		t.Parallel()
		fake := newAccountsFake(t)
		for _, role := range accountRoles {
			fake.addRoleBinding("ns-a", accountBinding("RoleBinding", roleBindingName(role), "ns-a",
				role.clusterRole, "p-alpha", "uid-1", role))
		}
		syncer, _ := newAccountsSyncer(t, fake)
		syncer.setAccounts(acctCluster, accountState{
			namespaces: map[string]string{"p-alpha": "uid-1", "p-beta": "uid-2"},
			settled:    map[string]string{"ns-a": settledKey("p-alpha", "uid-1")},
		})
		watch := newClusterWatch(10)
		seen := make(map[string]string)

		syncer.applyAccounts(context.Background(), acctCluster, watch, patchItem{target: tenantNamespace("ns-a", "p-beta")}, seen)

		for _, role := range accountRoles {
			path := roleBindingsPath(acctCluster, "ns-a") + "/" + roleBindingName(role)
			puts := filterMethod(fake.requestsOfPath(path), http.MethodPut)
			if len(puts) != 1 {
				t.Fatalf("PUT requests of %s = %d, want 1", path, len(puts))
			}
			updated := decodeBody[binding](t, puts[0].body)
			if len(updated.Subjects) != 1 || updated.Subjects[0].Namespace != accountNamespace("p-beta") {
				t.Errorf("subjects of %s after the move = %v, want the account namespace of p-beta", path, updated.Subjects)
			}
		}
		if got, want := seen["ns-a"], settledKey("p-beta", "uid-2"); got != want {
			t.Errorf("seen[ns-a] = %q, want %q", got, want)
		}
	})

	t.Run("deletes the role bindings of a namespace without a project label", func(t *testing.T) {
		t.Parallel()
		fake := newAccountsFake(t)
		for _, role := range accountRoles {
			fake.addRoleBinding("ns-b", accountBinding("RoleBinding", roleBindingName(role), "ns-b",
				role.clusterRole, "p-alpha", "uid-1", role))
		}
		syncer, _ := newAccountsSyncer(t, fake)
		syncer.setAccounts(acctCluster, accountState{
			namespaces: map[string]string{"p-alpha": "uid-1"},
			settled:    map[string]string{"ns-b": settledKey("p-alpha", "uid-1")},
		})
		watch := newClusterWatch(10)
		seen := make(map[string]string)

		var target namespace
		target.Metadata.Name = "ns-b"
		syncer.applyAccounts(context.Background(), acctCluster, watch, patchItem{target: target}, seen)

		for _, role := range accountRoles {
			path := roleBindingsPath(acctCluster, "ns-b") + "/" + roleBindingName(role)
			if got := filterMethod(fake.requestsOfPath(path), http.MethodDelete); len(got) != 1 {
				t.Errorf("DELETE requests of %s = %d, want 1", path, len(got))
			}
		}
		if got, want := seen["ns-b"], settledKey("", ""); got != want {
			t.Errorf("seen[ns-b] = %q, want %q", got, want)
		}
	})

	t.Run("a deleted item only clears the seen entry", func(t *testing.T) {
		t.Parallel()
		fake := newAccountsFake(t)
		syncer, _ := newAccountsSyncer(t, fake)
		watch := newClusterWatch(10)
		seen := map[string]string{"ns-a": "something"}

		var target namespace
		target.Metadata.Name = "ns-a"
		syncer.applyAccounts(context.Background(), acctCluster, watch, patchItem{target: target, deleted: true}, seen)

		if _, ok := seen["ns-a"]; ok {
			t.Error("seen still has ns-a")
		}
		if got := fake.all(); len(got) != 0 {
			t.Errorf("requests = %d, want 0", len(got))
		}
	})

	t.Run("skips a namespace inside an account project", func(t *testing.T) {
		t.Parallel()
		fake := newAccountsFake(t)
		syncer, _ := newAccountsSyncer(t, fake)
		syncer.setAccounts(acctCluster, accountState{projects: []string{"p-acct"}})
		watch := newClusterWatch(10)
		seen := make(map[string]string)

		var target namespace
		target.Metadata.Name = "junk-in-account"
		target.Metadata.Annotations = map[string]string{projectAnnotation: acctCluster + ":p-acct"}
		syncer.applyAccounts(context.Background(), acctCluster, watch, patchItem{target: target}, seen)

		if got := fake.all(); len(got) != 0 {
			t.Errorf("requests = %d, want 0", len(got))
		}
		if _, ok := seen["junk-in-account"]; !ok {
			t.Error("seen has no entry for junk-in-account")
		}
	})

	t.Run("handles a namespace named like an account namespace outside the account projects", func(t *testing.T) {
		t.Parallel()
		fake := newAccountsFake(t)
		syncer, _ := newAccountsSyncer(t, fake)
		syncer.setAccounts(acctCluster, accountState{
			projects:   []string{"p-acct"},
			namespaces: map[string]string{"p-real": "uid-real"},
		})
		syncer.setClusters(map[string]map[string]project{
			acctCluster: {"p-outside": {ID: acctCluster + ":p-outside", ClusterID: acctCluster, Name: "p-outside"}},
		})
		watch := newClusterWatch(10)
		seen := make(map[string]string)

		var target namespace
		target.Metadata.Name = accountNamespace("p-outside")
		target.Metadata.Annotations = map[string]string{projectAnnotation: acctCluster + ":p-real"}
		syncer.applyAccounts(context.Background(), acctCluster, watch, patchItem{target: target}, seen)

		if watch.projects.idle() {
			t.Error("the project queue is empty, want p-outside queued")
		}
		collection := roleBindingsPath(acctCluster, accountNamespace("p-outside"))
		posts := filterMethod(fake.requestsOfPath(collection), http.MethodPost)
		if len(posts) != 3 {
			t.Fatalf("role binding create requests = %d, want 3 (the namespace's own project is p-real)", len(posts))
		}
		var names []string
		for _, req := range posts {
			b := decodeBody[binding](t, req.body)
			names = append(names, b.Metadata.Name)
			if b.Subjects[0].Namespace != accountNamespace("p-real") {
				t.Errorf("subject namespace of %s = %s, want %s", b.Metadata.Name, b.Subjects[0].Namespace, accountNamespace("p-real"))
			}
		}
		slices.Sort(names)
		want := []string{roleBindingName(accountRoles[0]), roleBindingName(accountRoles[1]), roleBindingName(accountRoles[2])}
		slices.Sort(want)
		if !slices.Equal(names, want) {
			t.Errorf("created role bindings = %v, want %v", names, want)
		}
	})
}

func TestListProjectCreatesTheAccountsOfANewProjectThroughTheLister(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	markedProject(fake, "p-acct")
	syncer, _ := newAccountsSyncer(t, fake)
	syncer.setAccounts(acctCluster, accountState{projects: []string{"p-acct"}})
	syncer.setClusters(map[string]map[string]project{
		acctCluster: {"p-new": {ID: acctCluster + ":p-new", ClusterID: acctCluster, Name: "p-new"}},
	})

	watch := newClusterWatch(10)
	syncer.listProject(context.Background(), acctCluster, "p-new", watch.patches)

	nsPosts := filterMethod(fake.requestsOfPath(namespacesPath(acctCluster)), http.MethodPost)
	if len(nsPosts) != 1 {
		t.Fatalf("namespace create requests = %d, want 1", len(nsPosts))
	}
	if got, want := decodeBody[object](t, nsPosts[0].body).Metadata.Name, accountNamespace("p-new"); got != want {
		t.Errorf("name of the account namespace = %q, want %q", got, want)
	}
	if got := filterMethod(fake.requestsOfPath(serviceAccountsPath(acctCluster, "drover-p-new")), http.MethodPost); len(got) != 3 {
		t.Errorf("service account create requests = %d, want 3", len(got))
	}
	if got := filterMethod(fake.requestsOfPath(clusterRoleBindingsPath(acctCluster)), http.MethodPost); len(got) != 5 {
		t.Errorf("cluster role binding create requests = %d, want 5", len(got))
	}

	state, _ := syncer.accountsOf(acctCluster)
	if _, ok := state.namespaces["p-new"]; !ok {
		t.Error("the account state has no namespace uid for p-new")
	}
}

func TestNewAcceptsServiceAccountsWithoutAnyKey(t *testing.T) {
	t.Parallel()
	rancher := newFakeRancher(t)
	target, err := url.Parse(rancher.server.URL)
	if err != nil {
		t.Fatalf("parse the server URL: %v", err)
	}

	if _, err := New(Config{RancherURL: target, TokenFile: tokenFile(t, serviceToken), ServiceAccounts: true}); err != nil {
		t.Fatalf("New with only ServiceAccounts = %v, want no error", err)
	}
	if _, err := New(Config{RancherURL: target, TokenFile: tokenFile(t, serviceToken)}); err == nil {
		t.Fatal("New without any key and without ServiceAccounts = nil error, want an error")
	}
}

func TestSummaryLineHasAccountsChangedOnlyWithTheFeatureOn(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	on, logsOn := newAccountsSyncer(t, fake)
	off, logsOff := newAccountsSyncer(t, fake, func(cfg *Config) {
		cfg.ServiceAccounts = false
		cfg.Labels = []string{"cost-center"}
	})

	run := counters{clusters: 1, projects: 1, namespaces: 2, accounts: 16}
	on.logSummary(context.Background(), run)
	off.logSummary(context.Background(), run)

	if !strings.Contains(logsOn.String(), "accounts_changed=16") {
		t.Errorf("no accounts_changed with the feature on:\n%s", logsOn.String())
	}
	if strings.Contains(logsOff.String(), "accounts_changed") {
		t.Errorf("accounts_changed with the feature off:\n%s", logsOff.String())
	}
}
