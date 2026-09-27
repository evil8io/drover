package projectsync

import (
	"context"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// accountNamespaceIn returns a namespace of the account project p-acct with
// the name of the account namespace of project.
func accountNamespaceIn(project string) namespace {
	var ns namespace
	ns.Metadata.Name = accountNamespace(project)
	ns.Metadata.UID = "uid-" + ns.Metadata.Name
	ns.Metadata.Labels = map[string]string{accountProjectKey: project, projectLabel: "p-acct"}
	ns.Metadata.Annotations = map[string]string{projectAnnotation: acctCluster + ":p-acct"}
	return ns
}

// testCA is a Rancher CA chain in the tests. The service passes it on as it
// is, so it needs no valid certificate.
const testCA = "-----BEGIN CERTIFICATE-----\nMIIBtest\n-----END CERTIFICATE-----\n"

func wantOpenBaoRules() []policyRule {
	names := []string{"project-owner", "project-member", "read-only"}
	return []policyRule{
		{APIGroups: []string{""}, Resources: []string{"serviceaccounts"}, Verbs: []string{"get"}, ResourceNames: names},
		{APIGroups: []string{""}, Resources: []string{"serviceaccounts/token"}, Verbs: []string{"create"}, ResourceNames: names},
	}
}

func TestReconcileKeepsTheOpenBaoObjectsOfEveryAccountNamespace(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", nil)
	fake.addProject("p-beta", nil)
	fake.addNamespace(tenantNamespace("ns-a", "p-alpha"))
	setup := newOpenBaoSetup(t, fake)

	setup.syncer.reconcile(context.Background())

	var nsBodies []object
	for _, req := range filterMethod(fake.requestsOfPath(namespacesPath(acctCluster)), http.MethodPost) {
		if body := decodeBody[object](t, req.body); body.Metadata.Name == openbaoNamespace {
			nsBodies = append(nsBodies, body)
		}
	}
	if len(nsBodies) != 1 {
		t.Fatalf("create requests of %s = %d, want 1", openbaoNamespace, len(nsBodies))
	}
	if got, want := nsBodies[0].Metadata.Labels, map[string]string{accountRoleKey: openbaoLabel}; !reflect.DeepEqual(got, want) {
		t.Errorf("labels of %s = %v, want %v", openbaoNamespace, got, want)
	}
	if got, want := nsBodies[0].Metadata.Annotations[projectAnnotation], acctCluster+":p-acct"; got != want {
		t.Errorf("project annotation of %s = %q, want %q", openbaoNamespace, got, want)
	}

	saPosts := filterMethod(fake.requestsOfPath(serviceAccountsPath(acctCluster, openbaoNamespace)), http.MethodPost)
	if len(saPosts) != 1 {
		t.Fatalf("service account create requests in %s = %d, want 1", openbaoNamespace, len(saPosts))
	}
	if got := decodeBody[object](t, saPosts[0].body).Metadata.Name; got != openbaoAccount {
		t.Errorf("service account name = %q, want %q", got, openbaoAccount)
	}

	for _, project := range []string{"p-alpha", "p-beta"} {
		nsName := accountNamespace(project)
		wantLabels := map[string]string{accountProjectKey: project, accountRoleKey: openbaoLabel}

		rolePosts := filterMethod(fake.requestsOfPath(rolesPath(acctCluster, nsName)), http.MethodPost)
		if len(rolePosts) != 1 {
			t.Fatalf("role create requests in %s = %d, want 1", nsName, len(rolePosts))
		}
		gotRole := decodeBody[role](t, rolePosts[0].body)
		if gotRole.Metadata.Name != openbaoNamespace || !reflect.DeepEqual(gotRole.Metadata.Labels, wantLabels) {
			t.Errorf("role in %s = %s with labels %v, want %s with %v", nsName, gotRole.Metadata.Name, gotRole.Metadata.Labels, openbaoNamespace, wantLabels)
		}
		if !reflect.DeepEqual(gotRole.Rules, wantOpenBaoRules()) {
			t.Errorf("rules of the role in %s = %+v, want %+v", nsName, gotRole.Rules, wantOpenBaoRules())
		}

		var bindings []binding
		for _, req := range filterMethod(fake.requestsOfPath(roleBindingsPath(acctCluster, nsName)), http.MethodPost) {
			bindings = append(bindings, decodeBody[binding](t, req.body))
		}
		if len(bindings) != 1 {
			t.Fatalf("role binding create requests in %s = %d, want 1", nsName, len(bindings))
		}
		got := bindings[0]
		if got.Metadata.Name != openbaoNamespace || !reflect.DeepEqual(got.Metadata.Labels, wantLabels) {
			t.Errorf("role binding in %s = %s with labels %v, want %s with %v", nsName, got.Metadata.Name, got.Metadata.Labels, openbaoNamespace, wantLabels)
		}
		if want := (roleRef{APIGroup: rbacGroup, Kind: "Role", Name: openbaoNamespace}); got.RoleRef != want {
			t.Errorf("role ref in %s = %+v, want %+v", nsName, got.RoleRef, want)
		}
		if want := []subject{{Kind: "ServiceAccount", Name: openbaoAccount, Namespace: openbaoNamespace}}; !reflect.DeepEqual(got.Subjects, want) {
			t.Errorf("subjects in %s = %+v, want %+v", nsName, got.Subjects, want)
		}
		wantOwner := []ownerReference{{APIVersion: "v1", Kind: "Namespace", Name: openbaoNamespace, UID: fake.namespaceUID(openbaoNamespace)}}
		if !reflect.DeepEqual(got.Metadata.OwnerReferences, wantOwner) {
			t.Errorf("owner references in %s = %+v, want %+v", nsName, got.Metadata.OwnerReferences, wantOwner)
		}
	}
	for _, req := range filterMethod(fake.requestsOfPath(roleBindingsPath(acctCluster, "ns-a")), http.MethodPost) {
		if name := decodeBody[binding](t, req.body).Metadata.Name; name == openbaoNamespace {
			t.Errorf("role binding %s in the tenant namespace ns-a", name)
		}
	}

	fake.resetRequests()
	setup.bao.reset()
	setup.syncer.reconcile(context.Background())

	if got := fake.writes(); len(got) != 0 {
		t.Errorf("writes of the second run = %d, want 0:\n%v", len(got), got)
	}
	if got := fake.requestsOfPath(projectsPath + "/" + acctCluster + ":openbao"); len(got) != 0 {
		t.Errorf("project requests for the OpenBao namespace = %d, want 0", len(got))
	}
	if got := setup.bao.all(); len(got) != 0 {
		t.Errorf("OpenBao requests of the second run = %d, want 0", len(got))
	}
}

func TestReconcileCorrectsAChangedOrDeletedOpenBaoRoleAndRoleBinding(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", nil)
	fake.addProject("p-beta", nil)
	setup := newOpenBaoSetup(t, fake)
	setup.syncer.reconcile(context.Background())

	alpha, beta := accountNamespace("p-alpha"), accountNamespace("p-beta")
	fake.mutateRole(alpha, openbaoNamespace, func(item *role) {
		item.Rules[1].ResourceNames = []string{"project-owner", "project-member", "read-only", "someone-else"}
	})
	fake.removeRole(beta, openbaoNamespace)
	fake.mutateRoleBinding(alpha, openbaoNamespace, func(b *binding) {
		b.Subjects = append(b.Subjects, subject{Kind: "ServiceAccount", Name: "intruder", Namespace: "ns-a"})
	})
	fake.removeRoleBinding(beta, openbaoNamespace)

	fake.resetRequests()
	setup.syncer.reconcile(context.Background())

	rolePuts := filterMethod(fake.requestsOfPath(rolesPath(acctCluster, alpha)+"/"+openbaoNamespace), http.MethodPut)
	if len(rolePuts) != 1 {
		t.Fatalf("role update requests in %s = %d, want 1", alpha, len(rolePuts))
	}
	if got := decodeBody[role](t, rolePuts[0].body).Rules; !reflect.DeepEqual(got, wantOpenBaoRules()) {
		t.Errorf("rules after the update = %+v, want %+v", got, wantOpenBaoRules())
	}
	if got := filterMethod(fake.requestsOfPath(rolesPath(acctCluster, beta)), http.MethodPost); len(got) != 1 {
		t.Errorf("role create requests in %s = %d, want 1", beta, len(got))
	}

	bindingPuts := filterMethod(fake.requestsOfPath(roleBindingsPath(acctCluster, alpha)+"/"+openbaoNamespace), http.MethodPut)
	if len(bindingPuts) != 1 {
		t.Fatalf("role binding update requests in %s = %d, want 1", alpha, len(bindingPuts))
	}
	want := []subject{{Kind: "ServiceAccount", Name: openbaoAccount, Namespace: openbaoNamespace}}
	if got := decodeBody[binding](t, bindingPuts[0].body).Subjects; !reflect.DeepEqual(got, want) {
		t.Errorf("subjects after the update = %+v, want %+v", got, want)
	}
	if got := filterMethod(fake.requestsOfPath(roleBindingsPath(acctCluster, beta)), http.MethodPost); len(got) != 1 {
		t.Errorf("role binding create requests in %s = %d, want 1", beta, len(got))
	}

	if got := fake.writes(); len(got) != 4 {
		t.Errorf("writes of the second run = %d, want 4:\n%v", len(got), got)
	}
}

func TestReconcileGivesNoOpenBaoConfigWhenATenantOwnsTheNamespace(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	markedProject(fake, "p-acct")
	fake.addProject("p-alpha", nil)
	fake.addProject("p-beta", nil)
	fake.addNamespace(accountNamespaceIn("p-alpha"))
	// An OpenBao RoleBinding from before the tenant took the name.
	fake.addRoleBinding(accountNamespace("p-alpha"), openbaoRoleBinding("p-alpha", "uid-old"))
	fake.addNamespace(tenantNamespace(openbaoNamespace, "p-beta"))
	setup := newOpenBaoSetup(t, fake)

	setup.syncer.reconcile(context.Background())

	want := `level=WARN msg="the OpenBao namespace is in another project, and the cluster gets no OpenBao config" ` +
		`cluster=c-1 namespace=` + openbaoNamespace + ` namespace_project=p-beta`
	if !strings.Contains(setup.logs.String(), want) {
		t.Errorf("no warning line:\n%s", setup.logs.String())
	}
	stale := roleBindingsPath(acctCluster, accountNamespace("p-alpha")) + "/" + openbaoNamespace
	if got := filterMethod(fake.requestsOfPath(stale), http.MethodDelete); len(got) != 1 {
		t.Errorf("DELETE requests of the OpenBao role binding = %d, want 1", len(got))
	}
	nsRequests := fake.requestsOfPath(namespacePath(acctCluster, openbaoNamespace))
	if len(filterMethod(nsRequests, http.MethodPatch))+len(filterMethod(nsRequests, http.MethodDelete)) != 0 {
		t.Errorf("writes of the tenant namespace = %v, want none", nsRequests)
	}
	if got := filterMethod(fake.requestsOfPath(serviceAccountsPath(acctCluster, openbaoNamespace)), http.MethodPost); len(got) != 0 {
		t.Errorf("service account create requests in %s = %d, want 0", openbaoNamespace, len(got))
	}
	for _, project := range []string{"p-alpha", "p-beta"} {
		if got := filterMethod(fake.requestsOfPath(rolesPath(acctCluster, accountNamespace(project))), http.MethodPost); len(got) != 0 {
			t.Errorf("role create requests of %s = %d, want 0", project, len(got))
		}
	}
	if got := fake.requestsOfPath(tokenRequestPath(acctCluster)); len(got) != 0 {
		t.Errorf("token requests = %d, want 0", len(got))
	}
	if got := fake.requestsOfPath(rancherCAPath); len(got) != 0 {
		t.Errorf("Rancher CA requests without a ready cluster = %d, want 0", len(got))
	}
	if got := setup.bao.all(); len(got) != 0 {
		t.Errorf("OpenBao requests = %d, want 0", len(got))
	}
}

func TestReconcileDeletesTheOpenBaoRoleBindingOfAProjectWithoutAccounts(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	markedProject(fake, "p-acct")
	fake.addProject("p-alpha", nil)
	fake.addProject("p-beta", nil)
	taken := tenantNamespace(accountNamespace("p-alpha"), "p-beta")
	fake.addNamespace(taken)
	fake.addRoleBinding(taken.Metadata.Name, openbaoRoleBinding("p-alpha", "uid-old"))
	setup := newOpenBaoSetup(t, fake)

	setup.syncer.reconcile(context.Background())

	path := roleBindingsPath(acctCluster, taken.Metadata.Name) + "/" + openbaoNamespace
	if got := filterMethod(fake.requestsOfPath(path), http.MethodDelete); len(got) != 1 {
		t.Errorf("DELETE requests of %s = %d, want 1", path, len(got))
	}
	if got := filterMethod(fake.requestsOfPath(rolesPath(acctCluster, taken.Metadata.Name)), http.MethodPost); len(got) != 0 {
		t.Errorf("role create requests in %s = %d, want 0", taken.Metadata.Name, len(got))
	}
	if got := filterMethod(fake.requestsOfPath(rolesPath(acctCluster, accountNamespace("p-beta"))), http.MethodPost); len(got) != 1 {
		t.Errorf("role create requests of p-beta = %d, want 1", len(got))
	}
}

func TestReconcileMovesAnOpenBaoNamespaceInNoProjectOnlyWithProof(t *testing.T) {
	t.Parallel()
	orphanUID := "uid-" + openbaoNamespace

	cases := []struct {
		name string
		// proofIn is the namespace of an OpenBao role binding that names the
		// orphan as owner, or "" for none.
		proofIn string
		moved   bool
	}{
		{name: "with a role binding in an account namespace", proofIn: accountNamespace("p-alpha"), moved: true},
		{name: "with a role binding in a tenant namespace only", proofIn: "ns-a"},
		{name: "without a role binding"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fake := newAccountsFake(t)
			markedProject(fake, "p-acct")
			fake.addProject("p-alpha", nil)
			fake.addNamespace(accountNamespaceIn("p-alpha"))
			fake.addNamespace(tenantNamespace("ns-a", "p-alpha"))
			var orphan namespace
			orphan.Metadata.Name = openbaoNamespace
			orphan.Metadata.UID = orphanUID
			fake.addNamespace(orphan)
			if tc.proofIn != "" {
				proof := openbaoRoleBinding("p-alpha", orphanUID)
				proof.Metadata.Namespace = tc.proofIn
				fake.addRoleBinding(tc.proofIn, proof)
			}
			setup := newOpenBaoSetup(t, fake)

			setup.syncer.reconcile(context.Background())

			patches := filterMethod(fake.requestsOfPath(namespacePath(acctCluster, openbaoNamespace)), http.MethodPatch)
			if !tc.moved {
				if len(patches) != 0 {
					t.Errorf("PATCH requests of %s = %d, want 0", openbaoNamespace, len(patches))
				}
				want := `level=WARN msg="the OpenBao namespace has no project and no binding of the service, and the cluster gets no OpenBao config" ` +
					`cluster=c-1 namespace=` + openbaoNamespace
				if !strings.Contains(setup.logs.String(), want) {
					t.Errorf("no warning line:\n%s", setup.logs.String())
				}
				if tc.proofIn != "" {
					path := roleBindingsPath(acctCluster, tc.proofIn) + "/" + openbaoNamespace
					if got := filterMethod(fake.requestsOfPath(path), http.MethodDelete); len(got) != 1 {
						t.Errorf("DELETE requests of %s = %d, want 1", path, len(got))
					}
				}
				if got := setup.bao.all(); len(got) != 0 {
					t.Errorf("OpenBao requests = %d, want 0", len(got))
				}
				return
			}

			if len(patches) != 1 {
				t.Fatalf("PATCH requests of %s = %d, want 1", openbaoNamespace, len(patches))
			}
			body := decodeBody[object](t, patches[0].body)
			if got, want := body.Metadata.Annotations[projectAnnotation], acctCluster+":p-acct"; got != want {
				t.Errorf("project annotation of the move = %q, want %q", got, want)
			}
			want := `msg="account object changed" cluster=c-1 kind=namespace action=move namespace="" name=` + openbaoNamespace + ` project=""`
			if !strings.Contains(setup.logs.String(), want) {
				t.Errorf("no move line:\n%s", setup.logs.String())
			}
			if got := setup.bao.writes(acctCluster); len(got) != 1 {
				t.Errorf("OpenBao config writes = %d, want 1", len(got))
			}
		})
	}
}

func TestReconcileGivesNoAccountsToAProjectNamedOpenBao(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("openbao", nil)
	fake.addProject("p-alpha", nil)
	setup := newOpenBaoSetup(t, fake)

	setup.syncer.reconcile(context.Background())

	saPosts := filterMethod(fake.requestsOfPath(serviceAccountsPath(acctCluster, openbaoNamespace)), http.MethodPost)
	if len(saPosts) != 1 || decodeBody[object](t, saPosts[0].body).Metadata.Name != openbaoAccount {
		t.Errorf("service account creates in %s = %v, want the OpenBao ServiceAccount only", openbaoNamespace, saPosts)
	}
	for _, req := range filterMethod(fake.requestsOfPath(clusterRoleBindingsPath(acctCluster)), http.MethodPost) {
		if project := decodeBody[binding](t, req.body).Metadata.Labels[accountProjectKey]; project != "p-alpha" {
			t.Errorf("cluster role binding of project %q, want p-alpha only", project)
		}
	}
	if got := fake.requestsOfPath(rolesPath(acctCluster, openbaoNamespace)); len(got) != 0 {
		t.Errorf("role requests in %s = %d, want 0", openbaoNamespace, len(got))
	}
}

func TestReconcileWritesTheConfigOfAClusterIntoOpenBao(t *testing.T) {
	t.Parallel()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", nil)
	fake.setCA(testCA, 0)
	setup := newOpenBaoSetup(t, fake, func(cfg *Config) { cfg.MeterProvider = provider })

	setup.syncer.reconcile(context.Background())

	tokenRequests := fake.requestsOfPath(tokenRequestPath(acctCluster))
	if len(tokenRequests) != 1 || tokenRequests[0].method != http.MethodPost {
		t.Fatalf("token requests = %v, want one POST", tokenRequests)
	}
	wantRequest := map[string]any{
		"apiVersion": "authentication.k8s.io/v1",
		"kind":       "TokenRequest",
		"spec":       map[string]any{"expirationSeconds": float64(86400)},
	}
	if got := decodeMap(t, tokenRequests[0].body); !reflect.DeepEqual(got, wantRequest) {
		t.Errorf("token request body = %v, want %v", got, wantRequest)
	}

	requests := setup.bao.all()
	if len(requests) != 2 {
		t.Fatalf("OpenBao requests = %d, want a login and a write", len(requests))
	}
	login, write := requests[0], requests[1]
	if login.path != "/v1/auth/kubernetes/login" {
		t.Fatalf("first OpenBao request = %s, want the login", login.path)
	}
	if got, want := decodeMap(t, login.body), map[string]any{"role": baoRole, "jwt": "jwt-1"}; !reflect.DeepEqual(got, want) {
		t.Errorf("login body = %v, want %v", got, want)
	}
	if got := login.header.Get("X-Vault-Token"); got != "" {
		t.Errorf("login token header = %q, want none", got)
	}
	if write.path != "/v1/kubernetes/c-1/config" || write.method != http.MethodPost {
		t.Fatalf("second OpenBao request = %s %s, want POST /v1/kubernetes/c-1/config", write.method, write.path)
	}
	if got := write.header.Get("X-Vault-Token"); got != "bao-1" {
		t.Errorf("write token header = %q, want bao-1", got)
	}
	wantConfig := map[string]any{
		"kubernetes_host":      baoRancher + "/k8s/clusters/c-1",
		"kubernetes_ca_cert":   testCA,
		"service_account_jwt":  "sa-token-1",
		"disable_local_ca_jwt": true,
	}
	if got := decodeMap(t, write.body); !reflect.DeepEqual(got, wantConfig) {
		t.Errorf("config body = %v, want %v", got, wantConfig)
	}

	logs := setup.logs.String()
	if !strings.Contains(logs, `level=INFO msg="the OpenBao config is written" cluster=c-1 expires=2026-01-02T00:00:00Z`) {
		t.Errorf("no write line:\n%s", logs)
	}
	for _, secret := range []string{"sa-token-1", "jwt-1", "bao-1"} {
		if strings.Contains(logs, secret) {
			t.Errorf("the logs have the secret %s:\n%s", secret, logs)
		}
	}

	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	sum := findSum(t, data, "drover.sync.openbao.writes")
	if len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 1 {
		t.Fatalf("drover.sync.openbao.writes points = %v, want one point with value 1", sum.DataPoints)
	}
	attrs := sum.DataPoints[0].Attributes
	if v, _ := attrs.Value(attribute.Key("outcome")); v.AsString() != outcomeOK {
		t.Errorf("outcome = %q, want %q", v.AsString(), outcomeOK)
	}
	if v, _ := attrs.Value(attribute.Key("cluster")); v.AsString() != acctCluster {
		t.Errorf("cluster = %q, want %q", v.AsString(), acctCluster)
	}
}

func TestReconcileWritesAgainAfterHalfOfTheRealTokenLifetime(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		// cap is the longest lifetime that the API server grants, zero for
		// the requested 24h.
		cap    time.Duration
		before time.Duration
		after  time.Duration
	}{
		{name: "the requested lifetime", before: 11*time.Hour + 59*time.Minute, after: 2 * time.Minute},
		{name: "a lifetime that the API server shortens", cap: 2 * time.Hour, before: 59 * time.Minute, after: 2 * time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fake := newAccountsFake(t)
			fake.addProject("p-alpha", nil)
			fake.mu.Lock()
			fake.tokenCap = tc.cap
			fake.mu.Unlock()
			setup := newOpenBaoSetup(t, fake)
			setup.syncer.reconcile(context.Background())
			if got := setup.bao.writes(acctCluster); len(got) != 1 {
				t.Fatalf("OpenBao config writes of the first run = %d, want 1", len(got))
			}

			setup.clock.advance(tc.before)
			fake.resetRequests()
			setup.bao.reset()
			setup.syncer.reconcile(context.Background())
			if got := fake.requestsOfPath(tokenRequestPath(acctCluster)); len(got) != 0 {
				t.Errorf("token requests inside the first half = %d, want 0", len(got))
			}
			if got := setup.bao.all(); len(got) != 0 {
				t.Errorf("OpenBao requests inside the first half = %d, want 0", len(got))
			}

			setup.clock.advance(tc.after)
			setup.syncer.reconcile(context.Background())
			tokenRequests := fake.requestsOfPath(tokenRequestPath(acctCluster))
			if len(tokenRequests) != 1 {
				t.Fatalf("token requests after the first half = %d, want 1", len(tokenRequests))
			}
			spec, _ := decodeMap(t, tokenRequests[0].body)["spec"].(map[string]any)
			if got := spec["expirationSeconds"]; got != float64(86400) {
				t.Errorf("requested lifetime = %v, want 86400", got)
			}
			if got := setup.bao.logins(); len(got) != 1 {
				t.Errorf("OpenBao logins after the first half = %d, want 1", len(got))
			}
			writes := setup.bao.writes(acctCluster)
			if len(writes) != 1 {
				t.Fatalf("OpenBao config writes after the first half = %d, want 1", len(writes))
			}
			if got := decodeMap(t, writes[0].body)["service_account_jwt"]; got != "sa-token-2" {
				t.Errorf("written token = %v, want sa-token-2", got)
			}
		})
	}
}

func TestReconcileReadsTheJWTFileAtEveryLogin(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", nil)
	setup := newOpenBaoSetup(t, fake)
	setup.syncer.reconcile(context.Background())

	writeToken(t, setup.jwtFile, "jwt-2\n")
	setup.clock.advance(13 * time.Hour)
	setup.syncer.reconcile(context.Background())

	logins := setup.bao.logins()
	if len(logins) != 2 {
		t.Fatalf("OpenBao logins = %d, want 2", len(logins))
	}
	for i, want := range []string{"jwt-1", "jwt-2"} {
		if got := decodeMap(t, logins[i].body)["jwt"]; got != want {
			t.Errorf("jwt of login %d = %v, want %s", i+1, got, want)
		}
	}
}

func TestReconcileTriesAMissingMountAgainInTheNextRun(t *testing.T) {
	t.Parallel()
	for _, status := range []int{http.StatusNotFound, http.StatusBadRequest} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			fake := newAccountsFake(t)
			fake.addProject("p-alpha", nil)
			setup := newOpenBaoSetup(t, fake, func(cfg *Config) { cfg.MeterProvider = provider })
			setup.bao.setMissing(acctCluster, status)

			setup.syncer.reconcile(context.Background())

			logs := setup.logs.String()
			if !strings.Contains(logs, `level=INFO msg="the OpenBao mount does not exist yet" cluster=c-1 mount=kubernetes/c-1`) {
				t.Errorf("no info line for the missing mount:\n%s", logs)
			}
			if !strings.Contains(logs, "errors=0") {
				t.Errorf("the summary line counts the missing mount as an error:\n%s", logs)
			}
			var data metricdata.ResourceMetrics
			if err := reader.Collect(context.Background(), &data); err != nil {
				t.Fatalf("collect metrics: %v", err)
			}
			sum := findSum(t, data, "drover.sync.openbao.writes")
			if len(sum.DataPoints) != 1 {
				t.Fatalf("drover.sync.openbao.writes points = %d, want 1", len(sum.DataPoints))
			}
			if v, _ := sum.DataPoints[0].Attributes.Value(attribute.Key("outcome")); v.AsString() != outcomeMissingMount {
				t.Errorf("outcome = %q, want %q", v.AsString(), outcomeMissingMount)
			}

			setup.syncer.reconcile(context.Background())
			if got := setup.bao.writes(acctCluster); len(got) != 2 {
				t.Errorf("OpenBao config writes after the second run = %d, want 2", len(got))
			}

			setup.bao.setMissing(acctCluster, 0)
			setup.syncer.reconcile(context.Background())
			setup.syncer.reconcile(context.Background())
			if got := setup.bao.writes(acctCluster); len(got) != 3 {
				t.Errorf("OpenBao config writes after the mount exists = %d, want 3", len(got))
			}
		})
	}
}

func TestReconcileTriesAFailedLoginAgainInTheNextRun(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", nil)
	setup := newOpenBaoSetup(t, fake)
	setup.bao.setLoginStatus(http.StatusForbidden)

	setup.syncer.reconcile(context.Background())

	logs := setup.logs.String()
	if !strings.Contains(logs, `level=ERROR msg="the OpenBao login failed"`) {
		t.Errorf("no error line for the login:\n%s", logs)
	}
	if !strings.Contains(logs, "errors=1") {
		t.Errorf("the summary line does not count the failed login:\n%s", logs)
	}
	if got := fake.requestsOfPath(tokenRequestPath(acctCluster)); len(got) != 0 {
		t.Errorf("token requests after a failed login = %d, want 0", len(got))
	}

	setup.bao.setLoginStatus(0)
	setup.syncer.reconcile(context.Background())
	if got := setup.bao.writes(acctCluster); len(got) != 1 {
		t.Errorf("OpenBao config writes after the second run = %d, want 1", len(got))
	}
}

func TestReconcileWritesAgainForANewServiceAccount(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", nil)
	setup := newOpenBaoSetup(t, fake)
	setup.syncer.reconcile(context.Background())

	fake.removeServiceAccount(openbaoNamespace, openbaoAccount)
	fake.addServiceAccount(object{Metadata: objectMeta{
		Name: openbaoAccount, Namespace: openbaoNamespace, UID: "uid-new",
		Labels: map[string]string{accountRoleKey: openbaoLabel},
	}})
	setup.syncer.reconcile(context.Background())

	if got := setup.bao.writes(acctCluster); len(got) != 2 {
		t.Errorf("OpenBao config writes = %d, want 2", len(got))
	}
}

func TestReconcileSendsAnEmptyCAForAPublicRancherCertificate(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", nil)
	setup := newOpenBaoSetup(t, fake)

	setup.syncer.reconcile(context.Background())

	writes := setup.bao.writes(acctCluster)
	if len(writes) != 1 {
		t.Fatalf("OpenBao config writes = %d, want 1", len(writes))
	}
	ca, ok := decodeMap(t, writes[0].body)["kubernetes_ca_cert"]
	if !ok || ca != "" {
		t.Errorf("kubernetes_ca_cert = %v, present %v; want an empty string", ca, ok)
	}
}

func TestReconcileWritesANewRancherCAInsideTheWindow(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", nil)
	fake.setCA(testCA, 0)
	setup := newOpenBaoSetup(t, fake)
	setup.syncer.reconcile(context.Background())

	rotated := strings.Replace(testCA, "MIIB", "MIIC", 1)
	fake.setCA(rotated, 0)
	setup.clock.advance(time.Hour)
	setup.syncer.reconcile(context.Background())
	setup.syncer.reconcile(context.Background())

	writes := setup.bao.writes(acctCluster)
	if len(writes) != 2 {
		t.Fatalf("OpenBao config writes = %d, want 2", len(writes))
	}
	if got := decodeMap(t, writes[1].body)["kubernetes_ca_cert"]; got != rotated {
		t.Errorf("kubernetes_ca_cert of the second write = %v, want the new CA", got)
	}
}

func TestReconcileWritesNothingAfterAFailedRancherCARead(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", nil)
	fake.setCA(testCA, http.StatusInternalServerError)
	setup := newOpenBaoSetup(t, fake)

	setup.syncer.reconcile(context.Background())

	logs := setup.logs.String()
	if !strings.Contains(logs, `level=ERROR msg="the Rancher CA request failed"`) {
		t.Errorf("no error line for the CA read:\n%s", logs)
	}
	if !strings.Contains(logs, "errors=1") {
		t.Errorf("the summary line does not count the failed CA read:\n%s", logs)
	}
	if got := fake.requestsOfPath(tokenRequestPath(acctCluster)); len(got) != 0 {
		t.Errorf("token requests after a failed CA read = %d, want 0", len(got))
	}
	if got := setup.bao.all(); len(got) != 0 {
		t.Errorf("OpenBao requests after a failed CA read = %d, want 0", len(got))
	}

	fake.setCA(testCA, 0)
	setup.syncer.reconcile(context.Background())
	writes := setup.bao.writes(acctCluster)
	if len(writes) != 1 {
		t.Fatalf("OpenBao config writes after the second run = %d, want 1", len(writes))
	}
	if got := decodeMap(t, writes[0].body)["kubernetes_ca_cert"]; got != testCA {
		t.Errorf("kubernetes_ca_cert = %v, want the CA", got)
	}
}

func TestRefreshOpenBaoLogsInOncePerRun(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	setup := newOpenBaoSetup(t, fake)

	var run counters
	setup.syncer.refreshOpenBao(context.Background(), serviceToken, []string{"c-1", "c-2"},
		map[string]string{"c-1": "uid-1", "c-2": "uid-2"}, &run)

	if got := setup.bao.logins(); len(got) != 1 {
		t.Errorf("OpenBao logins = %d, want 1", len(got))
	}
	if got := fake.requestsOfPath(rancherCAPath); len(got) != 1 {
		t.Errorf("Rancher CA requests = %d, want 1", len(got))
	}
	for _, cluster := range []string{"c-1", "c-2"} {
		if got := fake.requestsOfPath(tokenRequestPath(cluster)); len(got) != 1 {
			t.Errorf("token requests of %s = %d, want 1", cluster, len(got))
		}
		if got := setup.bao.writes(cluster); len(got) != 1 {
			t.Errorf("OpenBao config writes of %s = %d, want 1", cluster, len(got))
		}
	}
	if run.errors != 0 {
		t.Errorf("errors = %d, want 0", run.errors)
	}
}

func TestNewChecksTheOpenBaoConfig(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	target, _ := url.Parse(fake.server.URL)
	address, _ := url.Parse("http://openbao.example.com:8200")
	rancher, _ := url.Parse(baoRancher)
	valid := func() OpenBaoConfig {
		return OpenBaoConfig{
			Address: address, AuthPath: "kubernetes", Role: baoRole, JWTFile: "/token",
			MountPrefix: "kubernetes", RancherURL: rancher, TokenTTL: 24 * time.Hour,
		}
	}
	insecure, _ := url.Parse("http://rancher.example.com")
	withPath, _ := url.Parse("https://rancher.example.com/rancher")

	cases := []struct {
		name            string
		edit            func(*OpenBaoConfig)
		serviceAccounts bool
	}{
		{name: "without the service accounts", edit: func(*OpenBaoConfig) {}},
		{name: "an http Rancher URL", edit: func(c *OpenBaoConfig) { c.RancherURL = insecure }, serviceAccounts: true},
		{name: "a Rancher URL with a path", edit: func(c *OpenBaoConfig) { c.RancherURL = withPath }, serviceAccounts: true},
		{name: "no JWT file", edit: func(c *OpenBaoConfig) { c.JWTFile = "" }, serviceAccounts: true},
		{name: "a lifetime below 10 minutes", edit: func(c *OpenBaoConfig) { c.TokenTTL = 5 * time.Minute }, serviceAccounts: true},
		{name: "an empty auth path", edit: func(c *OpenBaoConfig) { c.AuthPath = "/" }, serviceAccounts: true},
		{name: "a mount prefix with an empty segment", edit: func(c *OpenBaoConfig) { c.MountPrefix = "a//b" }, serviceAccounts: true},
		{name: "an empty role", edit: func(c *OpenBaoConfig) { c.Role = "" }, serviceAccounts: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := valid()
			tc.edit(&cfg)
			_, err := New(Config{RancherURL: target, TokenFile: "/token", Labels: []string{"team"}, ServiceAccounts: tc.serviceAccounts, OpenBao: &cfg})
			if err == nil {
				t.Error("New = nil error, want an error")
			}
		})
	}

	cfg := valid()
	if _, err := New(Config{RancherURL: target, TokenFile: "/token", ServiceAccounts: true, OpenBao: &cfg}); err != nil {
		t.Errorf("New with a valid OpenBao config = %v, want no error", err)
	}
}

// listNewProject runs the lister for the new project p-new of acctCluster,
// with the account project p-acct and the OpenBao namespace uid space in the
// account view, and no reconcile run.
func listNewProject(t *testing.T, setup *openbaoSetup, space string) {
	t.Helper()
	markedProject(setup.fake, "p-acct")
	setup.syncer.setAccounts(acctCluster, accountState{projects: []string{"p-acct"}, openbao: space})
	setup.syncer.setClusters(map[string]map[string]project{
		acctCluster: {"p-new": {ID: acctCluster + ":p-new", ClusterID: acctCluster, Name: "p-new"}},
	})
	setup.syncer.listProject(context.Background(), acctCluster, "p-new", newClusterWatch(10).patches)
}

func TestListProjectGivesANewProjectTheOpenBaoRoleAndRoleBinding(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	setup := newOpenBaoSetup(t, fake)

	listNewProject(t, setup, "uid-bao")

	nsName := accountNamespace("p-new")
	rolePosts := filterMethod(fake.requestsOfPath(rolesPath(acctCluster, nsName)), http.MethodPost)
	if len(rolePosts) != 1 {
		t.Fatalf("role create requests in %s = %d, want 1", nsName, len(rolePosts))
	}
	if got := decodeBody[role](t, rolePosts[0].body).Rules; !reflect.DeepEqual(got, wantOpenBaoRules()) {
		t.Errorf("rules = %+v, want %+v", got, wantOpenBaoRules())
	}
	var created *binding
	for _, req := range filterMethod(fake.requestsOfPath(roleBindingsPath(acctCluster, nsName)), http.MethodPost) {
		if b := decodeBody[binding](t, req.body); b.Metadata.Name == openbaoNamespace {
			created = &b
		}
	}
	if created == nil {
		t.Fatalf("no create of the role binding %s in %s", openbaoNamespace, nsName)
	}
	wantOwner := []ownerReference{{APIVersion: "v1", Kind: "Namespace", Name: openbaoNamespace, UID: "uid-bao"}}
	if !reflect.DeepEqual(created.Metadata.OwnerReferences, wantOwner) {
		t.Errorf("owner references = %+v, want %+v", created.Metadata.OwnerReferences, wantOwner)
	}

	if got := fake.requestsOfPath(projectsPath); len(got) != 0 {
		t.Errorf("project list requests = %d, want 0, because no reconcile run ran", len(got))
	}
	if got := fake.requestsOfPath(rolesPath(acctCluster, "")); len(got) != 0 {
		t.Errorf("role list requests = %d, want 0", len(got))
	}
	if got := fake.requestsOfPath(namespacePath(acctCluster, openbaoNamespace)); len(got) != 0 {
		t.Errorf("requests of the OpenBao namespace = %d, want 0", len(got))
	}
	state, _ := setup.syncer.accountsOf(acctCluster)
	if state.openbao != "uid-bao" {
		t.Errorf("OpenBao namespace uid after the lister = %q, want uid-bao", state.openbao)
	}
}

func TestListProjectGivesNoOpenBaoObjectsWithoutATrustedNamespace(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	setup := newOpenBaoSetup(t, fake)

	listNewProject(t, setup, "")

	nsName := accountNamespace("p-new")
	if got := fake.requestsOfPath(rolesPath(acctCluster, nsName)); len(got) != 0 {
		t.Errorf("role requests in %s = %d, want 0", nsName, len(got))
	}
	if got := fake.requestsOfPath(roleBindingsPath(acctCluster, nsName)); len(got) != 0 {
		t.Errorf("role binding requests in %s = %d, want 0", nsName, len(got))
	}
	if got := filterMethod(fake.requestsOfPath(serviceAccountsPath(acctCluster, nsName)), http.MethodPost); len(got) != 3 {
		t.Errorf("service account create requests = %d, want 3", len(got))
	}
}

func TestListProjectCorrectsTheOpenBaoRoleBindingOfAnExistingProject(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", nil)
	setup := newOpenBaoSetup(t, fake)
	setup.syncer.reconcile(context.Background())

	nsName := accountNamespace("p-alpha")
	fake.mutateRoleBinding(nsName, openbaoNamespace, func(b *binding) {
		b.Subjects = append(b.Subjects, subject{Kind: "ServiceAccount", Name: "intruder", Namespace: "ns-a"})
	})
	fake.resetRequests()
	setup.syncer.listProject(context.Background(), acctCluster, "p-alpha", newClusterWatch(10).patches)

	path := roleBindingsPath(acctCluster, nsName) + "/" + openbaoNamespace
	puts := filterMethod(fake.requestsOfPath(path), http.MethodPut)
	if len(puts) != 1 {
		t.Fatalf("role binding update requests = %d, want 1", len(puts))
	}
	want := []subject{{Kind: "ServiceAccount", Name: openbaoAccount, Namespace: openbaoNamespace}}
	if got := decodeBody[binding](t, puts[0].body).Subjects; !reflect.DeepEqual(got, want) {
		t.Errorf("subjects after the update = %+v, want %+v", got, want)
	}
	rolePath := rolesPath(acctCluster, nsName) + "/" + openbaoNamespace
	if got := filterMethod(fake.requestsOfPath(rolePath), http.MethodPut); len(got) != 0 {
		t.Errorf("role update requests = %d, want 0", len(got))
	}
}

func TestReconcileKeepsTheOpenBaoNamespaceUidOnlyWhileItIsTrusted(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", nil)
	fake.addProject("p-beta", nil)
	setup := newOpenBaoSetup(t, fake)

	setup.syncer.reconcile(context.Background())
	state, _ := setup.syncer.accountsOf(acctCluster)
	if want := fake.namespaceUID(openbaoNamespace); state.openbao != want || want == "" {
		t.Fatalf("OpenBao namespace uid after a trusted run = %q, want %q", state.openbao, want)
	}

	fake.mutateNamespace(openbaoNamespace, func(ns *namespace) {
		ns.Metadata.Annotations = map[string]string{projectAnnotation: acctCluster + ":p-beta"}
	})
	setup.syncer.reconcile(context.Background())
	state, _ = setup.syncer.accountsOf(acctCluster)
	if state.openbao != "" {
		t.Errorf("OpenBao namespace uid after an untrusted run = %q, want empty", state.openbao)
	}
}
