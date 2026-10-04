package projectsync

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const (
	deployStatement  = `{"name":"deploy","jwt":{"issuer":"github","claims":{"repository_id":"123456789"}},"role":"project-member"}`
	rotatorStatement = `{"name":"rotator","aws":{"arn":"arn:aws:iam::123456789012:role/team/rotator"},"role":"read-only"}`

	oneStatement  = `{"statements":[` + deployStatement + `]}`
	twoStatements = `{"statements":[` + deployStatement + `,` + rotatorStatement + `]}`
)

// loginTokenWant returns the token fields of a login role of the fixture
// rules with the ACL policy of project and role.
func loginTokenWant(project, role string) map[string]any {
	return map[string]any{
		"token_policies":          []any{"kubernetes-c-1-" + project + "-" + role},
		"token_type":              "batch",
		"token_ttl":               "5m",
		"token_max_ttl":           "5m",
		"token_bound_cidrs":       []any{},
		"token_explicit_max_ttl":  "0",
		"token_num_uses":          float64(0),
		"token_period":            "0",
		"token_no_default_policy": true,
	}
}

func withFields(base, more map[string]any) map[string]any {
	out := make(map[string]any, len(base)+len(more))
	for key, value := range base {
		out[key] = value
	}
	for key, value := range more {
		out[key] = value
	}
	return out
}

func projectPatches(fake *accountsFake, name string) []recorded {
	return filterMethod(fake.requestsOfPath(projectPath(name)), http.MethodPatch)
}

func TestReconcileWritesTheLoginRolesOfTheValidStatements(t *testing.T) {
	t.Parallel()
	document := `{"statements":[` + deployStatement + `,` + rotatorStatement +
		`,{"name":"bad","jwt":{"issuer":"github","claims":{"repository_id":"1"}},"role":"admin"}]}`
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", trusted(document))
	setup := newTrustSetup(t, fake)

	setup.syncer.reconcile(context.Background())

	jwtWant := withFields(loginTokenWant("p-alpha", "project-member"), map[string]any{
		"role_type":       "jwt",
		"bound_audiences": []any{"https://openbao.example.com"},
		// The statement does not contain ref, so the service binds the role
		// to every allowed value of it.
		"bound_claims":            map[string]any{"repository_id": "123456789", "ref": []any{"refs/heads/main", "refs/heads/release"}},
		"bound_claims_type":       "string",
		"bound_subject":           "",
		"user_claim":              "repository_id",
		"user_claim_json_pointer": false,
		"groups_claim":            "",
		"claim_mappings":          map[string]any{},
		"expiration_leeway":       float64(0),
		"not_before_leeway":       float64(0),
		"clock_skew_leeway":       float64(0),
	})
	if got, _ := setup.bao.login(githubMount, "p-alpha_deploy"); !reflect.DeepEqual(got, jwtWant) {
		t.Errorf("JWT role =\n%v\nwant\n%v", got, jwtWant)
	}
	awsWant := withFields(loginTokenWant("p-alpha", "read-only"), map[string]any{
		"auth_type":               "iam",
		"bound_iam_principal_arn": []any{"arn:aws:iam::123456789012:role/rotator"},
		"resolve_aws_unique_ids":  false,
		"inferred_entity_type":    "",
	})
	if got, _ := setup.bao.login(awsAuthPath, "p-alpha_rotator"); !reflect.DeepEqual(got, awsWant) {
		t.Errorf("AWS role =\n%v\nwant\n%v", got, awsWant)
	}
	if got := setup.bao.loginNames(githubMount); !slices.Equal(got, []string{"p-alpha_deploy"}) {
		t.Errorf("JWT roles = %v, want only p-alpha_deploy", got)
	}

	policyAt, roleAt := -1, -1
	for i, req := range setup.bao.all() {
		if req.method == http.MethodPut && req.path == "/v1/sys/policies/acl/kubernetes-c-1-p-alpha-project-member" {
			policyAt = i
		}
		if req.method == http.MethodPost && req.path == "/v1/"+githubMount+"/role/p-alpha_deploy" {
			roleAt = i
		}
	}
	if policyAt == -1 || roleAt < policyAt {
		t.Errorf("policy write at request %d, login role write at %d; want the policy first", policyAt, roleAt)
	}

	status, _ := fake.projectAnnotation("p-alpha", trustStatusKey)
	want := `{"observedHash":"` + trustHash(document) + `","observedAt":"2026-01-01T00:00:00Z","statements":[` +
		`{"name":"deploy","ready":true,"login":{"path":"auth/jwt/github","role":"p-alpha_deploy"}},` +
		`{"name":"rotator","ready":true,"login":{"path":"auth/aws","role":"p-alpha_rotator"}},` +
		`{"name":"bad","ready":false,"reason":"InvalidRole","message":"the role is not project-owner, project-member, or read-only"}]}`
	if status != want {
		t.Errorf("status =\n%s\nwant\n%s", status, want)
	}

	logs := setup.logs.String()
	if !strings.Contains(logs, "errors=0") {
		t.Errorf("the run has errors:\n%s", logs)
	}
	if !strings.Contains(logs, `msg="the trust statement is not ready" cluster=c-1 project=p-alpha index=2 reason=InvalidRole statement=bad`) {
		t.Errorf("no line for the invalid statement:\n%s", logs)
	}
	if strings.Contains(logs, "123456789\"") || strings.Contains(logs, "team/rotator") {
		t.Errorf("the logs have a value of the document:\n%s", logs)
	}
}

func TestReconcileWritesTheTrustStatusOnlyOnADifference(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", trusted(twoStatements))
	setup := newTrustSetup(t, fake)
	setup.syncer.reconcile(context.Background())
	first, _ := fake.projectAnnotation("p-alpha", trustStatusKey)

	fake.resetRequests()
	setup.bao.reset()
	setup.clock.advance(time.Minute)
	setup.syncer.reconcile(context.Background())

	if got := projectPatches(fake, "p-alpha"); len(got) != 0 {
		t.Errorf("status patches of an equal status = %d, want 0", len(got))
	}
	if second, _ := fake.projectAnnotation("p-alpha", trustStatusKey); second != first {
		t.Errorf("status after the second run =\n%s\nwant\n%s", second, first)
	}
	for _, req := range setup.bao.loginRequests() {
		if req.method != http.MethodGet {
			t.Errorf("%s %s inside the write window", req.method, req.path)
		}
	}

	fake.editProject("p-alpha", func(p *storedProject) { p.annotations[trustKey] = oneStatement })
	fake.resetRequests()
	setup.bao.reset()
	setup.clock.advance(time.Minute)
	setup.syncer.reconcile(context.Background())

	if got := projectPatches(fake, "p-alpha"); len(got) != 1 {
		t.Fatalf("status patches of a changed document = %d, want 1", len(got))
	}
	third, _ := fake.projectAnnotation("p-alpha", trustStatusKey)
	want := `{"observedHash":"` + trustHash(oneStatement) + `","observedAt":"2026-01-01T00:02:00Z","statements":[` +
		`{"name":"deploy","ready":true,"login":{"path":"auth/jwt/github","role":"p-alpha_deploy"}}]}`
	if third != want {
		t.Errorf("status =\n%s\nwant\n%s", third, want)
	}
	if _, ok := setup.bao.login(awsAuthPath, "p-alpha_rotator"); ok {
		t.Error("the role of the removed statement exists, want it deleted")
	}
	if got := countRequests(setup.bao.all(), http.MethodDelete, "/v1/auth/aws/role/p-alpha_rotator"); got != 1 {
		t.Errorf("deletes of the role of the removed statement = %d, want 1", got)
	}
	if got := countRequests(setup.bao.all(), http.MethodPost, "/v1/"+githubMount+"/role/p-alpha_deploy"); got != 0 {
		t.Errorf("writes of an unchanged role inside the window = %d, want 0", got)
	}
}

// loginChanges returns the sum of drover.sync.openbao.changes of the kind
// jwt-role.
func loginChanges(t *testing.T, reader *sdkmetric.ManualReader) int64 {
	t.Helper()
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	var total int64
	for _, point := range findSum(t, data, "drover.sync.openbao.changes").DataPoints {
		if v, _ := point.Attributes.Value(attribute.Key("kind")); v.AsString() == kindJWTRole {
			total += point.Value
		}
	}
	return total
}

func TestReconcileRefreshesAnUnchangedLoginRoleWithoutAChange(t *testing.T) {
	t.Parallel()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", trusted(oneStatement))
	setup := newTrustSetup(t, fake, func(cfg *Config) { cfg.MeterProvider = provider })
	setup.syncer.reconcile(context.Background())
	if got := loginChanges(t, reader); got != 1 {
		t.Fatalf("login role changes after the create = %d, want 1", got)
	}
	rolePath := "/v1/" + githubMount + "/role/p-alpha_deploy"

	setup.bao.reset()
	setup.clock.advance(openbaoVerifyAfter)
	offset := len(setup.logs.String())
	setup.syncer.reconcile(context.Background())

	if got := countRequests(setup.bao.all(), http.MethodPost, rolePath); got != 1 {
		t.Errorf("writes of the role after the window = %d, want 1", got)
	}
	logs := setup.logs.String()[offset:]
	if !strings.Contains(logs, `level=DEBUG msg="the login role is refreshed" cluster=c-1 project=p-alpha statement=deploy mount=auth/jwt/github`) {
		t.Errorf("no debug line for the refresh:\n%s", logs)
	}
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, "level=INFO") && strings.Contains(line, "p-alpha_deploy") {
			t.Errorf("an info line for the refresh: %s", line)
		}
	}
	if got := loginChanges(t, reader); got != 1 {
		t.Errorf("login role changes after the refresh = %d, want 1", got)
	}

	changed := `{"statements":[{"name":"deploy","jwt":{"issuer":"github","claims":{"repository_id":"123456789","ref":"refs/heads/main"}},"role":"project-member"}]}`
	fake.editProject("p-alpha", func(p *storedProject) { p.annotations[trustKey] = changed })
	offset = len(setup.logs.String())
	setup.syncer.reconcile(context.Background())

	if !strings.Contains(setup.logs.String()[offset:], `level=INFO msg="OpenBao object changed" cluster=c-1 kind=jwt-role action=write name=p-alpha_deploy`) {
		t.Errorf("no change line for a changed body:\n%s", setup.logs.String()[offset:])
	}
	if got := loginChanges(t, reader); got != 2 {
		t.Errorf("login role changes after a changed body = %d, want 2", got)
	}
}

func TestListProjectDeletesTheLoginRoleOfARemovedStatement(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", trusted(twoStatements))
	setup := newTrustSetup(t, fake)
	setup.syncer.reconcile(context.Background())

	fake.editProject("p-alpha", func(p *storedProject) { p.annotations[trustKey] = oneStatement })
	fake.resetRequests()
	setup.bao.reset()
	listTrustProject(t, setup, "p-alpha")

	if _, ok := setup.bao.login(awsAuthPath, "p-alpha_rotator"); ok {
		t.Error("the role of the removed statement exists, want it deleted")
	}
	if _, ok := setup.bao.login(githubMount, "p-alpha_deploy"); !ok {
		t.Error("the role of the kept statement is gone")
	}
	for _, req := range setup.bao.loginRequests() {
		if req.query.Get("list") == "true" {
			t.Errorf("LIST %s in the event path", req.path)
		}
	}
	if got := projectPatches(fake, "p-alpha"); len(got) != 1 {
		t.Errorf("status patches = %d, want 1", len(got))
	}
	status, _ := fake.projectAnnotation("p-alpha", trustStatusKey)
	if strings.Contains(status, "rotator") {
		t.Errorf("status has the removed statement:\n%s", status)
	}
}

// trustProjectFrame returns the Project object p-alpha of acctCluster with
// annotations, as a project watch event has it.
func trustProjectFrame(t *testing.T, annotations map[string]string) json.RawMessage {
	t.Helper()
	var object projectObject
	object.Metadata.Name = "p-alpha"
	object.Metadata.Namespace = acctCluster
	object.Metadata.ResourceVersion = "2"
	object.Metadata.Annotations = annotations
	object.Spec.ClusterName = acctCluster
	object.Spec.DisplayName = "p-alpha"
	data, err := json.Marshal(object)
	if err != nil {
		t.Fatalf("encode the project: %v", err)
	}
	return data
}

func TestProjectWatchQueuesNoWorkForTheOwnStatusWrite(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", trusted(oneStatement))
	setup := newTrustSetup(t, fake)
	setup.syncer.reconcile(context.Background())
	status, ok := fake.projectAnnotation("p-alpha", trustStatusKey)
	if !ok {
		t.Fatal("the run wrote no status")
	}

	watches := setup.syncer.newWatchSet()
	watch := newClusterWatch(10)
	watches.watchers[acctCluster] = watch
	apply := func(annotations map[string]string) {
		t.Helper()
		item, err := setup.syncer.decodeProject(trustProjectFrame(t, annotations))
		if err != nil {
			t.Fatalf("decode the project: %v", err)
		}
		setup.syncer.applyProject(context.Background(), watches, watchModified, item)
	}

	apply(map[string]string{trustKey: oneStatement, trustStatusKey: status})
	if !watch.projects.idle() {
		t.Error("the event of the own status write queued the project, want no work")
	}
	if got := setup.syncer.projectsOf(acctCluster)["p-alpha"].trustStatus; got != status {
		t.Errorf("status in the snapshot = %q, want the written status", got)
	}
	if strings.Contains(setup.logs.String(), `msg="project changed"`) {
		t.Errorf("the status write is a project change:\n%s", setup.logs.String())
	}

	apply(map[string]string{trustKey: twoStatements, trustStatusKey: status})
	if watch.projects.idle() {
		t.Error("a new trust document queued no work, want the project in the queue")
	}
	if !strings.Contains(setup.logs.String(), "trust_changed=true") {
		t.Errorf("the change line has no trust_changed=true:\n%s", setup.logs.String())
	}
}

func TestReconcileDeletesTheStaleLoginRoles(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	markedProject(fake, "p-acct")
	fake.addProject("p-alpha", trusted(oneStatement))
	fake.addUnlistedProject("p-new", nil)
	setup := newTrustSetup(t, fake)
	policyOf := func(project, role string) map[string]any {
		return map[string]any{"token_policies": []any{"kubernetes-c-1-" + project + "-" + role}}
	}
	// The project of this role is known, but it has no statement of that name.
	setup.bao.setLogin(githubMount, "p-alpha_old", policyOf("p-alpha", "read-only"))
	// Rancher answers 404 for the project of this role.
	setup.bao.setLogin(githubMount, "p-gone_ci", policyOf("p-gone", "project-owner"))
	// The project of this role exists, but the project list does not have it.
	setup.bao.setLogin(githubMount, "p-new_ci", policyOf("p-new", "read-only"))
	// The project of this role is known, but the run does not check it.
	setup.bao.setLogin(awsAuthPath, "p-acct_ci", policyOf("p-acct", "read-only"))
	// Another writer owns these roles. The first name is not a name of the
	// service, and the policy of the second is not a policy of the service.
	setup.bao.setLogin(awsAuthPath, "admin-deploy", map[string]any{"token_policies": []any{"admin"}})
	setup.bao.setLogin(awsAuthPath, "admin_deploy", map[string]any{"token_policies": []any{"admin"}})
	// The policy of this role names another project than the role name.
	setup.bao.setLogin(githubMount, "p-other_ci", policyOf("p-gone", "project-owner"))

	setup.syncer.reconcile(context.Background())

	if got := setup.bao.loginNames(githubMount); !slices.Equal(got, []string{"p-alpha_deploy", "p-new_ci", "p-other_ci"}) {
		t.Errorf("JWT roles = %v, want p-alpha_deploy, p-new_ci, and p-other_ci", got)
	}
	if got := setup.bao.loginNames(awsAuthPath); !slices.Equal(got, []string{"admin-deploy", "admin_deploy", "p-acct_ci"}) {
		t.Errorf("AWS roles = %v, want admin-deploy, admin_deploy, and p-acct_ci", got)
	}
	if got := countRequests(setup.bao.all(), http.MethodGet, "/v1/auth/aws/role/admin-deploy"); got != 0 {
		t.Errorf("reads of admin-deploy = %d, want 0, because the name is not a name of the service", got)
	}
	for _, project := range []string{"p-gone", "p-new"} {
		if got := fake.requestsOfPath(projectsPath + "/" + acctCluster + ":" + project); len(got) != 1 {
			t.Errorf("project requests for %s = %d, want 1", project, len(got))
		}
	}
	for _, name := range []string{"p-alpha_old", "p-acct_ci"} {
		if got := countRequests(setup.bao.all(), http.MethodGet, "/v1/"+githubMount+"/role/"+name) +
			countRequests(setup.bao.all(), http.MethodGet, "/v1/auth/aws/role/"+name); got != 0 {
			t.Errorf("reads of %s = %d, want 0, because the name names a known project", name, got)
		}
	}
	logs := setup.logs.String()
	for _, line := range []string{
		`msg="the login role has no policy of the service, so the service keeps it" mount=auth/aws name=admin_deploy`,
		`msg="the login role has no policy of the service, so the service keeps it" mount=auth/jwt/github name=p-other_ci`,
		`level=DEBUG msg="the login roles have no name of the service, so the service keeps them" count=1`,
	} {
		if !strings.Contains(logs, line) {
			t.Errorf("no line %s:\n%s", line, logs)
		}
	}
	if !strings.Contains(logs, "errors=0") {
		t.Errorf("the run has errors:\n%s", logs)
	}
}

func TestReconcileGivesMethodDisabledForAMissingAuthMount(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", trusted(twoStatements))
	setup := newOpenBaoSetup(t, fake, withTrust(trustRulesPath))
	setup.bao.addAuthMount(awsAuthPath)

	setup.syncer.reconcile(context.Background())

	status, _ := fake.projectAnnotation("p-alpha", trustStatusKey)
	want := `{"observedHash":"` + trustHash(twoStatements) + `","observedAt":"2026-01-01T00:00:00Z","statements":[` +
		`{"name":"deploy","ready":false,"reason":"MethodDisabled","message":"the login method is not enabled"},` +
		`{"name":"rotator","ready":true,"login":{"path":"auth/aws","role":"p-alpha_rotator"}}]}`
	if status != want {
		t.Errorf("status =\n%s\nwant\n%s", status, want)
	}
	if got := countRequests(setup.bao.all(), http.MethodPost, "/v1/"+githubMount+"/role/p-alpha_deploy"); got != 0 {
		t.Errorf("writes to the missing mount = %d, want 0, because the list found no mount", got)
	}
	if !strings.Contains(setup.logs.String(), "errors=0") {
		t.Errorf("the run has errors:\n%s", setup.logs.String())
	}

	fake.resetRequests()
	setup.bao.reset()
	listTrustProject(t, setup, "p-alpha")

	if got := countRequests(setup.bao.all(), http.MethodPost, "/v1/"+githubMount+"/role/p-alpha_deploy"); got != 1 {
		t.Errorf("writes of the event path = %d, want 1", got)
	}
	if got := projectPatches(fake, "p-alpha"); len(got) != 0 {
		t.Errorf("status patches = %d, want 0, because the status is the same", len(got))
	}
	if logs := setup.logs.String(); strings.Contains(logs, "level=ERROR") || strings.Contains(logs, "write failed") {
		t.Errorf("a missing mount is an error:\n%s", logs)
	}
}

func TestReconcileRemovesTheTrustStatusWithTheTrustAnnotation(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", trusted(twoStatements))
	setup := newTrustSetup(t, fake)
	setup.syncer.reconcile(context.Background())

	fake.editProject("p-alpha", func(p *storedProject) { delete(p.annotations, trustKey) })
	fake.resetRequests()
	setup.syncer.reconcile(context.Background())

	patches := projectPatches(fake, "p-alpha")
	if len(patches) != 1 {
		t.Fatalf("status patches = %d, want 1", len(patches))
	}
	if want := `{"metadata":{"annotations":{"drover-trust-status":null}}}`; patches[0].body != want {
		t.Errorf("patch body = %s, want %s", patches[0].body, want)
	}
	if got := patches[0].header.Get("Content-Type"); got != mergePatchType {
		t.Errorf("content type = %q, want %q", got, mergePatchType)
	}
	if _, ok := fake.projectAnnotation("p-alpha", trustStatusKey); ok {
		t.Error("the status annotation exists, want it removed")
	}
	if got := append(setup.bao.loginNames(githubMount), setup.bao.loginNames(awsAuthPath)...); len(got) != 0 {
		t.Errorf("login roles = %v, want none", got)
	}

	fake.resetRequests()
	setup.syncer.reconcile(context.Background())
	if got := projectPatches(fake, "p-alpha"); len(got) != 0 {
		t.Errorf("status patches without a status = %d, want 0", len(got))
	}
}

func TestReconcileWritesTheReasonOfAnInvalidDocument(t *testing.T) {
	t.Parallel()
	document := `{"statements": [{"name": "secret-value"`
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", trusted(document))
	setup := newTrustSetup(t, fake)

	setup.syncer.reconcile(context.Background())

	status, _ := fake.projectAnnotation("p-alpha", trustStatusKey)
	want := `{"observedHash":"` + trustHash(document) + `","observedAt":"2026-01-01T00:00:00Z",` +
		`"error":{"reason":"InvalidJSON","message":"the annotation value is not valid JSON"}}`
	if status != want {
		t.Errorf("status =\n%s\nwant\n%s", status, want)
	}
	logs := setup.logs.String()
	if !strings.Contains(logs, `msg="the trust document is not valid" cluster=c-1 project=p-alpha reason=InvalidJSON`) {
		t.Errorf("no line for the invalid document:\n%s", logs)
	}
	if strings.Contains(logs, "secret-value") {
		t.Errorf("the logs have the document:\n%s", logs)
	}
}

func TestReconcileKeepsTheRoleOfAProjectWhoseNameStartsWithAnotherProjectName(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p", trusted(`{"statements":[{"name":"alpha-deploy","jwt":{"issuer":"github","claims":{"repository_id":"1"}},"role":"read-only"}]}`))
	fake.addProject("p-alpha", trusted(oneStatement))
	setup := newTrustSetup(t, fake)

	setup.syncer.reconcile(context.Background())
	setup.clock.advance(openbaoVerifyAfter)
	setup.syncer.reconcile(context.Background())

	if got := setup.bao.loginNames(githubMount); !slices.Equal(got, []string{"p-alpha_deploy", "p_alpha-deploy"}) {
		t.Errorf("JWT roles = %v, want p-alpha_deploy and p_alpha-deploy", got)
	}
	for _, req := range setup.bao.loginRequests() {
		if req.method == http.MethodDelete {
			t.Errorf("DELETE %s, want no delete", req.path)
		}
	}
	for project, role := range map[string]string{"p": "p_alpha-deploy", "p-alpha": "p-alpha_deploy"} {
		status, _ := fake.projectAnnotation(project, trustStatusKey)
		if !strings.Contains(status, `"ready":true,"login":{"path":"auth/jwt/github","role":"`+role+`"}`) {
			t.Errorf("status of %s = %s, want the ready role %s", project, status, role)
		}
	}
	if logs := setup.logs.String(); strings.Contains(logs, "the login role name is not unique") || strings.Contains(logs, "errors=1") {
		t.Errorf("the run found a name of two projects, or an error:\n%s", logs)
	}
}

func TestRefreshTrustGivesWriteFailedForAProjectNameInTwoClusters(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", trusted(oneStatement))
	setup := newTrustSetup(t, fake)
	setup.syncer.reconcile(context.Background())
	rolePath := "/v1/" + githubMount + "/role/p-alpha_deploy"
	if _, ok := setup.bao.login(githubMount, "p-alpha_deploy"); !ok {
		t.Fatal("the first run wrote no role")
	}

	// The cluster c-2 also has a project p-alpha.
	projects := map[string]map[string]project{
		acctCluster: setup.syncer.projectsOf(acctCluster),
		"c-2":       {"p-alpha": {}},
	}
	ready := map[string]openbaoTarget{acctCluster: {tenants: []string{"p-alpha"}}}
	setup.bao.reset()
	errs := setup.syncer.refreshTrust(context.Background(), serviceToken, []string{acctCluster, "c-2"}, ready, projects, setup.syncer.trustRules())

	if errs != 0 {
		t.Errorf("errors = %d, want 0", errs)
	}
	status, _ := fake.projectAnnotation("p-alpha", trustStatusKey)
	if !strings.Contains(status, `{"name":"deploy","ready":false,"reason":"WriteFailed"`) {
		t.Errorf("status = %s, want WriteFailed for deploy", status)
	}
	if got := countRequests(setup.bao.all(), http.MethodPost, rolePath); got != 0 {
		t.Errorf("writes of the role = %d, want 0", got)
	}
	// The stale pass takes the cluster of the role from its first token
	// policy.
	if got := countRequests(setup.bao.all(), http.MethodGet, rolePath); got != 1 {
		t.Errorf("reads of the role = %d, want 1", got)
	}
	if _, ok := setup.bao.login(githubMount, "p-alpha_deploy"); ok {
		t.Error("the role exists, want it deleted")
	}
	if !strings.Contains(setup.logs.String(), "the login role name is not unique") {
		t.Errorf("no line for the name of two projects:\n%s", setup.logs.String())
	}
}

func TestRefreshTrustTakesTheProjectFromTheSnapshot(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", trusted(twoStatements))
	setup := newTrustSetup(t, fake)
	setup.syncer.reconcile(context.Background())
	listed := setup.syncer.snapshot()
	ready := map[string]openbaoTarget{acctCluster: {tenants: []string{"p-alpha"}}}
	rotatorPath := "/v1/auth/aws/role/p-alpha_rotator"

	// A project event after the project list of the run narrows the
	// document.
	fake.editProject("p-alpha", func(p *storedProject) { p.annotations[trustKey] = oneStatement })
	listTrustProject(t, setup, "p-alpha")
	if _, ok := setup.bao.login(awsAuthPath, "p-alpha_rotator"); ok {
		t.Fatal("the event kept the role of the removed statement")
	}
	// The project watch stores the status that the event wrote.
	watchProject(t, setup, "p-alpha")
	setup.bao.reset()
	fake.resetRequests()

	errs := setup.syncer.refreshTrust(context.Background(), serviceToken, []string{acctCluster}, ready, listed, setup.syncer.trustRules())

	if errs != 0 {
		t.Errorf("errors = %d, want 0", errs)
	}
	if got := countRequests(setup.bao.all(), http.MethodPost, rotatorPath); got != 0 {
		t.Errorf("writes of the removed role = %d, want 0", got)
	}
	if _, ok := setup.bao.login(awsAuthPath, "p-alpha_rotator"); ok {
		t.Error("the run wrote the role of the removed statement again")
	}
	if got := projectPatches(fake, "p-alpha"); len(got) != 0 {
		t.Errorf("status patches = %d, want 0, because the event wrote the status", len(got))
	}

	// A project event deletes the project from the snapshot.
	if !setup.syncer.deleteProject(acctCluster, "p-alpha") {
		t.Fatalf("the snapshot has no cluster %s", acctCluster)
	}
	setup.bao.reset()
	fake.resetRequests()

	errs = setup.syncer.refreshTrust(context.Background(), serviceToken, []string{acctCluster}, ready, listed, setup.syncer.trustRules())

	if errs != 0 {
		t.Errorf("errors = %d, want 0", errs)
	}
	for _, req := range setup.bao.loginRequests() {
		if req.method != http.MethodGet {
			t.Errorf("%s %s for a project that the snapshot does not have", req.method, req.path)
		}
	}
	if got := projectPatches(fake, "p-alpha"); len(got) != 0 {
		t.Errorf("status patches = %d, want 0", len(got))
	}
}

func TestProjectEventDeletesEachRoleOfALongStatusOnce(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", trusted(oneStatement))
	setup := newTrustSetup(t, fake)
	setup.syncer.reconcile(context.Background())

	entries := make([]string, 0, 3000)
	for i := range 3000 {
		entries = append(entries, fmt.Sprintf(`{"name":"s","ready":true,"login":{"path":"auth/aws","role":"p-alpha_s%d"}}`, i%10))
	}
	// The status is above the size limit of a status from Rancher, so the
	// test puts it into the snapshot directly.
	item := setup.syncer.projectsOf(acctCluster)["p-alpha"]
	item.trustStatus = `{"statements":[` + strings.Join(entries, ",") + `]}`
	if !setup.syncer.setProject(acctCluster, "p-alpha", item) {
		t.Fatalf("the snapshot has no cluster %s", acctCluster)
	}
	setup.bao.reset()
	offset := len(setup.logs.String())

	setup.syncer.keepProjectTrust(context.Background(), serviceToken, acctCluster, "p-alpha")

	limit := setup.syncer.trustRules().maxStatements + 1
	deletes := 0
	for _, req := range setup.bao.loginRequests() {
		if req.method == http.MethodDelete {
			deletes++
		}
	}
	if deletes == 0 || deletes > limit {
		t.Errorf("delete requests = %d, want 1 to %d", deletes, limit)
	}
	if lines := strings.Count(setup.logs.String()[offset:], "kind=aws-role action=delete"); lines != deletes {
		t.Errorf("delete lines = %d, want %d", lines, deletes)
	}
}

func TestProjectEventKeepsTheRolesOfAStatusThatTheProjectDoesNotOwn(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", trusted(oneStatement))
	fake.addProject("p-beta", trusted(oneStatement))
	setup := newTrustSetup(t, fake)
	setup.syncer.reconcile(context.Background())

	keys := []loginKey{
		// The role of another project.
		{mount: githubMount, name: "p-beta_deploy"},
		// A mount that the rules file does not have.
		{mount: "auth/jwt/other", name: "p-alpha_deploy"},
		// A role name with path text.
		{mount: githubMount, name: "p-alpha_deploy/../../../../sys/policies/acl/admin"},
	}
	run := &trustRun{rules: setup.syncer.trustRules(), owners: newProjectNames(setup.syncer.snapshot())}
	if !setup.syncer.ownsLogin(run, acctCluster, "p-alpha", loginKey{mount: githubMount, name: "p-alpha_deploy"}) {
		t.Error("p-alpha does not own its own role, want it to own the role")
	}
	entries := make([]string, 0, len(keys))
	for _, key := range keys {
		if setup.syncer.ownsLogin(run, acctCluster, "p-alpha", key) {
			t.Errorf("p-alpha owns %s %s, want no owner", key.mount, key.name)
		}
		data, err := json.Marshal(statementStatus{Name: "x", Ready: true, Login: &loginRef{Path: key.mount, Role: key.name}})
		if err != nil {
			t.Fatalf("encode the entry: %v", err)
		}
		entries = append(entries, string(data))
	}
	status := `{"statements":[` + strings.Join(entries, ",") + `]}`
	fake.editProject("p-alpha", func(p *storedProject) { p.annotations[trustStatusKey] = status })
	setup.bao.reset()

	listTrustProject(t, setup, "p-alpha")

	for _, req := range setup.bao.all() {
		if req.method == http.MethodDelete {
			t.Errorf("DELETE %s, want no delete", req.path)
		}
	}
	if _, ok := setup.bao.login(githubMount, "p-beta_deploy"); !ok {
		t.Error("the role of p-beta is gone")
	}
}

func TestReconcileKeepsTheRoleOfAFailedWrite(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", trusted(oneStatement))
	setup := newTrustSetup(t, fake)
	setup.syncer.reconcile(context.Background())
	before, ok := setup.bao.login(githubMount, "p-alpha_deploy")
	if !ok {
		t.Fatal("the first run wrote no role")
	}

	changed := `{"statements":[{"name":"deploy","jwt":{"issuer":"github","claims":{"repository_id":"123456789","ref":"refs/heads/main"}},"role":"project-member"}]}`
	fake.editProject("p-alpha", func(p *storedProject) { p.annotations[trustKey] = changed })
	setup.bao.fail(githubMount+"/role/p-alpha_deploy", http.StatusInternalServerError)
	setup.bao.reset()
	offset := len(setup.logs.String())
	setup.syncer.reconcile(context.Background())

	status, _ := fake.projectAnnotation("p-alpha", trustStatusKey)
	want := `{"observedHash":"` + trustHash(changed) + `","observedAt":"2026-01-01T00:00:00Z","statements":[` +
		`{"name":"deploy","ready":false,"reason":"WriteFailed","message":"the login role write in OpenBao failed"}]}`
	if status != want {
		t.Errorf("status =\n%s\nwant\n%s", status, want)
	}
	if after, _ := setup.bao.login(githubMount, "p-alpha_deploy"); !reflect.DeepEqual(after, before) {
		t.Errorf("role after the failed write =\n%v\nwant the role of the first run\n%v", after, before)
	}
	for _, req := range setup.bao.loginRequests() {
		if req.method == http.MethodDelete {
			t.Errorf("DELETE %s, want no delete", req.path)
		}
	}
	logs := setup.logs.String()[offset:]
	if !strings.Contains(logs, `msg="the OpenBao jwt-role write failed" cluster=c-1 name=p-alpha_deploy`) || !strings.Contains(logs, "errors=1") {
		t.Errorf("no error line and no error count for the failed write:\n%s", logs)
	}
}

func TestReconcileLeavesTheMountOfARemovedIssuer(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile(trustRulesPath)
	if err != nil {
		t.Fatalf("read the rules: %v", err)
	}
	path := filepath.Join(t.TempDir(), "rules.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write the rules: %v", err)
	}
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", trusted(twoStatements))
	setup := newTrustSetup(t, fake, withTrust(path))
	setup.syncer.reconcile(context.Background())

	var rules map[string]any
	if err := json.Unmarshal(data, &rules); err != nil {
		t.Fatalf("decode the rules: %v", err)
	}
	delete(rules["jwtIssuers"].(map[string]any), "github")
	if err := os.WriteFile(path, must(json.Marshal(rules)), 0o600); err != nil {
		t.Fatalf("write the rules: %v", err)
	}
	setup.bao.reset()
	offset := len(setup.logs.String())
	setup.syncer.reconcile(context.Background())

	status, _ := fake.projectAnnotation("p-alpha", trustStatusKey)
	want := `{"observedHash":"` + trustHash(twoStatements) + `","observedAt":"2026-01-01T00:00:00Z","statements":[` +
		`{"name":"deploy","ready":false,"reason":"UnknownIssuer","message":"the JWT issuer is not configured"},` +
		`{"name":"rotator","ready":true,"login":{"path":"auth/aws","role":"p-alpha_rotator"}}]}`
	if status != want {
		t.Errorf("status =\n%s\nwant\n%s", status, want)
	}
	for _, req := range setup.bao.all() {
		if strings.HasPrefix(req.path, "/v1/"+githubMount+"/") {
			t.Errorf("%s %s, want no request to the mount of the removed issuer", req.method, req.path)
		}
	}
	if _, ok := setup.bao.login(githubMount, "p-alpha_deploy"); !ok {
		t.Error("the role of the removed issuer is gone, want it kept")
	}
	if logs := setup.logs.String()[offset:]; !strings.Contains(logs, "errors=0") {
		t.Errorf("the run has errors:\n%s", logs)
	}
}

// newTrustGaugeSetup returns a trust setup whose meter provider has a manual
// reader.
func newTrustGaugeSetup(t *testing.T, fake *accountsFake) (*openbaoSetup, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	return newTrustSetup(t, fake, func(cfg *Config) { cfg.MeterProvider = provider }), reader
}

// trustReadiness returns the points of drover.sync.trust.statements.ready of
// the project name, each as its encoded attributes and its value, sorted.
func trustReadiness(t *testing.T, reader *sdkmetric.ManualReader, name string) []string {
	t.Helper()
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	gauge, _ := findGauge(t, data, "drover.sync.trust.statements.ready")
	var out []string
	for _, point := range gauge.DataPoints {
		if project, _ := point.Attributes.Value("project"); project.AsString() == name {
			out = append(out, fmt.Sprintf("%s %d", point.Attributes.Encoded(attribute.DefaultEncoder()), point.Value))
		}
	}
	slices.Sort(out)
	return out
}

func TestReconcileExportsOneTrustPointPerStatement(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", trusted(twoStatements))
	setup, reader := newTrustGaugeSetup(t, fake)

	setup.syncer.reconcile(context.Background())

	want := []string{
		"drover.cluster=c-1,project=p-alpha,statement=deploy 1",
		"drover.cluster=c-1,project=p-alpha,statement=rotator 1",
	}
	if got := trustReadiness(t, reader, "p-alpha"); !slices.Equal(got, want) {
		t.Errorf("trust points =\n%v\nwant\n%v", got, want)
	}
}

func TestReconcileExportsTheReasonOfATrustStatementThatIsNotReady(t *testing.T) {
	t.Parallel()
	document := `{"statements":[` + deployStatement +
		`,{"name":"other","jwt":{"issuer":"unknown","claims":{"sub":"1"}},"role":"read-only"}` +
		`,{"name":"Secret Value","aws":{"arn":"arn:aws:iam::123456789012:role/x"},"role":"read-only"}]}`
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", trusted(document))
	setup, reader := newTrustGaugeSetup(t, fake)

	setup.syncer.reconcile(context.Background())

	want := []string{
		"drover.cluster=c-1,project=p-alpha,reason=InvalidName 0",
		"drover.cluster=c-1,project=p-alpha,reason=UnknownIssuer,statement=other 0",
		"drover.cluster=c-1,project=p-alpha,statement=deploy 1",
	}
	if got := trustReadiness(t, reader, "p-alpha"); !slices.Equal(got, want) {
		t.Errorf("trust points =\n%v\nwant\n%v", got, want)
	}
}

func TestReconcileExportsOneTrustPointForADocumentError(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", trusted(`{"statements": [{"name": "secret-value"`))
	setup, reader := newTrustGaugeSetup(t, fake)

	setup.syncer.reconcile(context.Background())

	want := []string{"drover.cluster=c-1,project=p-alpha,reason=InvalidJSON 0"}
	if got := trustReadiness(t, reader, "p-alpha"); !slices.Equal(got, want) {
		t.Errorf("trust points =\n%v\nwant\n%v", got, want)
	}
}

func TestReconcileRemovesTheTrustPointsOfARemovedStatementAndAnnotation(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", trusted(twoStatements))
	setup, reader := newTrustGaugeSetup(t, fake)
	setup.syncer.reconcile(context.Background())
	if got := trustReadiness(t, reader, "p-alpha"); len(got) != 2 {
		t.Fatalf("trust points after the first run = %v, want 2", got)
	}

	fake.editProject("p-alpha", func(p *storedProject) { p.annotations[trustKey] = oneStatement })
	setup.syncer.reconcile(context.Background())

	want := []string{"drover.cluster=c-1,project=p-alpha,statement=deploy 1"}
	if got := trustReadiness(t, reader, "p-alpha"); !slices.Equal(got, want) {
		t.Errorf("trust points after the statement remove =\n%v\nwant\n%v", got, want)
	}

	fake.editProject("p-alpha", func(p *storedProject) { delete(p.annotations, trustKey) })
	setup.syncer.reconcile(context.Background())

	if got := trustReadiness(t, reader, "p-alpha"); len(got) != 0 {
		t.Errorf("trust points without the trust annotation = %v, want none", got)
	}
}

func TestReconcileRemovesTheTrustPointsOfAProjectThatIsGone(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", trusted(oneStatement))
	fake.addProject("p-beta", trusted(oneStatement))
	setup, reader := newTrustGaugeSetup(t, fake)
	setup.syncer.reconcile(context.Background())
	if got := trustReadiness(t, reader, "p-alpha"); len(got) != 1 {
		t.Fatalf("trust points of p-alpha after the first run = %v, want 1", got)
	}

	fake.removeProject("p-alpha")
	setup.syncer.reconcile(context.Background())

	if got := trustReadiness(t, reader, "p-alpha"); len(got) != 0 {
		t.Errorf("trust points of the removed project = %v, want none", got)
	}
	want := []string{"drover.cluster=c-1,project=p-beta,statement=deploy 1"}
	if got := trustReadiness(t, reader, "p-beta"); !slices.Equal(got, want) {
		t.Errorf("trust points of p-beta =\n%v\nwant\n%v", got, want)
	}
}

func TestProjectWatchRemovesTheTrustPointsOfADeletedProject(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", trusted(oneStatement))
	setup, reader := newTrustGaugeSetup(t, fake)
	setup.syncer.reconcile(context.Background())
	if got := trustReadiness(t, reader, "p-alpha"); len(got) != 1 {
		t.Fatalf("trust points after the run = %v, want 1", got)
	}

	item, err := setup.syncer.decodeProject(trustProjectFrame(t, map[string]string{trustKey: oneStatement}))
	if err != nil {
		t.Fatalf("decode the project: %v", err)
	}
	setup.syncer.applyProject(context.Background(), setup.syncer.newWatchSet(), watchDeleted, item)

	if got := trustReadiness(t, reader, "p-alpha"); len(got) != 0 {
		t.Errorf("trust points after the DELETED event = %v, want none", got)
	}
}

func TestReconcileRemovesTheTrustPointsOfAClusterThatIsGone(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", trusted(oneStatement))
	setup, reader := newTrustGaugeSetup(t, fake)
	setup.syncer.reconcile(context.Background())
	if got := trustReadiness(t, reader, "p-alpha"); len(got) != 1 {
		t.Fatalf("trust points after the first run = %v, want 1", got)
	}

	fake.mu.Lock()
	names := sortedKeys(fake.projects)
	fake.mu.Unlock()
	for _, name := range names {
		fake.removeProject(name)
	}
	setup.syncer.reconcile(context.Background())

	if got := trustReadiness(t, reader, "p-alpha"); len(got) != 0 {
		t.Errorf("trust points of a cluster without projects = %v, want none", got)
	}
}

func TestReconcileKeepsTheTrustPointsOfAClusterThatFailed(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", trusted(twoStatements))
	setup, reader := newTrustGaugeSetup(t, fake)
	setup.syncer.reconcile(context.Background())

	fake.fail(http.MethodGet, namespacesPath(acctCluster), http.StatusForbidden)
	setup.syncer.reconcile(context.Background())

	want := []string{
		"drover.cluster=c-1,project=p-alpha,statement=deploy 1",
		"drover.cluster=c-1,project=p-alpha,statement=rotator 1",
	}
	if got := trustReadiness(t, reader, "p-alpha"); !slices.Equal(got, want) {
		t.Errorf("trust points of the failed cluster =\n%v\nwant\n%v", got, want)
	}
}

func TestKeepTrustStoresNoTrustPointsOfAnOldProject(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-alpha", trusted(twoStatements))
	setup, reader := newTrustGaugeSetup(t, fake)
	setup.syncer.reconcile(context.Background())
	old := setup.syncer.projectsOf(acctCluster)["p-alpha"]
	run := &trustRun{rules: setup.syncer.trustRules(), token: serviceToken, owners: newProjectNames(setup.syncer.snapshot())}

	// The project watch stores a narrower document while the run has the old
	// project.
	fake.editProject("p-alpha", func(p *storedProject) { p.annotations[trustKey] = oneStatement })
	listTrustProject(t, setup, "p-alpha")
	setup.syncer.keepTrust(context.Background(), run, acctCluster, "p-alpha", old)

	want := []string{"drover.cluster=c-1,project=p-alpha,statement=deploy 1"}
	if got := trustReadiness(t, reader, "p-alpha"); !slices.Equal(got, want) {
		t.Errorf("trust points after the run with the old document =\n%v\nwant\n%v", got, want)
	}

	// The project watch removes the project while the run has it.
	item, err := setup.syncer.decodeProject(trustProjectFrame(t, map[string]string{trustKey: oneStatement}))
	if err != nil {
		t.Fatalf("decode the project: %v", err)
	}
	setup.syncer.applyProject(context.Background(), setup.syncer.newWatchSet(), watchDeleted, item)
	setup.syncer.keepTrust(context.Background(), run, acctCluster, "p-alpha", old)

	if got := trustReadiness(t, reader, "p-alpha"); len(got) != 0 {
		t.Errorf("trust points after the run with the deleted project = %v, want none", got)
	}
}
