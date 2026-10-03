package projectsync

import (
	"context"
	"encoding/json"
	"net/http"
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
		// The statement does not bind ref, so the role gets every allowed
		// value of it.
		"bound_claims":      map[string]any{"repository_id": "123456789", "ref": []any{"refs/heads/main", "refs/heads/release"}},
		"bound_claims_type": "string",
		"bound_subject":     "",
		"user_claim":        "repository_id",
		"claim_mappings":    map[string]any{},
	})
	if got, _ := setup.bao.login(githubMount, "p-alpha-deploy"); !reflect.DeepEqual(got, jwtWant) {
		t.Errorf("JWT role =\n%v\nwant\n%v", got, jwtWant)
	}
	awsWant := withFields(loginTokenWant("p-alpha", "read-only"), map[string]any{
		"auth_type":               "iam",
		"bound_iam_principal_arn": []any{"arn:aws:iam::123456789012:role/rotator"},
		"resolve_aws_unique_ids":  false,
		"inferred_entity_type":    "",
	})
	if got, _ := setup.bao.login(awsAuthPath, "p-alpha-rotator"); !reflect.DeepEqual(got, awsWant) {
		t.Errorf("AWS role =\n%v\nwant\n%v", got, awsWant)
	}
	if got := setup.bao.loginNames(githubMount); !slices.Equal(got, []string{"p-alpha-deploy"}) {
		t.Errorf("JWT roles = %v, want only p-alpha-deploy", got)
	}

	policyAt, roleAt := -1, -1
	for i, req := range setup.bao.all() {
		if req.method == http.MethodPut && req.path == "/v1/sys/policies/acl/kubernetes-c-1-p-alpha-project-member" {
			policyAt = i
		}
		if req.method == http.MethodPost && req.path == "/v1/"+githubMount+"/role/p-alpha-deploy" {
			roleAt = i
		}
	}
	if policyAt == -1 || roleAt < policyAt {
		t.Errorf("policy write at request %d, login role write at %d; want the policy first", policyAt, roleAt)
	}

	status, _ := fake.projectAnnotation("p-alpha", trustStatusKey)
	want := `{"observedHash":"` + trustHash(document) + `","observedAt":"2026-01-01T00:00:00Z","statements":[` +
		`{"name":"deploy","ready":true,"login":{"path":"auth/jwt/github","role":"p-alpha-deploy"}},` +
		`{"name":"rotator","ready":true,"login":{"path":"auth/aws","role":"p-alpha-rotator"}},` +
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
		`{"name":"deploy","ready":true,"login":{"path":"auth/jwt/github","role":"p-alpha-deploy"}}]}`
	if third != want {
		t.Errorf("status =\n%s\nwant\n%s", third, want)
	}
	if _, ok := setup.bao.login(awsAuthPath, "p-alpha-rotator"); ok {
		t.Error("the role of the removed statement exists, want it deleted")
	}
	if got := countRequests(setup.bao.all(), http.MethodDelete, "/v1/auth/aws/role/p-alpha-rotator"); got != 1 {
		t.Errorf("deletes of the role of the removed statement = %d, want 1", got)
	}
	if got := countRequests(setup.bao.all(), http.MethodPost, "/v1/"+githubMount+"/role/p-alpha-deploy"); got != 0 {
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
	rolePath := "/v1/" + githubMount + "/role/p-alpha-deploy"

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
		if strings.Contains(line, "level=INFO") && strings.Contains(line, "p-alpha-deploy") {
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

	if !strings.Contains(setup.logs.String()[offset:], `level=INFO msg="OpenBao object changed" cluster=c-1 kind=jwt-role action=write name=p-alpha-deploy`) {
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

	if _, ok := setup.bao.login(awsAuthPath, "p-alpha-rotator"); ok {
		t.Error("the role of the removed statement exists, want it deleted")
	}
	if _, ok := setup.bao.login(githubMount, "p-alpha-deploy"); !ok {
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
	setup.bao.setLogin(githubMount, "p-alpha-old", policyOf("p-alpha", "read-only"))
	// Rancher answers 404 for the project of this role.
	setup.bao.setLogin(githubMount, "p-gone-ci", policyOf("p-gone", "project-owner"))
	// The project of this role exists, but the project list does not have it.
	setup.bao.setLogin(githubMount, "p-new-ci", policyOf("p-new", "read-only"))
	// The project of this role is known, but the run does not check it.
	setup.bao.setLogin(awsAuthPath, "p-acct-ci", policyOf("p-acct", "read-only"))
	// Another writer owns this role.
	setup.bao.setLogin(awsAuthPath, "admin-deploy", map[string]any{"token_policies": []any{"admin"}})

	setup.syncer.reconcile(context.Background())

	if got := setup.bao.loginNames(githubMount); !slices.Equal(got, []string{"p-alpha-deploy", "p-new-ci"}) {
		t.Errorf("JWT roles = %v, want p-alpha-deploy and p-new-ci", got)
	}
	if got := setup.bao.loginNames(awsAuthPath); !slices.Equal(got, []string{"admin-deploy", "p-acct-ci"}) {
		t.Errorf("AWS roles = %v, want admin-deploy and p-acct-ci", got)
	}
	for _, project := range []string{"p-gone", "p-new"} {
		if got := fake.requestsOfPath(projectsPath + "/" + acctCluster + ":" + project); len(got) != 1 {
			t.Errorf("project requests for %s = %d, want 1", project, len(got))
		}
	}
	for _, name := range []string{"p-alpha-old", "p-acct-ci"} {
		if got := countRequests(setup.bao.all(), http.MethodGet, "/v1/"+githubMount+"/role/"+name) +
			countRequests(setup.bao.all(), http.MethodGet, "/v1/auth/aws/role/"+name); got != 0 {
			t.Errorf("reads of %s = %d, want 0, because the name names a known project", name, got)
		}
	}
	logs := setup.logs.String()
	if !strings.Contains(logs, `msg="the login role has no policy of the service, so the service keeps it" mount=auth/aws name=admin-deploy`) {
		t.Errorf("no debug line for the role of another writer:\n%s", logs)
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
		`{"name":"rotator","ready":true,"login":{"path":"auth/aws","role":"p-alpha-rotator"}}]}`
	if status != want {
		t.Errorf("status =\n%s\nwant\n%s", status, want)
	}
	if got := countRequests(setup.bao.all(), http.MethodPost, "/v1/"+githubMount+"/role/p-alpha-deploy"); got != 0 {
		t.Errorf("writes to the missing mount = %d, want 0, because the list found no mount", got)
	}
	if !strings.Contains(setup.logs.String(), "errors=0") {
		t.Errorf("the run has errors:\n%s", setup.logs.String())
	}

	fake.resetRequests()
	setup.bao.reset()
	listTrustProject(t, setup, "p-alpha")

	if got := countRequests(setup.bao.all(), http.MethodPost, "/v1/"+githubMount+"/role/p-alpha-deploy"); got != 1 {
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

func TestReconcileWritesNoLoginRoleWhoseNameFitsTwoProjects(t *testing.T) {
	t.Parallel()
	fake := newAccountsFake(t)
	fake.addProject("p-a", trusted(`{"statements":[{"name":"x-ci","jwt":{"issuer":"github","claims":{"repository_id":"1"}},"role":"read-only"}]}`))
	fake.addProject("p-a-x", trusted(`{"statements":[{"name":"ci","jwt":{"issuer":"github","claims":{"repository_id":"2"}},"role":"read-only"}]}`))
	setup := newTrustSetup(t, fake)

	setup.syncer.reconcile(context.Background())

	if got := setup.bao.loginNames(githubMount); len(got) != 0 {
		t.Errorf("JWT roles = %v, want none", got)
	}
	for _, project := range []string{"p-a", "p-a-x"} {
		status, _ := fake.projectAnnotation(project, trustStatusKey)
		if !strings.Contains(status, `"reason":"WriteFailed"`) {
			t.Errorf("status of %s = %s, want WriteFailed", project, status)
		}
	}
	if !strings.Contains(setup.logs.String(), "the login role name is not unique") {
		t.Errorf("no line for the name of two projects:\n%s", setup.logs.String())
	}
}
