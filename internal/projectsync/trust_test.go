package projectsync

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// trustCase is one case of the fixture set. document is the annotation value.
type trustCase struct {
	Valid    bool   `json:"valid"`
	Reason   string `json:"reason"`
	Document string `json:"document"`
}

func fixtureRules(t *testing.T) *trustRules {
	t.Helper()
	rules, err := loadTrustRules(trustRulesPath, nil)
	if err != nil {
		t.Fatalf("load the fixture rules: %v", err)
	}
	return rules
}

func TestParseTrustFixtures(t *testing.T) {
	t.Parallel()
	rules := fixtureRules(t)
	paths, err := filepath.Glob("testdata/trust/*.json")
	if err != nil {
		t.Fatalf("list the fixtures: %v", err)
	}

	valid := 0
	reasons := make(map[string]bool)
	for _, path := range paths {
		if filepath.Base(path) == "rules.json" {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		var tc trustCase
		if err := json.Unmarshal(data, &tc); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
		if tc.Valid {
			valid++
		}
		reasons[tc.Reason] = true

		t.Run(strings.TrimSuffix(filepath.Base(path), ".json"), func(t *testing.T) {
			t.Parallel()
			doc := parseTrust(tc.Document, rules)
			if got := doc.valid(); got != tc.Valid {
				t.Errorf("valid = %v, want %v", got, tc.Valid)
			}
			if got := doc.firstReason(); got != tc.Reason {
				t.Errorf("reason = %q, want %q", got, tc.Reason)
			}
		})
	}

	if valid < 4 {
		t.Errorf("valid cases = %d, want at least 4", valid)
	}
	// No fixture has the reason MethodDisabled, because the AWS method is on
	// in the fixture rules. No fixture has the reason WriteFailed, because the
	// parser never sets it.
	for reason := range trustMessages {
		if reason != reasonWriteFailed && reason != reasonMethodDisabled && !reasons[reason] {
			t.Errorf("the fixture set has no case for the reason %s", reason)
		}
	}
}

func TestParseTrustGivesMethodDisabledAfterTheAccountCheck(t *testing.T) {
	t.Parallel()
	rules := fixtureRules(t)
	off := *rules
	off.awsEnabled = false

	tests := []struct {
		name, arn, want string
	}{
		{"allowed account", "arn:aws:iam::123456789012:role/rotator", reasonMethodDisabled},
		{"other account", "arn:aws:iam::210987654321:role/rotator", reasonAccountNotAllowed},
		{"bad ARN", "arn:aws:iam::123456789012:role/*", reasonInvalidARN},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			document := `{"statements":[{"name":"rotator","aws":{"arn":"` + tt.arn + `"},"role":"Owner"}]}`
			if got := parseTrust(document, &off).firstReason(); got != tt.want {
				t.Errorf("reason = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseTrustRejectsARoleARNWithoutAName(t *testing.T) {
	t.Parallel()
	document := `{"statements":[{"name":"rotator","aws":{"arn":"arn:aws:iam::123456789012:role/team/"},"role":"read-only"}]}`
	if got := parseTrust(document, fixtureRules(t)).firstReason(); got != reasonInvalidARN {
		t.Errorf("reason = %q, want %q", got, reasonInvalidARN)
	}
}

const validTrustRules = `{"annotation":"drover-trust","statusAnnotation":"drover-trust-status","maxStatements":20,` +
	`"audience":"https://openbao.example.com","loginTokenTTL":"5m",` +
	`"jwtIssuers":{"github":{"requiredClaims":["repository_id"],"allowedClaims":{"ref":["refs/heads/main"]}}},` +
	`"aws":{"enabled":true,"allowedAccounts":["123456789012"]},"chartOnly":{"key":"ignored"}}`

func TestParseTrustRules(t *testing.T) {
	t.Parallel()

	rules, err := parseTrustRules([]byte(validTrustRules))
	if err != nil {
		t.Fatalf("parse the valid rules: %v", err)
	}
	if rules.annotation != "drover-trust" || rules.statusAnnotation != "drover-trust-status" || rules.maxStatements != 20 ||
		rules.loginTokenTTL != "5m" || !rules.awsEnabled || len(rules.issuers) != 1 {
		t.Errorf("rules = %+v", rules)
	}

	tests := []struct {
		name string
		edit func(map[string]any)
		want string
	}{
		{"empty annotation", func(m map[string]any) { m["annotation"] = "" }, "a metadata key is empty"},
		{"annotation of Kubernetes", func(m map[string]any) { m["annotation"] = "kubernetes.io/trust" }, "kubernetes.io"},
		{"same keys", func(m map[string]any) { m["statusAnnotation"] = "drover-trust" }, "are the same key"},
		{"no limit", func(m map[string]any) { delete(m, "maxStatements") }, "maxStatements 0 is not 1 to 100"},
		{"limit above 100", func(m map[string]any) { m["maxStatements"] = 101 }, "maxStatements 101"},
		{"no audience", func(m map[string]any) { m["audience"] = "" }, "audience is empty"},
		{"short lifetime", func(m map[string]any) { m["loginTokenTTL"] = "30s" }, "loginTokenTTL 30s is not 1m0s to 24h0m0s"},
		{"long lifetime", func(m map[string]any) { m["loginTokenTTL"] = "25h" }, "loginTokenTTL 25h0m0s"},
		{"lifetime without a unit", func(m map[string]any) { m["loginTokenTTL"] = "300" }, "loginTokenTTL"},
		{"issuer name", func(m map[string]any) {
			m["jwtIssuers"] = map[string]any{"Git/Hub": map[string]any{"requiredClaims": []any{"sub"}}}
		}, "is not a lowercase DNS label"},
		{"no required claim", func(m map[string]any) {
			m["jwtIssuers"] = map[string]any{"github": map[string]any{}}
		}, "has no required claim"},
		{"required aud", func(m map[string]any) {
			m["jwtIssuers"] = map[string]any{"github": map[string]any{"requiredClaims": []any{"aud"}}}
		}, "which a statement cannot bind"},
		{"empty allowed list", func(m map[string]any) {
			m["jwtIssuers"] = map[string]any{"github": map[string]any{"requiredClaims": []any{"sub"}, "allowedClaims": map[string]any{"ref": []any{}}}}
		}, "allowed claim \"ref\""},
		{"bad account", func(m map[string]any) {
			m["aws"] = map[string]any{"enabled": true, "allowedAccounts": []any{"1234"}}
		}, "12-digit"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var file map[string]any
			if err := json.Unmarshal([]byte(validTrustRules), &file); err != nil {
				t.Fatalf("decode the valid rules: %v", err)
			}
			tt.edit(file)
			data, err := json.Marshal(file)
			if err != nil {
				t.Fatalf("encode the rules: %v", err)
			}
			_, err = parseTrustRules(data)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %v, want one with %q", err, tt.want)
			}
		})
	}
}

func TestNewChecksTheTrustRules(t *testing.T) {
	t.Parallel()
	target, _ := url.Parse("https://rancher.example.com")
	bao, _ := url.Parse("http://openbao:8200")
	openbao := &OpenBaoConfig{
		Address: bao, AuthPath: "kubernetes", Role: "project-sync", JWTFile: "/jwt", MountPrefix: "kubernetes",
		RancherURL: target, TokenTTL: time.Hour, CredentialTTL: 15 * time.Minute, CredentialMaxTTL: time.Hour,
	}
	missing := filepath.Join(t.TempDir(), "missing.json")

	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{"without OpenBao", Config{ServiceAccounts: true, TrustFile: trustRulesPath}, "the trust rules need the OpenBao config"},
		{"missing file", Config{ServiceAccounts: true, OpenBao: openbao, TrustFile: missing}, "read the trust rules file"},
		{"trust key copied", Config{ServiceAccounts: true, OpenBao: openbao, TrustFile: trustRulesPath, Annotations: []string{trustKey}}, "copies to a namespace"},
		{"status key copied", Config{ServiceAccounts: true, OpenBao: openbao, TrustFile: trustRulesPath, NameAnnotation: trustStatusKey}, "copies to a namespace"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := tt.cfg
			cfg.RancherURL, cfg.TokenFile = target, "/token"
			_, err := New(cfg)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("New = %v, want an error with %q", err, tt.want)
			}
		})
	}

	cfg := Config{RancherURL: target, TokenFile: "/token", ServiceAccounts: true, OpenBao: openbao, TrustFile: trustRulesPath}
	syncer, err := New(cfg)
	if err != nil {
		t.Fatalf("New with the fixture rules: %v", err)
	}
	if rules := syncer.trustRules(); rules == nil || rules.maxStatements != 3 {
		t.Errorf("rules = %+v, want the fixture rules", rules)
	}
}

func TestReloadTrustKeepsTheLastValidRules(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "trust.json")
	if err := os.WriteFile(path, []byte(validTrustRules), 0o600); err != nil {
		t.Fatalf("write the rules: %v", err)
	}
	setup := newOpenBaoSetup(t, newAccountsFake(t), withTrust(path))
	first := setup.syncer.trustRules()

	if err := os.WriteFile(path, []byte(`{"annotation":`), 0o600); err != nil {
		t.Fatalf("write the rules: %v", err)
	}
	var run counters
	setup.syncer.reloadTrust(context.Background(), &run)
	if run.errors != 1 || setup.syncer.trustRules() != first {
		t.Errorf("after a bad file: errors = %d, rules changed = %v; want 1 error and the first rules", run.errors, setup.syncer.trustRules() != first)
	}
	if !strings.Contains(setup.logs.String(), "the trust rules are not valid") {
		t.Errorf("no log line for the bad file:\n%s", setup.logs.String())
	}

	if err := os.WriteFile(path, []byte(strings.Replace(validTrustRules, `"maxStatements":20`, `"maxStatements":5`, 1)), 0o600); err != nil {
		t.Fatalf("write the rules: %v", err)
	}
	run = counters{}
	setup.syncer.reloadTrust(context.Background(), &run)
	if run.errors != 0 || setup.syncer.trustRules().maxStatements != 5 {
		t.Errorf("after a valid file: errors = %d, maxStatements = %d; want 0 and 5", run.errors, setup.syncer.trustRules().maxStatements)
	}
}

func TestTrustStatusIsDeterministic(t *testing.T) {
	t.Parallel()
	entries := []statementStatus{
		{Name: "deploy", Ready: true, Login: &loginRef{Path: githubMount, Role: "p-abc12_deploy"}},
		notReady("rotator", reasonAccountNotAllowed),
	}
	status := trustStatus{ObservedHash: "sha256:00", ObservedAt: "2026-10-03T12:00:00Z", Statements: &entries}
	data, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	want := `{"observedHash":"sha256:00","observedAt":"2026-10-03T12:00:00Z","statements":[` +
		`{"name":"deploy","ready":true,"login":{"path":"auth/jwt/github","role":"p-abc12_deploy"}},` +
		`{"name":"rotator","ready":false,"reason":"AccountNotAllowed","message":"the account is not in the allowed accounts"}]}`
	if string(data) != want {
		t.Errorf("status =\n%s\nwant\n%s", data, want)
	}

	failed := trustStatus{ObservedHash: "sha256:00", ObservedAt: "2026-10-03T12:00:00Z", Error: newTrustReason(reasonInvalidJSON)}
	data, err = json.Marshal(failed)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	want = `{"observedHash":"sha256:00","observedAt":"2026-10-03T12:00:00Z","error":{"reason":"InvalidJSON","message":"the annotation value is not valid JSON"}}`
	if string(data) != want {
		t.Errorf("status =\n%s\nwant\n%s", data, want)
	}

	later := status
	later.ObservedAt = "2026-10-04T00:00:00Z"
	if !sameTrustStatus(string(must(json.Marshal(status))), later) {
		t.Error("two statuses that differ in observedAt only differ, want equal")
	}
	if sameTrustStatus(strings.Replace(string(must(json.Marshal(status))), `{"observedHash"`, `{"extra":1,"observedHash"`, 1), status) {
		t.Error("a status with an extra key is equal, want a difference")
	}
	if sameTrustStatus(oversizeStatus, status) || sameTrustStatus("", status) {
		t.Error("an oversize or an absent status is equal, want a difference")
	}
}

func must(data []byte, err error) []byte {
	if err != nil {
		panic(err)
	}
	return data
}

func TestStatusNameCutsALongInvalidName(t *testing.T) {
	t.Parallel()
	name := strings.Repeat("a", 62) + "é"
	got := statusName(name)
	if got != strings.Repeat("a", 62) {
		t.Errorf("statusName = %q, want 62 characters without the cut rune", got)
	}
	if statusName("deploy") != "deploy" {
		t.Error("statusName changed a short name")
	}
}

func TestWithoutPath(t *testing.T) {
	t.Parallel()
	for arn, want := range map[string]string{
		"arn:aws:iam::123456789012:role/a/b/rotator": "arn:aws:iam::123456789012:role/rotator",
		"arn:aws:iam::123456789012:role/rotator":     "arn:aws:iam::123456789012:role/rotator",
	} {
		if got := withoutPath(arn); got != want {
			t.Errorf("withoutPath(%s) = %s, want %s", arn, got, want)
		}
	}
}

func TestProjectNamesSplitALoginRoleNameAtTheUnderscore(t *testing.T) {
	t.Parallel()
	names := newProjectNames(map[string]map[string]project{
		"c-1": {"p": {}, "p-alpha": {}, "p-abc12": {}},
		"c-2": {"p-xyz34": {}, "p-abc12": {}},
	}, map[string]map[string]project{
		"c-2": {"p-xyz34": {}},
	})

	if got, _ := names.owners("p-xyz34_deploy"); len(got) != 1 || !names.owns("p-xyz34_deploy", "c-2", "p-xyz34") {
		t.Errorf("owners of p-xyz34_deploy = %v, want only p-xyz34 of c-2, also with the project in two sets", got)
	}
	if !names.owns("p-alpha_deploy", "c-1", "p-alpha") || names.owns("p-alpha_deploy", "c-1", "p") {
		t.Error("the owner of p-alpha_deploy is not p-alpha alone, want p-alpha and not p")
	}
	if got, _ := names.owners("p_alpha-deploy"); len(got) != 1 || got[0] != (projectRef{cluster: "c-1", name: "p"}) {
		t.Errorf("owners of p_alpha-deploy = %v, want only p of c-1", got)
	}
	if got, _ := names.owners("p-abc12_ci"); len(got) != 2 || names.owns("p-abc12_ci", "c-1", "p-abc12") {
		t.Errorf("owners of p-abc12_ci = %v, want p-abc12 of c-1 and of c-2, and no single owner", got)
	}
	if got, ok := names.owners("p-gone_ci"); !ok || len(got) != 0 {
		t.Errorf("owners of p-gone_ci = %v, %v; want a name of the service without an owner", got, ok)
	}
	for _, role := range []string{"p-alpha-deploy", "admin-deploy", "p-alpha_de_ploy", "p-alpha__deploy", "_deploy", "p-alpha_", "p-alpha_Deploy", "p-alpha_deploy/../x"} {
		if got, ok := names.owners(role); ok || got != nil {
			t.Errorf("owners of %q = %v, %v; want no name of the service", role, got, ok)
		}
	}
}

func TestPreviousLoginsReadsEachRoleOnceUpToTheLimit(t *testing.T) {
	t.Parallel()
	entries := make([]string, 0, 3000)
	for i := range 3000 {
		entries = append(entries, fmt.Sprintf(`{"name":"s","ready":true,"login":{"path":"auth/aws","role":"p-alpha_s%d"}}`, i/2))
	}
	status := `{"statements":[{"name":"x","ready":false},` + strings.Join(entries, ",") + `]}`

	got := previousLogins(status, 4)
	want := []loginRef{{Path: awsAuthPath, Role: "p-alpha_s0"}, {Path: awsAuthPath, Role: "p-alpha_s1"}}
	if !slices.Equal(got, want) {
		t.Errorf("previousLogins = %v, want %v", got, want)
	}
	if got := previousLogins("not JSON", 4); got != nil {
		t.Errorf("previousLogins of a value that is not JSON = %v, want nil", got)
	}
}
