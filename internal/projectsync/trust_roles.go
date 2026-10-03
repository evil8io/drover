package projectsync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	jwtAuthPrefix = "auth/jwt/"
	awsAuthPath   = "auth/aws"

	kindJWTRole = "jwt-role"
	kindAWSRole = "aws-role"
)

// loginKey is one login role: the path of its auth mount, and its name.
type loginKey struct {
	mount string
	name  string
}

func (k loginKey) path() string { return k.mount + "/role/" + k.name }

func (k loginKey) kind() string {
	if k.mount == awsAuthPath {
		return kindAWSRole
	}
	return kindJWTRole
}

// listPath returns the LIST path of the roles of mount. The LIST path of the
// AWS method is roles, and its read and write path is role/<name>.
func listPath(mount string) string {
	if mount == awsAuthPath {
		return mount + "/roles"
	}
	return mount + "/role"
}

// mounts returns the auth mounts of the rules: one per JWT issuer, sorted,
// and the AWS mount.
func (r *trustRules) mounts() []string {
	out := make([]string, 0, len(r.issuers)+1)
	for _, name := range slices.Sorted(maps.Keys(r.issuers)) {
		out = append(out, jwtAuthPrefix+name)
	}
	return append(out, awsAuthPath)
}

// loginKey returns the login role of the valid statement item of project.
func (r *trustRules) loginKey(project string, item trustStatement) loginKey {
	key := loginKey{mount: awsAuthPath, name: project + "-" + item.name}
	if item.jwt != nil {
		key.mount = jwtAuthPrefix + item.jwt.issuer
	}
	return key
}

// loginToken is the token part of a login role. The service sends every
// field from which OpenBao can give a token more rights, also when the field
// is empty. The reason is that OpenBao keeps a field that is not in the body
// of a write.
type loginToken struct {
	TokenPolicies        []string `json:"token_policies"`
	TokenType            string   `json:"token_type"`
	TokenTTL             string   `json:"token_ttl"`
	TokenMaxTTL          string   `json:"token_max_ttl"`
	TokenBoundCIDRs      []string `json:"token_bound_cidrs"`
	TokenExplicitMaxTTL  string   `json:"token_explicit_max_ttl"`
	TokenNumUses         int      `json:"token_num_uses"`
	TokenPeriod          string   `json:"token_period"`
	TokenNoDefaultPolicy bool     `json:"token_no_default_policy"`
}

type jwtLoginRole struct {
	RoleType        string            `json:"role_type"`
	BoundAudiences  []string          `json:"bound_audiences"`
	BoundClaims     map[string]any    `json:"bound_claims"`
	BoundClaimsType string            `json:"bound_claims_type"`
	BoundSubject    string            `json:"bound_subject"`
	UserClaim       string            `json:"user_claim"`
	ClaimMappings   map[string]string `json:"claim_mappings"`
	loginToken
}

type awsLoginRole struct {
	AuthType             string   `json:"auth_type"`
	BoundIAMPrincipalARN []string `json:"bound_iam_principal_arn"`
	ResolveAWSUniqueIDs  bool     `json:"resolve_aws_unique_ids"`
	InferredEntityType   string   `json:"inferred_entity_type"`
	loginToken
}

func (r *trustRules) token(policy string) loginToken {
	return loginToken{
		TokenPolicies:        []string{policy},
		TokenType:            "batch",
		TokenTTL:             r.loginTokenTTL,
		TokenMaxTTL:          r.loginTokenTTL,
		TokenBoundCIDRs:      []string{},
		TokenExplicitMaxTTL:  "0",
		TokenPeriod:          "0",
		TokenNoDefaultPolicy: true,
	}
}

// loginBody returns the role body of the valid statement item of project in
// cluster. OpenBao gives the token the ACL policy of the project role of
// item.
func (s *Syncer) loginBody(rules *trustRules, cluster, project string, item trustStatement) any {
	role, _ := accountRoleOf(item.role)
	token := rules.token(s.openbao.policyName(cluster, project, role))
	if item.aws != nil {
		return awsLoginRole{
			AuthType:             "iam",
			BoundIAMPrincipalARN: []string{withoutPath(item.aws.arn)},
			loginToken:           token,
		}
	}
	issuer := rules.issuers[item.jwt.issuer]
	claims := maps.Clone(item.jwt.claims)
	for claim, allowed := range issuer.allowed {
		if _, ok := claims[claim]; !ok {
			claims[claim] = slices.Clone(allowed)
		}
	}
	return jwtLoginRole{
		RoleType:        "jwt",
		BoundAudiences:  []string{rules.audience},
		BoundClaims:     claims,
		BoundClaimsType: "string",
		UserClaim:       issuer.required[0],
		ClaimMappings:   map[string]string{},
		loginToken:      token,
	}
}

// withoutPath returns the role ARN without the path of the role. The caller
// identity of an assumed role has no path, so the AWS method compares the ARN
// without it.
func withoutPath(arn string) string {
	head, rest, _ := strings.Cut(arn, ":role/")
	return head + ":role/" + rest[strings.LastIndex(rest, "/")+1:]
}

// loginWrite is the hash of the body of the last write of a login role, and
// the time of that write.
type loginWrite struct {
	hash string
	at   time.Time
}

// loginFresh reports whether the service wrote key with the body hash within
// openbaoVerifyAfter.
func (w *openbaoWriter) loginFresh(key loginKey, hash string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	last, ok := w.logins[key]
	return ok && last.hash == hash && w.now().Sub(last.at) < openbaoVerifyAfter
}

func (w *openbaoWriter) markLogin(key loginKey, hash string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.logins == nil {
		w.logins = make(map[loginKey]loginWrite)
	}
	w.logins[key] = loginWrite{hash: hash, at: w.now()}
}

// lastLogin returns the body hash of the last write of key, and false when
// the service did not write key.
func (w *openbaoWriter) lastLogin(key loginKey) (string, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	last, ok := w.logins[key]
	return last.hash, ok
}

func (w *openbaoWriter) forgetLogin(key loginKey) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.logins, key)
}

// projectRef is the cluster and the name of one project.
type projectRef struct {
	cluster string
	name    string
}

// projectNames maps a project name to the clusters that have a project with
// that name. A login role name starts with a project name, and the auth
// mounts are not per cluster. One role name can therefore fit several
// projects.
type projectNames map[string][]string

func newProjectNames(sets ...map[string]map[string]project) projectNames {
	out := make(projectNames)
	for _, clusters := range sets {
		for cluster, projects := range clusters {
			for name := range projects {
				if !slices.Contains(out[name], cluster) {
					out[name] = append(out[name], cluster)
				}
			}
		}
	}
	return out
}

// owners returns the projects whose name, followed by a dash, is a prefix of
// role.
func (n projectNames) owners(role string) []projectRef {
	var out []projectRef
	for i := range len(role) {
		if role[i] != '-' {
			continue
		}
		for _, cluster := range n[role[:i]] {
			out = append(out, projectRef{cluster: cluster, name: role[:i]})
		}
	}
	return out
}

// owns reports whether the project name of cluster is the only owner of
// role.
func (n projectNames) owns(role, cluster, name string) bool {
	owners := n.owners(role)
	return len(owners) == 1 && owners[0] == projectRef{cluster: cluster, name: name}
}

// trustRun is the context of the trust step of one reconcile run or one
// project event.
type trustRun struct {
	rules  *trustRules
	token  string
	owners projectNames
	// listed has the roles of each auth mount that the run listed, and
	// missing has the auth mounts that do not exist. Both are empty for an
	// event.
	listed  map[string]map[string]bool
	missing map[string]bool
}

// trustResult is the result of the trust step of one project. wanted are the
// login roles of the valid statements, and deleted are the roles that the
// service deleted in the step.
type trustResult struct {
	wanted  []loginKey
	deleted []loginKey
	errors  int
}

// keepTrust brings the login roles and the status of one project up to date.
// It writes the role of each valid statement. It deletes each role of the last
// status when the role has no valid statement. It then writes the status when
// the status differs.
func (s *Syncer) keepTrust(ctx context.Context, run *trustRun, cluster, name string, item project) trustResult {
	var result trustResult
	want := make(map[loginKey]bool)
	var status *trustStatus
	if item.trust.present() {
		status = &trustStatus{ObservedHash: item.trust.hash}
		doc := parseTrustValue(item.trust, run.rules)
		if doc.reason != "" {
			status.Error = newTrustReason(doc.reason)
		} else {
			entries := make([]statementStatus, 0, min(len(doc.statements), run.rules.maxStatements+1))
			for index, statement := range doc.statements {
				entry := s.keepLogin(ctx, run, cluster, name, statement, want, &result)
				// The status has one statement past the limit, so that its
				// size does not depend on the count of statements.
				if index <= run.rules.maxStatements {
					entries = append(entries, entry)
				}
			}
			status.Statements = &entries
		}
	}
	result.wanted = slices.Collect(maps.Keys(want))

	for _, old := range previousLogins(item.trustStatus) {
		key := loginKey{mount: old.Path, name: old.Role}
		if want[key] || !s.ownsLogin(run, cluster, name, key) {
			continue
		}
		if s.deleteLogin(ctx, cluster, key) {
			result.deleted = append(result.deleted, key)
		} else {
			result.errors++
		}
	}

	result.errors += s.writeTrustStatus(ctx, run, cluster, name, item.trustStatus, status)
	return result
}

// keepLogin writes the login role of statement when it is valid, and returns
// its status. It adds each role that stays in OpenBao to want.
func (s *Syncer) keepLogin(ctx context.Context, run *trustRun, cluster, project string, statement trustStatement, want map[loginKey]bool, result *trustResult) statementStatus {
	if statement.reason != "" {
		return notReady(statement.name, statement.reason)
	}
	key := run.rules.loginKey(project, statement)
	if !run.owners.owns(key.name, cluster, project) {
		s.logger.WarnContext(ctx, "the login role name is not unique",
			"cluster", cluster, "project", project, "statement", statement.name, "mount", key.mount)
		return notReady(statement.name, reasonWriteFailed)
	}
	if run.missing[key.mount] {
		return notReady(statement.name, reasonMethodDisabled)
	}
	ready := statementStatus{Name: statement.name, Ready: true, Login: &loginRef{Path: key.mount, Role: key.name}}

	data, err := json.Marshal(s.loginBody(run.rules, cluster, project, statement))
	if err != nil {
		result.errors++
		s.openbaoObjectFailure(ctx, cluster, key.kind(), "write", key.name, err)
		return notReady(statement.name, reasonWriteFailed)
	}
	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])
	w := s.openbao
	listed, wasListed := run.listed[key.mount]
	absent := wasListed && !listed[key.name]
	if !absent && w.loginFresh(key, hash) {
		want[key] = true
		return ready
	}
	previous, known := w.lastLogin(key)

	err = takeToken(ctx)
	if err == nil {
		err = w.call(ctx, http.MethodPost, key.path(), nil, json.RawMessage(data), nil)
	}
	switch {
	case missingMount(err):
		s.logger.InfoContext(ctx, "the OpenBao auth mount does not exist",
			"cluster", cluster, "project", project, "statement", statement.name, "mount", key.mount)
		return notReady(statement.name, reasonMethodDisabled)
	case err != nil:
		// After a failed write, the role of an earlier write is still in
		// OpenBao, so the service keeps that role.
		want[key] = true
		result.errors++
		s.openbaoObjectFailure(ctx, cluster, key.kind(), "write", key.name, err)
		return notReady(statement.name, reasonWriteFailed)
	}
	want[key] = true
	w.markLogin(key, hash)
	if known && previous == hash && !absent {
		s.logger.DebugContext(ctx, "the login role is refreshed",
			"cluster", cluster, "project", project, "statement", statement.name, "mount", key.mount)
		return ready
	}
	action := "write"
	if absent {
		action = "create"
	}
	s.openbaoChanged(ctx, cluster, key.kind(), action, key.name)
	return ready
}

// ownsLogin reports whether the project name of cluster can own the login
// role key. These conditions must be true:
//   - The mount of key is a mount of the rules file.
//   - The name of key is the project name, a dash, and a valid statement name.
//   - No other known project fits the name of key.
//
// The status is tenant input, so the service keeps a role when one of these
// conditions is false.
func (s *Syncer) ownsLogin(run *trustRun, cluster, name string, key loginKey) bool {
	if !slices.Contains(run.rules.mounts(), key.mount) {
		return false
	}
	statement, ok := strings.CutPrefix(key.name, name+"-")
	return ok && statementName.MatchString(statement) && run.owners.owns(key.name, cluster, name)
}

// deleteLogin deletes the login role key. A role that is gone is not an
// error. It returns false on an error.
func (s *Syncer) deleteLogin(ctx context.Context, cluster string, key loginKey) bool {
	err := takeToken(ctx)
	if err == nil {
		err = s.openbao.call(ctx, http.MethodDelete, key.path(), nil, nil, nil)
	}
	if err != nil && !missingMount(err) {
		s.openbaoObjectFailure(ctx, cluster, key.kind(), "delete", key.name, err)
		return false
	}
	s.openbao.forgetLogin(key)
	if err == nil {
		s.openbaoChanged(ctx, cluster, key.kind(), "delete", key.name)
	}
	return true
}

// writeTrustStatus writes status into the status annotation of the project
// when status differs from current. It ignores observedAt in that check. For
// a nil status, it removes the annotation. It returns the count of errors.
func (s *Syncer) writeTrustStatus(ctx context.Context, run *trustRun, cluster, name, current string, status *trustStatus) int {
	var value *string
	if status == nil {
		if current == "" {
			return 0
		}
	} else {
		if sameTrustStatus(current, *status) {
			return 0
		}
		status.ObservedAt = s.openbao.now().UTC().Format(time.RFC3339)
		data, err := json.Marshal(status)
		if err != nil {
			s.logFailure(ctx, slog.LevelError, "the trust status encode failed", err, "cluster", cluster, "project", name)
			return 1
		}
		text := string(data)
		value = &text
	}

	path := clusterPath(rancherCluster) + managementProjects + cluster + "/projects/" + name
	body := map[string]any{"metadata": map[string]any{"annotations": map[string]*string{run.rules.statusAnnotation: value}}}
	err := s.send(ctx, run.token, http.MethodPatch, path, body, nil)
	if hasStatus(err, http.StatusNotFound, http.StatusConflict) {
		s.logger.DebugContext(ctx, "the trust status patch is skipped",
			"cluster", cluster, "project", name, "error", err.Error())
		return 0
	}
	if err != nil {
		s.logFailure(ctx, slog.LevelWarn, "the trust status patch failed", err, "cluster", cluster, "project", name)
		return 1
	}
	if value == nil {
		s.logger.InfoContext(ctx, "the trust status is removed", "cluster", cluster, "project", name)
		return 0
	}
	s.logTrustStatus(ctx, cluster, name, status)
	return 0
}

// logTrustStatus logs a written status: one line per statement that is not
// ready, and one summary line. It logs a statement name only when the name is
// valid. It never logs the trust document.
func (s *Syncer) logTrustStatus(ctx context.Context, cluster, name string, status *trustStatus) {
	if status.Error != nil {
		s.logger.InfoContext(ctx, "the trust document is not valid",
			"cluster", cluster, "project", name, "reason", status.Error.Reason)
		return
	}
	ready := 0
	for index, item := range *status.Statements {
		if item.Ready {
			ready++
			continue
		}
		attrs := []any{"cluster", cluster, "project", name, "index", index, "reason", item.Reason}
		if statementName.MatchString(item.Name) {
			attrs = append(attrs, "statement", item.Name)
		}
		s.logger.InfoContext(ctx, "the trust statement is not ready", attrs...)
	}
	s.logger.InfoContext(ctx, "the trust status is written",
		"cluster", cluster, "project", name, "statements", len(*status.Statements), "ready", ready)
}

// keepProjectTrust runs the trust step of the project name of cluster after
// a project event.
func (s *Syncer) keepProjectTrust(ctx context.Context, token, cluster, name string) {
	rules := s.trustRules()
	if rules == nil {
		return
	}
	item, ok := s.projectsOf(cluster)[name]
	if !ok {
		return
	}
	run := &trustRun{rules: rules, token: token, owners: newProjectNames(s.snapshot())}
	s.keepTrust(ctx, run, cluster, name, item)
}

// refreshTrust runs the trust step of every tenant of ready, and then deletes
// the stale login roles. names are the clusters of the run, and projects are
// the projects of the run by cluster. It returns the count of errors.
func (s *Syncer) refreshTrust(ctx context.Context, token string, names []string, ready map[string]openbaoTarget, projects map[string]map[string]project, rules *trustRules) int {
	w := s.openbao
	run := &trustRun{
		rules:   rules,
		token:   token,
		owners:  newProjectNames(projects, s.snapshot()),
		listed:  make(map[string]map[string]bool),
		missing: make(map[string]bool),
	}
	errs := 0
	for _, mount := range rules.mounts() {
		roles, err := w.list(ctx, listPath(mount))
		switch {
		case missingMount(err):
			run.missing[mount] = true
			s.logger.DebugContext(ctx, "the OpenBao auth mount does not exist", "mount", mount)
		case err != nil:
			errs++
			s.logFailure(ctx, slog.LevelError, "the OpenBao login role list request failed", err, "mount", mount)
		default:
			set := make(map[string]bool, len(roles))
			for _, role := range roles {
				set[role] = true
			}
			run.listed[mount] = set
		}
	}

	var mu sync.Mutex
	processed := make(map[projectRef]trustValue)
	wanted := make(map[loginKey]bool)
	deleted := make(map[loginKey]bool)
	eachCluster(slices.Sorted(maps.Keys(ready)), func(cluster string) {
		live := s.projectsOf(cluster)
		for _, name := range ready[cluster].tenants {
			item, ok := projects[cluster][name]
			if !ok {
				if item, ok = live[name]; !ok {
					continue
				}
			}
			result := s.keepTrust(ctx, run, cluster, name, item)
			mu.Lock()
			processed[projectRef{cluster: cluster, name: name}] = item.trust
			for _, key := range result.wanted {
				wanted[key] = true
			}
			for _, key := range result.deleted {
				deleted[key] = true
			}
			errs += result.errors
			mu.Unlock()
		}
	})
	return errs + s.dropStaleLogins(ctx, run, names, projects, wanted, deleted, processed)
}

// dropStaleLogins deletes each listed login role that has no valid statement
// in the run. A role whose name starts with the name of one known project
// belongs to that project. For another role, the name of the first token
// policy contains the project.
//
// The service deletes a role of a project that the run checked. When neither
// the run nor the project watch has the project, the service deletes the role
// only after Rancher answers 404 for that project. The service keeps a role
// whose policy name does not have the form of a policy of the service.
// dropStaleLogins returns the count of errors.
func (s *Syncer) dropStaleLogins(ctx context.Context, run *trustRun, names []string, projects map[string]map[string]project, wanted, deleted map[loginKey]bool, processed map[projectRef]trustValue) int {
	errs := 0
	gone := make(map[projectRef]bool)
	for _, mount := range slices.Sorted(maps.Keys(run.listed)) {
		for _, role := range slices.Sorted(maps.Keys(run.listed[mount])) {
			key := loginKey{mount: mount, name: role}
			if wanted[key] || deleted[key] {
				continue
			}
			owners := run.owners.owners(role)
			if len(owners) == 1 {
				if s.checkedAsIs(owners[0], processed) && !s.deleteLogin(ctx, owners[0].cluster, key) {
					errs++
				}
				continue
			}

			owner, ok, err := s.loginOwner(ctx, key, names)
			if err != nil {
				errs++
				continue
			}
			if !ok {
				continue
			}
			_, known := projects[owner.cluster][owner.name]
			_, live := s.projectsOf(owner.cluster)[owner.name]
			switch {
			case slices.Contains(owners, owner):
				// The service writes no role whose name fits several projects.
				if s.checkedAsIs(owner, processed) && !s.deleteLogin(ctx, owner.cluster, key) {
					errs++
				}
			case !known && !live:
				isGone, checked := gone[owner]
				if !checked {
					if isGone, err = s.projectGone(ctx, run.token, owner.cluster, owner.name); err != nil {
						errs++
						s.logFailure(ctx, slog.LevelError, "the project request failed", err,
							"cluster", owner.cluster, "project", owner.name)
						continue
					}
					gone[owner] = isGone
				}
				if isGone && !s.deleteLogin(ctx, owner.cluster, key) {
					errs++
				}
			}
		}
	}
	return errs
}

// checkedAsIs reports whether the run checked the project ref, and whether
// the project still has the trust value of that check. After the project
// list of the run, the project watch can get a project event with a new
// statement.
func (s *Syncer) checkedAsIs(ref projectRef, processed map[projectRef]trustValue) bool {
	used, ok := processed[ref]
	if !ok {
		return false
	}
	live, ok := s.projectsOf(ref.cluster)[ref.name]
	return ok && live.trust == used
}

// loginOwner reads the login role key, and returns the project of its first
// token policy. It returns false for a role that is gone. It also returns
// false when the policy name does not have the form of a policy of the
// service.
func (s *Syncer) loginOwner(ctx context.Context, key loginKey, names []string) (projectRef, bool, error) {
	var answer struct {
		Data struct {
			TokenPolicies []string `json:"token_policies"`
		} `json:"data"`
	}
	err := s.openbao.call(ctx, http.MethodGet, key.path(), nil, nil, &answer)
	if hasOpenBaoStatus(err, http.StatusNotFound) {
		return projectRef{}, false, nil
	}
	if err != nil {
		s.logFailure(ctx, slog.LevelError, "the OpenBao login role read failed", err, "mount", key.mount, "name", key.name)
		return projectRef{}, false, err
	}
	if policies := answer.Data.TokenPolicies; len(policies) > 0 {
		if cluster, project, _, ok := s.openbao.parsePolicyName(policies[0], names); ok {
			return projectRef{cluster: cluster, name: project}, true, nil
		}
	}
	s.logger.DebugContext(ctx, "the login role has no policy of the service, so the service keeps it",
		"mount", key.mount, "name", key.name)
	return projectRef{}, false, nil
}
