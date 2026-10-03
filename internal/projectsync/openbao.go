package projectsync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/evil8io/drover/internal/rancherclient"
)

const (
	// openbaoNamespace is the namespace of the OpenBao ServiceAccount in the
	// account project of each cluster. The Role and the RoleBinding in each
	// account namespace have the same name.
	openbaoNamespace = accountPrefix + "openbao"
	openbaoAccount   = "openbao"
	// openbaoLabel is the value of the role label on every OpenBao object.
	openbaoLabel = "openbao"

	// minTokenTTL is the shortest lifetime that the TokenRequest API accepts.
	// It is also the minimum of a credential, because OpenBao creates each
	// credential with a TokenRequest.
	minTokenTTL = 10 * time.Minute

	// maxOpenBaoBody bounds the answer of OpenBao that the service reads.
	maxOpenBaoBody = 1 << 20

	// rancherCASetting is the Rancher setting with the CA chain of a private
	// Rancher certificate. It is empty for a certificate of a public CA.
	rancherCASetting = "cacerts"
	rancherCAPath    = "/v3/settings/" + rancherCASetting
)

// OpenBaoConfig configures the write of the Kubernetes secrets engine config
// of every cluster into OpenBao.
type OpenBaoConfig struct {
	// Address is the OpenBao URL. The scheme is http or https, and the path is
	// empty.
	Address *url.URL
	// AuthPath is the path of the Kubernetes auth mount that the service logs
	// in to.
	AuthPath string
	// Role is the role of that auth mount.
	Role string
	// JWTFile contains the ServiceAccount token of the pod. The service reads
	// it at every login, because the kubelet replaces it.
	JWTFile string
	// MountPrefix is the path prefix of the secrets engine mounts. The config
	// of a cluster is at <MountPrefix>/<cluster>/config.
	MountPrefix string
	// RancherURL is the Rancher URL that OpenBao uses. The scheme is https,
	// and the path is empty.
	RancherURL *url.URL
	// TokenTTL is the requested lifetime of the token of a cluster. The API
	// server can shorten it.
	TokenTTL time.Duration
	// CredentialTTL and CredentialMaxTTL are the default and the longest
	// lifetime of a credential of a project role.
	CredentialTTL    time.Duration
	CredentialMaxTTL time.Duration
}

// openbaoWriter writes the config of each cluster into OpenBao, and keeps the
// expiry of the token that the last write of each cluster sent.
type openbaoWriter struct {
	address          url.URL
	authPath         string
	role             string
	jwtFile          string
	mountPrefix      string
	rancher          url.URL
	ttl              time.Duration
	credentialTTL    time.Duration
	credentialMaxTTL time.Duration
	timeout          time.Duration
	userAgent        string
	client           *http.Client
	tracer           trace.Tracer
	now              func() time.Time

	// loginMu guards session, the kept client token, sessionUntil, the time
	// after which the service logs in again, and retried, which is true after
	// a login for a refused token in this reconcile run.
	loginMu      sync.Mutex
	session      string
	sessionUntil time.Time
	retried      bool

	// mu guards written, verified, and logins. verified has the time of the
	// last read or write of each role and policy, by cluster and object key.
	// logins has the last write of each login role.
	mu       sync.Mutex
	written  map[string]openbaoEntry
	verified map[string]map[string]time.Time
	logins   map[loginKey]loginWrite
}

// openbaoEntry is the token and the Rancher CA of the last successful write of
// a cluster.
type openbaoEntry struct {
	// account is the uid of the ServiceAccount of the token. A new
	// ServiceAccount makes every token of the old one invalid.
	account string
	ca      string
	issued  time.Time
	expiry  time.Time
}

func newOpenBaoWriter(cfg OpenBaoConfig, timeout time.Duration, userAgent string, meterProvider metric.MeterProvider, tracer trace.Tracer) (*openbaoWriter, error) {
	address := cfg.Address
	if address == nil || (address.Scheme != "http" && address.Scheme != "https") || address.Host == "" {
		return nil, errors.New("the OpenBao address is not an http or https URL with a host")
	}
	if p := address.Path; p != "" && p != "/" {
		return nil, fmt.Errorf("the OpenBao address path %q is not empty", p)
	}
	rancher := cfg.RancherURL
	if rancher == nil || rancher.Scheme != "https" || rancher.Host == "" {
		return nil, errors.New("the Rancher URL of OpenBao is not an https URL with a host")
	}
	if p := rancher.Path; p != "" && p != "/" {
		return nil, fmt.Errorf("the Rancher URL path %q of OpenBao is not empty", p)
	}
	if cfg.JWTFile == "" {
		return nil, errors.New("the JWT file of the OpenBao login is required")
	}
	if cfg.TokenTTL < minTokenTTL {
		return nil, fmt.Errorf("the token lifetime %s is shorter than %s", cfg.TokenTTL, minTokenTTL)
	}
	if cfg.CredentialTTL < minTokenTTL || cfg.CredentialMaxTTL < cfg.CredentialTTL {
		return nil, fmt.Errorf("the credential lifetime %s is shorter than %s, or longer than the maximum %s", cfg.CredentialTTL, minTokenTTL, cfg.CredentialMaxTTL)
	}
	authPath, err := cleanPath("auth path", cfg.AuthPath)
	if err != nil {
		return nil, err
	}
	mountPrefix, err := cleanPath("mount prefix", cfg.MountPrefix)
	if err != nil {
		return nil, err
	}
	if mountPrefix != strings.ToLower(mountPrefix) {
		return nil, fmt.Errorf("the OpenBao mount prefix %q has an uppercase letter, and OpenBao stores a policy name in lowercase", mountPrefix)
	}
	if cfg.Role == "" {
		return nil, errors.New("the OpenBao role is empty")
	}

	transport, err := rancherclient.Transport("", false)
	if err != nil {
		return nil, err
	}
	return &openbaoWriter{
		address:          url.URL{Scheme: address.Scheme, Host: address.Host},
		authPath:         authPath,
		role:             cfg.Role,
		jwtFile:          cfg.JWTFile,
		mountPrefix:      mountPrefix,
		rancher:          url.URL{Scheme: rancher.Scheme, Host: rancher.Host},
		ttl:              cfg.TokenTTL,
		credentialTTL:    cfg.CredentialTTL,
		credentialMaxTTL: cfg.CredentialMaxTTL,
		timeout:          timeout,
		userAgent:        userAgent,
		client:           &http.Client{Transport: rancherclient.WrapTransport(transport, meterProvider)},
		tracer:           tracer,
		now:              time.Now,
		written:          make(map[string]openbaoEntry),
		verified:         make(map[string]map[string]time.Time),
	}, nil
}

// cleanPath returns an OpenBao path without the slashes around it. It rejects
// an empty path and an empty, "." or ".." segment.
func cleanPath(name, value string) (string, error) {
	value = strings.Trim(value, "/")
	if value == "" {
		return "", fmt.Errorf("the OpenBao %s is empty", name)
	}
	for _, part := range strings.Split(value, "/") {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("the OpenBao %s %q has an invalid segment", name, value)
		}
	}
	return value, nil
}

func openbaoSpace() accountSpace {
	return accountSpace{
		name:   openbaoNamespace,
		labels: map[string]string{accountRoleKey: openbaoLabel},
		what:   "OpenBao namespace",
		loss:   "the cluster gets no OpenBao config",
	}
}

func openbaoLabels(project string) map[string]string {
	return map[string]string{accountProjectKey: project, accountRoleKey: openbaoLabel}
}

// isOpenBaoBinding reports whether item is an OpenBao RoleBinding of the
// service.
func isOpenBaoBinding(item binding) bool {
	return item.Metadata.Name == openbaoNamespace && item.Metadata.Labels[accountRoleKey] == openbaoLabel
}

// openbaoRole returns the Role in the account namespace of project that lets
// the OpenBao ServiceAccount read the ServiceAccounts of the project roles and
// request their tokens.
func openbaoRole(project string) role {
	names := make([]string, 0, len(accountRoles))
	for _, item := range accountRoles {
		names = append(names, item.name)
	}
	return role{
		APIVersion: rbacAPIVersion,
		Kind:       "Role",
		Metadata: objectMeta{
			Name:      openbaoNamespace,
			Namespace: accountNamespace(project),
			Labels:    openbaoLabels(project),
		},
		Rules: []policyRule{
			{APIGroups: []string{""}, Resources: []string{"serviceaccounts"}, Verbs: []string{"get"}, ResourceNames: names},
			{APIGroups: []string{""}, Resources: []string{"serviceaccounts/token"}, Verbs: []string{"create"}, ResourceNames: slices.Clone(names)},
		},
	}
}

// openbaoRoleBinding returns the RoleBinding in the account namespace of
// project to the Role of openbaoRole, for the OpenBao ServiceAccount. The
// OpenBao namespace with uid owns it, so the garbage collector deletes it with
// that namespace.
func openbaoRoleBinding(project, uid string) binding {
	return binding{
		APIVersion: rbacAPIVersion,
		Kind:       "RoleBinding",
		Metadata: objectMeta{
			Name:            openbaoNamespace,
			Namespace:       accountNamespace(project),
			Labels:          openbaoLabels(project),
			OwnerReferences: []ownerReference{{APIVersion: "v1", Kind: "Namespace", Name: openbaoNamespace, UID: uid}},
		},
		RoleRef:  roleRef{APIGroup: rbacGroup, Kind: "Role", Name: openbaoNamespace},
		Subjects: []subject{{Kind: "ServiceAccount", Name: openbaoAccount, Namespace: openbaoNamespace}},
	}
}

// syncOpenBao keeps the OpenBao objects of cluster: the OpenBao namespace in
// the first account project, its ServiceAccount, and a Role and a RoleBinding
// in the account namespace of every project of trusted. trusted maps a tenant
// project to the uid of its account namespace. roleBindings are the listed
// role bindings with the role label, and roleBindingsIn has them by
// namespace. When the namespace is trusted, syncOpenBao returns its uid and
// the uid of the ServiceAccount. Otherwise it returns "" for both.
func (s *Syncer) syncOpenBao(ctx context.Context, token, cluster string, owners []string, byName map[string]namespace, trusted map[string]string, roleBindings []binding, roleBindingsIn map[string][]binding, run *counters) (string, string) {
	// Only the service and an admin write in an account namespace, so an
	// OpenBao RoleBinding there proves the owner of the OpenBao namespace.
	var proof []binding
	for _, item := range roleBindings {
		name, ok := strings.CutPrefix(item.Metadata.Namespace, accountPrefix)
		if _, known := trusted[name]; ok && known && isOpenBaoBinding(item) {
			proof = append(proof, item)
		}
	}
	item, listed := byName[openbaoNamespace]
	uid, outcome := s.checkAccountNamespace(ctx, token, cluster, openbaoSpace(), owners, item, listed, proof, true, run)
	switch outcome {
	case trustUnknown:
		return "", ""
	case trustNo:
		// A RoleBinding names the ServiceAccount by namespace and name, so it
		// would grant the ServiceAccount of the namespace owner.
		for _, item := range roleBindings {
			if isOpenBaoBinding(item) {
				s.dropBinding(ctx, token, cluster, roleBindingsPath(cluster, item.Metadata.Namespace), item, item.Metadata.Labels[accountProjectKey], run)
			}
		}
		return "", ""
	}

	account := s.ensureOpenBaoAccount(ctx, token, cluster, run)

	roles, err := listAll(ctx, s, token, rolesPath(cluster, ""), accountRoleKey+"="+openbaoLabel, pruneRole)
	if err != nil {
		s.accountFailure(ctx, run, "the role list request failed", err, "cluster", cluster)
		return uid, account
	}
	rolesIn := groupBy(roles, func(item role) string { return item.Metadata.Namespace })
	for _, name := range slices.Sorted(maps.Keys(trusted)) {
		nsName := accountNamespace(name)
		s.ensureOpenBaoAccess(ctx, token, cluster, name, uid, rolesIn[nsName], roleBindingsIn[nsName], run)
	}
	return uid, account
}

// ensureOpenBaoAccess reconciles the OpenBao Role and RoleBinding in the
// account namespace of project name. uid is the uid of the trusted OpenBao
// namespace. roles and bindings are the listed objects of that account
// namespace. Without them, the service creates each object first, and reads
// it after a 409.
func (s *Syncer) ensureOpenBaoAccess(ctx context.Context, token, cluster, name, uid string, roles []role, bindings []binding, run *counters) {
	wantRole := openbaoRole(name)
	s.reconcileRole(ctx, token, cluster, wantRole, findRole(roles, wantRole.Metadata.Name), name, run)
	wantBinding := openbaoRoleBinding(name, uid)
	s.reconcileBinding(ctx, token, cluster, roleBindingsPath(cluster, accountNamespace(name)), wantBinding,
		findBinding(bindings, wantBinding.Metadata.Name), name, run)
}

func findRole(items []role, name string) *role {
	for i := range items {
		if items[i].Metadata.Name == name {
			return &items[i]
		}
	}
	return nil
}

// ensureOpenBaoAccount creates the OpenBao ServiceAccount of cluster when it is
// missing. It returns the uid of the ServiceAccount, or "" when a request
// fails.
func (s *Syncer) ensureOpenBaoAccount(ctx context.Context, token, cluster string, run *counters) string {
	collection := serviceAccountsPath(cluster, openbaoNamespace)
	var item object
	found, err := s.getObject(ctx, token, collection+"/"+openbaoAccount, &item)
	if err != nil {
		s.accountFailure(ctx, run, "the service account request failed", err, "cluster", cluster, "name", openbaoAccount)
		return ""
	}
	if found {
		return item.Metadata.UID
	}
	body := object{
		APIVersion: "v1",
		Kind:       "ServiceAccount",
		Metadata: objectMeta{
			Name:      openbaoAccount,
			Namespace: openbaoNamespace,
			Labels:    map[string]string{accountRoleKey: openbaoLabel},
		},
	}
	var created object
	if err := s.send(ctx, token, http.MethodPost, collection, body, &created); err != nil {
		s.accountFailure(ctx, run, "the service account create failed", err, "cluster", cluster, "name", openbaoAccount)
		return ""
	}
	s.accountChanged(ctx, run, cluster, "serviceaccount", "create", openbaoNamespace, openbaoAccount, "")
	return created.Metadata.UID
}

// openbaoTarget is a cluster whose OpenBao state the run keeps.
type openbaoTarget struct {
	// account is the uid of the OpenBao ServiceAccount of the cluster.
	account string
	// tenants are the tenant projects with a trusted account namespace.
	tenants []string
}

// refreshOpenBao keeps the OpenBao state of every cluster of ready: its
// mount, its roles and policies, and its config when that is due. The service
// reads the Rancher CA once per run. A failed read of the CA writes nothing,
// so that no write clears the CA of a private Rancher certificate. projects
// are the projects of the run by cluster, and a role of a project outside
// them is a candidate for a delete. names are the clusters of the run, and
// the writer forgets every other cluster.
func (s *Syncer) refreshOpenBao(ctx context.Context, token string, names []string, ready map[string]openbaoTarget, projects map[string]map[string]project, run *counters) {
	w := s.openbao
	w.keep(names)
	w.startRun()
	if len(ready) == 0 {
		return
	}
	ca, err := s.rancherCA(ctx, token)
	if err != nil {
		run.errors++
		s.logFailure(ctx, slog.LevelError, "the Rancher CA request failed", err)
		return
	}
	due := w.due(ready, ca)

	if _, err := w.token(ctx, ""); err != nil {
		run.errors++
		s.logFailure(ctx, slog.LevelError, "the OpenBao login failed", err)
		for _, cluster := range due {
			s.metrics.openbaoWritten(ctx, cluster, outcomeError)
		}
		return
	}
	policies, err := w.listPolicies(ctx)
	if err != nil {
		run.errors++
		s.logFailure(ctx, slog.LevelError, "the OpenBao policy list request failed", err)
	}
	if policies != nil {
		run.errors += s.dropOrphanPolicies(ctx, token, names, projects, policies)
		run.errors += s.dropGoneClusters(ctx, token, names, policies)
	}

	var mu sync.Mutex
	eachCluster(slices.Sorted(maps.Keys(ready)), func(cluster string) {
		errs := s.keepOpenBaoCluster(ctx, token, cluster, ready[cluster], projects[cluster], policies, slices.Contains(due, cluster), ca)
		mu.Lock()
		defer mu.Unlock()
		run.errors += errs
	})
	// Each login role contains the name of the ACL policy of a project role,
	// so the service writes the login roles after the policies.
	if rules := s.trustRules(); rules != nil {
		run.errors += s.refreshTrust(ctx, token, names, ready, projects, rules)
	}
}

// writeOpenBao requests a new token of the OpenBao ServiceAccount of cluster,
// and writes it into the config of the cluster, with the Rancher CA ca.
// account is the uid of that ServiceAccount. It returns false when the write
// fails with an error. A missing mount is no error, and the next run tries
// again.
func (s *Syncer) writeOpenBao(ctx context.Context, token, cluster, account, ca string) bool {
	ctx, span := s.tracer.Start(ctx, "openbao_config", trace.WithAttributes(attribute.String("drover.cluster", cluster)))
	defer span.End()
	w := s.openbao

	issued := w.now()
	jwt, expiry, err := s.requestToken(ctx, token, cluster)
	if err == nil && !expiry.After(issued) {
		err = fmt.Errorf("the token expires at %s, not after the request", expiry.Format(time.RFC3339))
	}
	if err != nil {
		s.openbaoFailure(ctx, span, cluster, "the token request failed", err)
		return false
	}

	err = w.writeConfig(ctx, cluster, jwt, ca)
	if missingMount(err) {
		s.metrics.openbaoWritten(ctx, cluster, outcomeMissingMount)
		s.logger.InfoContext(ctx, "the OpenBao mount does not exist yet",
			"cluster", cluster, "mount", w.mount(cluster), "error", err.Error())
		return true
	}
	if err != nil {
		s.openbaoFailure(ctx, span, cluster, "the OpenBao config write failed", err)
		return false
	}

	w.remember(cluster, openbaoEntry{account: account, ca: ca, issued: issued, expiry: expiry})
	w.markVerified(cluster, openbaoConfigKey)
	s.metrics.openbaoWritten(ctx, cluster, outcomeOK)
	s.logger.InfoContext(ctx, "the OpenBao config is written",
		"cluster", cluster, "expires", expiry.UTC().Format(time.RFC3339))
	return true
}

func (s *Syncer) openbaoFailure(ctx context.Context, span trace.Span, cluster, message string, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	s.metrics.openbaoWritten(ctx, cluster, outcomeError)
	s.logFailure(ctx, slog.LevelError, message, err, "cluster", cluster)
}

// requestToken asks the API server of cluster, through the Rancher proxy, for
// a token of the OpenBao ServiceAccount. It returns the token and its expiry.
func (s *Syncer) requestToken(ctx context.Context, token, cluster string) (string, time.Time, error) {
	body := map[string]any{
		"apiVersion": "authentication.k8s.io/v1",
		"kind":       "TokenRequest",
		"spec":       map[string]any{"expirationSeconds": int64(s.openbao.ttl / time.Second)},
	}
	var answer struct {
		Status struct {
			Token               string    `json:"token"`
			ExpirationTimestamp time.Time `json:"expirationTimestamp"`
		} `json:"status"`
	}
	path := serviceAccountsPath(cluster, openbaoNamespace) + "/" + openbaoAccount + "/token"
	if err := s.send(ctx, token, http.MethodPost, path, body, &answer); err != nil {
		return "", time.Time{}, err
	}
	if answer.Status.Token == "" {
		return "", time.Time{}, errors.New("the token request answer has no token")
	}
	return answer.Status.Token, answer.Status.ExpirationTimestamp, nil
}

// rancherCA returns the value of the Rancher setting cacerts: the CA chain of
// a private Rancher certificate, or "" for a certificate of a public CA.
func (s *Syncer) rancherCA(ctx context.Context, token string) (string, error) {
	status, body, err := s.do(ctx, http.MethodGet, s.target(rancherCAPath, nil), token, "", nil)
	if err != nil {
		return "", err
	}
	if status != http.StatusOK {
		return "", newStatusError(http.MethodGet, rancherCAPath, status, body)
	}
	var answer struct {
		ID    string `json:"id"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal(body, &answer); err != nil {
		return "", fmt.Errorf("decode the answer of GET %s: %w", rancherCAPath, err)
	}
	if answer.ID != rancherCASetting {
		return "", fmt.Errorf("the answer of GET %s is not the setting %s", rancherCAPath, rancherCASetting)
	}
	return answer.Value, nil
}

// keep forgets every cluster that names does not have.
func (w *openbaoWriter) keep(names []string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	maps.DeleteFunc(w.written, func(cluster string, _ openbaoEntry) bool {
		return !slices.Contains(names, cluster)
	})
	maps.DeleteFunc(w.verified, func(cluster string, _ map[string]time.Time) bool {
		return !slices.Contains(names, cluster)
	})
}

// due returns the clusters of ready that need a write, sorted. A cluster needs
// one without an entry, after a change of its ServiceAccount or of the Rancher
// CA ca, and when less than half of the lifetime of its token remains. OpenBao
// keeps a written token and never renews it, so the other half is the time
// left for a retry.
func (w *openbaoWriter) due(ready map[string]openbaoTarget, ca string) []string {
	now := w.now()
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []string
	for _, cluster := range slices.Sorted(maps.Keys(ready)) {
		entry, ok := w.written[cluster]
		if !ok || entry.account != ready[cluster].account || entry.ca != ca ||
			entry.expiry.Sub(now) < entry.expiry.Sub(entry.issued)/2 {
			out = append(out, cluster)
		}
	}
	return out
}

func (w *openbaoWriter) remember(cluster string, entry openbaoEntry) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.written[cluster] = entry
}

// errRetried is the answer of token for a refused token after a login for a
// refused token in the same reconcile run.
var errRetried = errors.New("the service logged in again after a 403 in this run already")

// startRun lets the next refused token of the reconcile run start a login.
func (w *openbaoWriter) startRun() {
	w.loginMu.Lock()
	defer w.loginMu.Unlock()
	w.retried = false
}

// token returns a client token of OpenBao. It keeps the token of a login
// until a fifth of its lifetime remains, and then logs in again. stale is a
// token that OpenBao refused, and the service never returns it again. For a
// stale token inside its lifetime, token logs in again at most once per
// reconcile run, and returns errRetried after that.
func (w *openbaoWriter) token(ctx context.Context, stale string) (string, error) {
	w.loginMu.Lock()
	defer w.loginMu.Unlock()
	valid := w.session != "" && w.now().Before(w.sessionUntil)
	if valid && w.session != stale {
		return w.session, nil
	}
	if valid {
		if w.retried {
			return "", errRetried
		}
		w.retried = true
	}
	w.session = ""
	token, lease, err := w.login(ctx)
	if err != nil {
		return "", err
	}
	w.session, w.sessionUntil = token, w.now().Add(lease-lease/5)
	return token, nil
}

// login logs in to the Kubernetes auth mount of OpenBao with the token of the
// pod, and returns the client token and its lifetime.
func (w *openbaoWriter) login(ctx context.Context) (string, time.Duration, error) {
	ctx, span := w.tracer.Start(ctx, "openbao_login")
	defer span.End()
	token, lease, err := w.loginOnce(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return token, lease, err
}

func (w *openbaoWriter) loginOnce(ctx context.Context) (string, time.Duration, error) {
	jwt, err := rancherclient.ReadToken(w.jwtFile)
	if err != nil {
		return "", 0, fmt.Errorf("read the JWT file: %w", err)
	}
	var answer struct {
		Auth struct {
			ClientToken   string `json:"client_token"`
			LeaseDuration int64  `json:"lease_duration"`
		} `json:"auth"`
	}
	body := map[string]string{"role": w.role, "jwt": jwt}
	if err := w.send(ctx, http.MethodPost, "auth/"+w.authPath+"/login", nil, "", body, &answer); err != nil {
		return "", 0, err
	}
	if answer.Auth.ClientToken == "" {
		return "", 0, errors.New("the OpenBao login answer has no client token")
	}
	return answer.Auth.ClientToken, time.Duration(answer.Auth.LeaseDuration) * time.Second, nil
}

// call sends one request to OpenBao with the kept client token. A 403 answer
// logs in again and repeats the request, because a kept token can expire.
// After one such login in a reconcile run, a 403 is the answer of the
// request, because the next login gets the same rights.
func (w *openbaoWriter) call(ctx context.Context, method, path string, query url.Values, body, out any) error {
	// A login error keeps only its text, so that no caller reads its status
	// as the status of the request.
	token, err := w.token(ctx, "")
	if err != nil {
		return fmt.Errorf("log in to OpenBao: %v", err)
	}
	err = w.send(ctx, method, path, query, token, body, out)
	if !hasOpenBaoStatus(err, http.StatusForbidden) {
		return err
	}
	retry, loginErr := w.token(ctx, token)
	if errors.Is(loginErr, errRetried) {
		return err
	}
	if loginErr != nil {
		return fmt.Errorf("log in to OpenBao: %v", loginErr)
	}
	return w.send(ctx, method, path, query, retry, body, out)
}

// openbaoConfig is the part of the config of a mount that a read returns.
// OpenBao never returns service_account_jwt.
type openbaoConfig struct {
	Host              string `json:"kubernetes_host"`
	CACert            string `json:"kubernetes_ca_cert"`
	DisableLocalCAJWT bool   `json:"disable_local_ca_jwt"`
}

// wantConfig returns the config of the mount of cluster: the engine reaches
// the cluster through the Rancher proxy, and verifies Rancher with ca. An
// empty ca clears the stored CA, so that OpenBao verifies with the system
// roots.
func (w *openbaoWriter) wantConfig(cluster, ca string) openbaoConfig {
	host := w.rancher
	host.Path = clusterPath(cluster)
	return openbaoConfig{Host: host.String(), CACert: ca, DisableLocalCAJWT: true}
}

// writeConfig writes the config of the secrets engine mount of cluster, with
// jwt for the requests of the engine to the cluster.
func (w *openbaoWriter) writeConfig(ctx context.Context, cluster, jwt, ca string) error {
	body := struct {
		openbaoConfig
		JWT string `json:"service_account_jwt"`
	}{openbaoConfig: w.wantConfig(cluster, ca), JWT: jwt}
	return w.call(ctx, http.MethodPost, w.mount(cluster)+"/config", nil, body, nil)
}

// readConfig reads the config of the mount of cluster. It returns false and
// no error when OpenBao answers 404, as it does for a mount without config.
func (w *openbaoWriter) readConfig(ctx context.Context, cluster string) (openbaoConfig, bool, error) {
	var answer struct {
		Data openbaoConfig `json:"data"`
	}
	err := w.call(ctx, http.MethodGet, w.mount(cluster)+"/config", nil, nil, &answer)
	if hasOpenBaoStatus(err, http.StatusNotFound) {
		return openbaoConfig{}, false, nil
	}
	return answer.Data, err == nil, err
}

// configStale reports whether the mount of cluster has no config, or a config
// that differs from the config with ca.
func (s *Syncer) configStale(ctx context.Context, cluster, ca string) (bool, error) {
	have, found, err := s.openbao.readConfig(ctx, cluster)
	if err != nil {
		return false, err
	}
	return !found || have != s.openbao.wantConfig(cluster, ca), nil
}

// send sends a request with method to the API path of OpenBao, with the
// client token when it is not empty, and decodes the answer into out when out
// is not nil. A nil body sends none. A status outside 2xx returns an
// openbaoError. No error has the body of the request.
func (w *openbaoWriter) send(ctx context.Context, method, path string, query url.Values, clientToken string, body, out any) error {
	target := w.address
	target.Path = "/v1/" + path
	target.RawQuery = query.Encode()
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode the body of %s %s: %w", method, target.Path, err)
		}
		reader = bytes.NewReader(data)
	}

	ctx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, target.String(), reader)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", jsonType)
	req.Header.Set("Accept", jsonType)
	req.Header.Set("User-Agent", w.userAgent)
	if clientToken != "" {
		req.Header.Set("X-Vault-Token", clientToken)
	}

	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	answer, err := io.ReadAll(io.LimitReader(resp.Body, maxOpenBaoBody))
	if err != nil {
		return fmt.Errorf("read the answer of %s %s: %w", method, target.Path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return newOpenBaoError(method, target.Path, resp.StatusCode, answer)
	}
	if out != nil {
		if err := json.Unmarshal(answer, out); err != nil {
			return fmt.Errorf("decode the answer of %s %s: %w", method, target.Path, err)
		}
	}
	return nil
}

// openbaoError is an answer of OpenBao with a status outside 2xx.
type openbaoError struct {
	method   string
	path     string
	status   int
	messages []string
}

// newOpenBaoError returns the error of an answer with status. It reads the
// messages from the errors list of the body, and it accepts a body without one.
func newOpenBaoError(method, path string, status int, body []byte) openbaoError {
	err := openbaoError{method: method, path: path, status: status}
	var answer struct {
		Errors []string `json:"errors"`
	}
	if json.Unmarshal(body, &answer) == nil {
		err.messages = answer.Errors
	}
	return err
}

func (e openbaoError) Error() string {
	if len(e.messages) == 0 {
		return fmt.Sprintf("%s %s returned status %d", e.method, e.path, e.status)
	}
	return fmt.Sprintf("%s %s returned status %d: %s", e.method, e.path, e.status, strings.Join(e.messages, "; "))
}

// hasOpenBaoStatus reports whether err is an openbaoError with status.
func hasOpenBaoStatus(err error, status int) bool {
	var answer openbaoError
	return errors.As(err, &answer) && answer.status == status
}

// missingMount reports whether err is the answer of OpenBao to a path under a
// mount that does not exist. OpenBao answers 404 with "no handler for route",
// and an older server answers 400 with the same message.
func missingMount(err error) bool {
	var answer openbaoError
	if !errors.As(err, &answer) {
		return false
	}
	switch answer.status {
	case http.StatusNotFound:
		return true
	case http.StatusBadRequest:
		return slices.ContainsFunc(answer.messages, func(message string) bool {
			return strings.Contains(message, "no handler for route")
		})
	}
	return false
}
