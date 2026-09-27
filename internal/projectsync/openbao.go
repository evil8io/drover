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
}

// openbaoWriter writes the config of each cluster into OpenBao, and keeps the
// expiry of the token that the last write of each cluster sent.
type openbaoWriter struct {
	address     url.URL
	authPath    string
	role        string
	jwtFile     string
	mountPrefix string
	rancher     url.URL
	ttl         time.Duration
	timeout     time.Duration
	userAgent   string
	client      *http.Client
	now         func() time.Time

	mu      sync.Mutex
	written map[string]openbaoEntry
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

func newOpenBaoWriter(cfg OpenBaoConfig, timeout time.Duration, userAgent string, meterProvider metric.MeterProvider) (*openbaoWriter, error) {
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
	authPath, err := cleanPath("auth path", cfg.AuthPath)
	if err != nil {
		return nil, err
	}
	mountPrefix, err := cleanPath("mount prefix", cfg.MountPrefix)
	if err != nil {
		return nil, err
	}
	if cfg.Role == "" {
		return nil, errors.New("the OpenBao role is empty")
	}

	transport, err := rancherclient.Transport("", false)
	if err != nil {
		return nil, err
	}
	return &openbaoWriter{
		address:     url.URL{Scheme: address.Scheme, Host: address.Host},
		authPath:    authPath,
		role:        cfg.Role,
		jwtFile:     cfg.JWTFile,
		mountPrefix: mountPrefix,
		rancher:     url.URL{Scheme: rancher.Scheme, Host: rancher.Host},
		ttl:         cfg.TokenTTL,
		timeout:     timeout,
		userAgent:   userAgent,
		client:      &http.Client{Transport: rancherclient.WrapTransport(transport, meterProvider)},
		now:         time.Now,
		written:     make(map[string]openbaoEntry),
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
// namespace. syncOpenBao returns the uid of the ServiceAccount when the
// namespace is trusted, and "" otherwise.
func (s *Syncer) syncOpenBao(ctx context.Context, token, cluster string, owners []string, byName map[string]namespace, trusted map[string]string, roleBindings []binding, roleBindingsIn map[string][]binding, run *counters) string {
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
		return ""
	case trustNo:
		// A RoleBinding names the ServiceAccount by namespace and name, so it
		// would grant the ServiceAccount of the namespace owner.
		for _, item := range roleBindings {
			if isOpenBaoBinding(item) {
				s.dropBinding(ctx, token, cluster, roleBindingsPath(cluster, item.Metadata.Namespace), item, item.Metadata.Labels[accountProjectKey], run)
			}
		}
		return ""
	}

	account := s.ensureOpenBaoAccount(ctx, token, cluster, run)

	roles, err := listAll(ctx, s, token, rolesPath(cluster, ""), accountRoleKey+"="+openbaoLabel, pruneRole)
	if err != nil {
		s.accountFailure(ctx, run, "the role list request failed", err, "cluster", cluster)
		return account
	}
	rolesIn := groupBy(roles, func(item role) string { return item.Metadata.Namespace })
	for _, name := range slices.Sorted(maps.Keys(trusted)) {
		nsName := accountNamespace(name)
		wantRole := openbaoRole(name)
		s.reconcileRole(ctx, token, cluster, wantRole, findRole(rolesIn[nsName], wantRole.Metadata.Name), name, run)
		wantBinding := openbaoRoleBinding(name, uid)
		s.reconcileBinding(ctx, token, cluster, roleBindingsPath(cluster, nsName), wantBinding,
			findBinding(roleBindingsIn[nsName], wantBinding.Metadata.Name), name, run)
	}
	return account
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

// refreshOpenBao writes the config of every cluster of ready that is due.
// ready maps a cluster to the uid of its OpenBao ServiceAccount. The service
// reads the Rancher CA once, and logs in once, only when a cluster is due. A
// failed read of the CA writes nothing, so that no write clears the CA of a
// private Rancher certificate. names are the clusters of the run, and the
// writer forgets every other cluster.
func (s *Syncer) refreshOpenBao(ctx context.Context, token string, names []string, ready map[string]string, run *counters) {
	w := s.openbao
	w.keep(names)
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
	if len(due) == 0 {
		return
	}

	clientToken, err := s.loginOpenBao(ctx)
	if err != nil {
		run.errors++
		s.logFailure(ctx, slog.LevelError, "the OpenBao login failed", err)
		for _, cluster := range due {
			s.metrics.openbaoWritten(ctx, cluster, outcomeError)
		}
		return
	}

	var mu sync.Mutex
	eachCluster(due, func(cluster string) {
		if s.writeOpenBao(ctx, token, clientToken, cluster, ready[cluster], ca) {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		run.errors++
	})
}

func (s *Syncer) loginOpenBao(ctx context.Context) (string, error) {
	ctx, span := s.tracer.Start(ctx, "openbao_login")
	defer span.End()
	clientToken, err := s.openbao.login(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return clientToken, err
}

// writeOpenBao requests a new token of the OpenBao ServiceAccount of cluster,
// and writes it into the config of the cluster, with the Rancher CA ca.
// account is the uid of that ServiceAccount. It returns false when the write
// fails with an error. A missing mount is no error, and the next run tries
// again.
func (s *Syncer) writeOpenBao(ctx context.Context, token, clientToken, cluster, account, ca string) bool {
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

	err = w.writeConfig(ctx, clientToken, cluster, jwt, ca)
	if missingMount(err) {
		s.metrics.openbaoWritten(ctx, cluster, outcomeMissingMount)
		s.logger.InfoContext(ctx, "the OpenBao mount does not exist yet",
			"cluster", cluster, "mount", w.mountPrefix+"/"+cluster, "error", err.Error())
		return true
	}
	if err != nil {
		s.openbaoFailure(ctx, span, cluster, "the OpenBao config write failed", err)
		return false
	}

	w.remember(cluster, openbaoEntry{account: account, ca: ca, issued: issued, expiry: expiry})
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
}

// due returns the clusters of ready that need a write, sorted. A cluster needs
// one without an entry, after a change of its ServiceAccount or of the Rancher
// CA ca, and when less than half of the lifetime of its token remains. OpenBao
// keeps a written token and never renews it, so the other half is the time
// left for a retry.
func (w *openbaoWriter) due(ready map[string]string, ca string) []string {
	now := w.now()
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []string
	for _, cluster := range slices.Sorted(maps.Keys(ready)) {
		entry, ok := w.written[cluster]
		if !ok || entry.account != ready[cluster] || entry.ca != ca ||
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

// login logs in to the Kubernetes auth mount of OpenBao with the token of the
// pod, and returns the client token.
func (w *openbaoWriter) login(ctx context.Context) (string, error) {
	jwt, err := rancherclient.ReadToken(w.jwtFile)
	if err != nil {
		return "", fmt.Errorf("read the JWT file: %w", err)
	}
	var answer struct {
		Auth struct {
			ClientToken string `json:"client_token"`
		} `json:"auth"`
	}
	body := map[string]string{"role": w.role, "jwt": jwt}
	if err := w.post(ctx, "auth/"+w.authPath+"/login", "", body, &answer); err != nil {
		return "", err
	}
	if answer.Auth.ClientToken == "" {
		return "", errors.New("the OpenBao login answer has no client token")
	}
	return answer.Auth.ClientToken, nil
}

// writeConfig writes the config of the secrets engine mount of cluster. The
// engine reaches the cluster through the Rancher proxy, with jwt, and verifies
// Rancher with ca. An empty ca clears the stored CA, so that OpenBao verifies
// with the system roots.
func (w *openbaoWriter) writeConfig(ctx context.Context, clientToken, cluster, jwt, ca string) error {
	host := w.rancher
	host.Path = clusterPath(cluster)
	body := struct {
		Host              string `json:"kubernetes_host"`
		CACert            string `json:"kubernetes_ca_cert"`
		JWT               string `json:"service_account_jwt"`
		DisableLocalCAJWT bool   `json:"disable_local_ca_jwt"`
	}{Host: host.String(), CACert: ca, JWT: jwt, DisableLocalCAJWT: true}
	return w.post(ctx, w.mountPrefix+"/"+cluster+"/config", clientToken, body, nil)
}

// post sends body to the API path of OpenBao, with the client token when it
// is not empty, and decodes the answer into out when out is not nil. A status
// outside 2xx returns an openbaoError. No error has the body of the request.
func (w *openbaoWriter) post(ctx context.Context, path, clientToken string, body, out any) error {
	target := w.address
	target.Path = "/v1/" + path
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("encode the body of POST %s: %w", target.Path, err)
	}

	ctx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(data))
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
		return fmt.Errorf("read the answer of POST %s: %w", target.Path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return newOpenBaoError(target.Path, resp.StatusCode, answer)
	}
	if out != nil {
		if err := json.Unmarshal(answer, out); err != nil {
			return fmt.Errorf("decode the answer of POST %s: %w", target.Path, err)
		}
	}
	return nil
}

// openbaoError is an answer of OpenBao with a status outside 2xx.
type openbaoError struct {
	path     string
	status   int
	messages []string
}

// newOpenBaoError returns the error of an answer with status. It reads the
// messages from the errors list of the body, and it accepts a body without one.
func newOpenBaoError(path string, status int, body []byte) openbaoError {
	err := openbaoError{path: path, status: status}
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
		return fmt.Sprintf("POST %s returned status %d", e.path, e.status)
	}
	return fmt.Sprintf("POST %s returned status %d: %s", e.path, e.status, strings.Join(e.messages, "; "))
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
