package filter

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"
)

const (
	// serviceAccountPrefix starts the subject of a ServiceAccount token.
	// Rancher applies the same test to pick its ServiceAccount authenticator.
	serviceAccountPrefix = "system:serviceaccount:"
	bearerScheme         = "bearer "

	rulesReviewPath = "/apis/authorization.k8s.io/v1/selfsubjectrulesreviews"
	// clusterRulesNamespace is the namespace of the rules review. No
	// namespace can have a name with a colon, so no RoleBinding is in it, and
	// the review returns the rules of the ClusterRoleBindings only.
	clusterRulesNamespace = "drover:cluster-rules"
)

// resourceRule is the part of a ResourceRule of a SelfSubjectRulesReview that
// the RBAC leg reads.
type resourceRule struct {
	Verbs         []string `json:"verbs"`
	APIGroups     []string `json:"apiGroups"`
	Resources     []string `json:"resources"`
	ResourceNames []string `json:"resourceNames"`
}

// rulesReview is the part of a SelfSubjectRulesReview answer that the RBAC
// leg reads.
type rulesReview struct {
	Status struct {
		ResourceRules   []resourceRule `json:"resourceRules"`
		Incomplete      bool           `json:"incomplete"`
		EvaluationError string         `json:"evaluationError"`
	} `json:"status"`
}

// serviceAccountSubject reports whether auth is a bearer JWT of a Kubernetes
// ServiceAccount: a token of three segments whose payload has a sub with the
// ServiceAccount prefix, a namespace, and a name. It returns the subject. The
// API server verifies the token. The filter reads the claim only to pick the
// leg of the allowed-set fetch, and it trusts the subject only after the API
// server answered a review for that token.
func serviceAccountSubject(auth string) (string, bool) {
	if len(auth) < len(bearerScheme) || !strings.EqualFold(auth[:len(bearerScheme)], bearerScheme) {
		return "", false
	}
	segments := strings.Split(strings.TrimSpace(auth[len(bearerScheme):]), ".")
	if len(segments) != 3 {
		return "", false
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(segments[1], "="))
	if err != nil {
		return "", false
	}
	var claims struct {
		Subject string `json:"sub"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return "", false
	}
	rest, found := strings.CutPrefix(claims.Subject, serviceAccountPrefix)
	if !found {
		return "", false
	}
	namespace, name, found := strings.Cut(rest, ":")
	if !found || namespace == "" || name == "" {
		return "", false
	}
	return claims.Subject, true
}

// fetchRules reads the namespace names that a ServiceAccount caller may get,
// from a SelfSubjectRulesReview with the credentials of the caller. The review
// runs in clusterRulesNamespace, so it has the rules of the ClusterRoleBindings
// of the caller only. A Role grants its rules in its own namespace only, so a
// get on namespaces from a RoleBinding does not let the caller get a
// namespace with another name. The name set is the union of the
// resourceNames of the rules that grant get on namespaces. A rule without
// names grants get on every namespace and separates no tenant, and no rule
// grants nothing. Both give a denied set, which the cache keeps for one TTL.
// A non-nil response is the 401 or 403 answer of the API server.
func (s *Service) fetchRules(ctx context.Context, cluster, auth, subject string) (allowedSet, *http.Response, error) {
	body, err := json.Marshal(map[string]any{
		"apiVersion": "authorization.k8s.io/v1",
		"kind":       "SelfSubjectRulesReview",
		"spec":       map[string]string{"namespace": clusterRulesNamespace},
	})
	if err != nil {
		return allowedSet{}, nil, err
	}
	target := *s.upstream
	target.Path = "/k8s/clusters/" + cluster + rulesReviewPath
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(body))
	if err != nil {
		return allowedSet{}, nil, err
	}
	req.Header.Set("Authorization", auth)
	req.Header.Set("Content-Type", jsonContentType)
	req.Header.Set("Accept", jsonContentType)

	resp, err := s.base.RoundTrip(req)
	if err != nil {
		return allowedSet{}, nil, fmt.Errorf("rules review request failed: %w", err)
	}
	answer, tooLarge, err := readLimited(resp.Body, maxSteveBody)
	_ = resp.Body.Close()
	if err != nil {
		return allowedSet{}, nil, fmt.Errorf("rules review response: %w", err)
	}
	if tooLarge {
		return allowedSet{}, nil, fmt.Errorf("rules review response is larger than %d bytes", maxSteveBody)
	}
	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated:
	case http.StatusUnauthorized, http.StatusForbidden:
		resp.Body = io.NopCloser(bytes.NewReader(answer))
		resp.ContentLength = int64(len(answer))
		return allowedSet{}, resp, nil
	default:
		return allowedSet{}, nil, fmt.Errorf("rules review request returned %s", resp.Status)
	}

	var review rulesReview
	if err := json.Unmarshal(answer, &review); err != nil {
		return allowedSet{}, nil, fmt.Errorf("rules review response: %w", err)
	}
	if review.Status.Incomplete {
		s.logger.DebugContext(ctx, "the rules review is incomplete", "cluster", cluster, "reason", review.Status.EvaluationError)
	}
	names, bounded := namespaceNames(review.Status.ResourceRules)
	if !bounded || len(names) == 0 {
		s.logger.DebugContext(ctx, "the rules of the caller name no namespace", "cluster", cluster, "bounded", bounded)
		return allowedSet{denied: true}, nil, nil
	}
	if len(names) > maxAllowedNames {
		return allowedSet{}, nil, fmt.Errorf("allowed set has more than %d names", maxAllowedNames)
	}
	s.clusters.add(cluster)
	return allowedSet{names: names, extras: names, user: subject}, nil, nil
}

// namespaceNames returns the sorted names of the namespaces that rules let the
// caller get, with no duplicates. It reports false when a rule grants get on
// namespaces without names.
func namespaceNames(rules []resourceRule) ([]string, bool) {
	set := make(map[string]struct{})
	for _, rule := range rules {
		if !grantsGetNamespaces(rule) {
			continue
		}
		if len(rule.ResourceNames) == 0 {
			return nil, false
		}
		for _, name := range rule.ResourceNames {
			if name != "" {
				set[name] = struct{}{}
			}
		}
	}
	return slices.Sorted(maps.Keys(set)), true
}

// grantsGetNamespaces reports whether rule grants get on the namespaces
// resource of the core group. A wildcard matches too.
func grantsGetNamespaces(rule resourceRule) bool {
	return matchesRule(rule.Verbs, "get") && matchesRule(rule.APIGroups, "") && matchesRule(rule.Resources, "namespaces")
}

// matchesRule reports whether values has value or the wildcard.
func matchesRule(values []string, value string) bool {
	return slices.Contains(values, value) || slices.Contains(values, "*")
}
