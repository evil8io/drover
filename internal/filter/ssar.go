package filter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
)

const (
	maxReviewBody   = 64 << 10
	grantedReason   = "granted by " + serviceName
	outcomeGrant    = "granted"
	jsonContentType = "application/json"
)

// resourceAttributes has the fields of a ResourceAttributes that the filter reads.
type resourceAttributes struct {
	Namespace   string `json:"namespace,omitempty"`
	Verb        string `json:"verb,omitempty"`
	Group       string `json:"group,omitempty"`
	Resource    string `json:"resource,omitempty"`
	Subresource string `json:"subresource,omitempty"`
	Name        string `json:"name,omitempty"`
}

// reviewSpec has the fields of a SelfSubjectAccessReviewSpec that the filter reads.
type reviewSpec struct {
	resource    *resourceAttributes
	nonResource bool
}

func (s *Service) roundTripReview(req *http.Request, cluster string) (*http.Response, error) {
	if hasImpersonation(req.Header) {
		resp, err := s.base.RoundTrip(req)
		if err != nil {
			return nil, s.reviewError(req.Context(), cluster, err)
		}
		s.logReview(req.Context(), cluster, outcomePassthrough, resp.StatusCode, "")
		return resp, nil
	}

	body, tooLarge, err := readLimited(req.Body, maxReviewBody)
	if req.Body != nil {
		_ = req.Body.Close()
	}
	if err != nil {
		return nil, s.reviewError(req.Context(), cluster, err)
	}
	if tooLarge {
		s.logReview(req.Context(), cluster, outcomeError, http.StatusRequestEntityTooLarge, "")
		message := serviceName + ": the request body is larger than " + strconv.Itoa(maxReviewBody) + " bytes"
		return statusResponse(req, http.StatusRequestEntityTooLarge, reasonTooLarge, message), nil
	}

	out := req.Clone(req.Context())
	setBody(out, body)

	match, replacement := s.filteredReview(req.Header.Get("Content-Type"), body)
	if !match {
		resp, err := s.base.RoundTrip(out)
		if err != nil {
			return nil, s.reviewError(req.Context(), cluster, err)
		}
		s.logReview(req.Context(), cluster, outcomePassthrough, resp.StatusCode, "")
		return resp, nil
	}

	if replacement != nil {
		setBody(out, replacement)
		out.Header.Set("Content-Type", jsonContentType)
		out.Header.Set("Accept", jsonContentType)
	}
	out.Header.Del("Accept-Encoding")
	resp, err := s.base.RoundTrip(out)
	if err != nil {
		return nil, s.reviewError(req.Context(), cluster, err)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		s.logReview(req.Context(), cluster, outcomeNative, resp.StatusCode, "")
		return resp, nil
	}

	answer, tooLarge, err := readLimited(resp.Body, maxReviewBody)
	if err != nil {
		_ = resp.Body.Close()
		return nil, s.reviewError(req.Context(), cluster, err)
	}
	if tooLarge {
		resp.Body = joinedBody{Reader: io.MultiReader(bytes.NewReader(answer), resp.Body), Closer: resp.Body}
		s.logReview(req.Context(), cluster, outcomeNative, resp.StatusCode, "")
		return resp, nil
	}
	_ = resp.Body.Close()

	if !reviewDenied(answer) {
		return s.nativeReview(req.Context(), cluster, "", resp, answer)
	}

	// The API server echoes the spec it evaluated. Its parser matches keys
	// with case, and this one does not.
	echoedSpec, err := jsonReviewSpec(answer)
	if err != nil || !s.filteredVerb(echoedSpec) {
		return s.nativeReview(req.Context(), cluster, "", resp, answer)
	}

	set, denied, err := s.allowed(req.Context(), cluster, req.Header)
	if denied != nil {
		_ = denied.Body.Close()
	}
	if err != nil {
		s.logger.WarnContext(req.Context(), "selfsubjectaccessreview", "cluster", cluster, "error", err.Error())
	}
	if denied != nil || err != nil || len(set.names) == 0 {
		return s.nativeReview(req.Context(), cluster, set.user, resp, answer)
	}

	granted, ok := grantReview(answer)
	if !ok {
		return s.nativeReview(req.Context(), cluster, set.user, resp, answer)
	}

	resp.Body = io.NopCloser(bytes.NewReader(granted))
	resp.ContentLength = int64(len(granted))
	resp.TransferEncoding = nil
	resp.Header.Set("Content-Length", strconv.Itoa(len(granted)))
	s.logReview(req.Context(), cluster, outcomeGrant, resp.StatusCode, set.user)
	return resp, nil
}

// nativeReview returns the review answer of the upstream unchanged, and logs
// the native outcome.
func (s *Service) nativeReview(ctx context.Context, cluster, user string, resp *http.Response, answer []byte) (*http.Response, error) {
	resp.Body = io.NopCloser(bytes.NewReader(answer))
	s.logReview(ctx, cluster, outcomeNative, resp.StatusCode, user)
	return resp, nil
}

// filteredReview reports whether the review asks for a verb that the filter
// answers itself. A protobuf review also gets the JSON body that replaces it,
// because the filter answers the review in JSON.
func (s *Service) filteredReview(contentType string, body []byte) (match bool, replacement []byte) {
	if isProtobuf(contentType) {
		spec, err := decodeProtobufReview(body)
		if err != nil || !s.filteredVerb(spec) {
			return false, nil
		}
		replacement, err := reviewJSON(*spec.resource)
		if err != nil {
			return false, nil
		}
		return true, replacement
	}

	spec, err := jsonReviewSpec(body)
	if err != nil {
		return false, nil
	}
	return s.filteredVerb(spec), nil
}

// filteredVerb reports whether the review asks for the namespace list, or for
// a cluster-wide list that the fan-out answers.
func (s *Service) filteredVerb(spec reviewSpec) bool {
	return namespaceList(spec) || (s.fanoutEnabled && collectionList(spec))
}

// collectionList reports whether the review asks for a cluster-wide list or
// watch of another resource. The filter grants it on the allowed set alone: a
// check per namespace would cost one request per namespace, and the list and
// the watch apply the real permission themselves, so a grant that the
// permission does not carry gives an empty answer instead of an error.
func collectionList(spec reviewSpec) bool {
	if spec.nonResource || spec.resource == nil {
		return false
	}
	attributes := spec.resource
	if attributes.Verb != "list" && attributes.Verb != "watch" {
		return false
	}
	if attributes.Namespace != "" && attributes.Namespace != "*" {
		return false
	}
	if attributes.Resource == "" || attributes.Resource == "*" || attributes.Group == "*" {
		return false
	}
	return attributes.Name == "" && attributes.Subresource == ""
}

func jsonReviewSpec(body []byte) (reviewSpec, error) {
	var review struct {
		Spec struct {
			ResourceAttributes    *resourceAttributes `json:"resourceAttributes"`
			NonResourceAttributes json.RawMessage     `json:"nonResourceAttributes"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(body, &review); err != nil {
		return reviewSpec{}, err
	}
	raw := review.Spec.NonResourceAttributes
	return reviewSpec{
		resource:    review.Spec.ResourceAttributes,
		nonResource: len(raw) > 0 && string(raw) != "null",
	}, nil
}

// namespaceList reports whether the review asks for a list or a watch of the
// namespaces resource. It ignores the namespace attribute: kubectl sends the
// context namespace even for this cluster-scoped resource, and RBAC evaluates
// the review the same with or without it.
func namespaceList(spec reviewSpec) bool {
	if spec.nonResource || spec.resource == nil {
		return false
	}
	attributes := spec.resource
	if attributes.Verb != "list" && attributes.Verb != "watch" {
		return false
	}
	if attributes.Resource != "namespaces" {
		return false
	}
	return attributes.Group == "" && attributes.Name == "" && attributes.Subresource == ""
}

// reviewJSON returns the SelfSubjectAccessReview body of a JSON client.
func reviewJSON(attributes resourceAttributes) ([]byte, error) {
	type spec struct {
		ResourceAttributes resourceAttributes `json:"resourceAttributes"`
	}
	return json.Marshal(struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Spec       spec   `json:"spec"`
	}{
		APIVersion: "authorization.k8s.io/v1",
		Kind:       "SelfSubjectAccessReview",
		Spec:       spec{ResourceAttributes: attributes},
	})
}

// reviewDenied reports whether the review answer has a well-formed status
// with allowed set to false.
func reviewDenied(body []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil {
		return false
	}
	status, ok := object["status"].(map[string]any)
	if !ok {
		return false
	}
	allowed, ok := status["allowed"].(bool)
	return ok && !allowed
}

// grantReview sets status.allowed to true. It reports whether it changed the body.
func grantReview(body []byte) ([]byte, bool) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil {
		return nil, false
	}
	status, ok := object["status"].(map[string]any)
	if !ok {
		return nil, false
	}
	allowed, ok := status["allowed"].(bool)
	if !ok || allowed {
		return nil, false
	}
	status["allowed"] = true
	delete(status, "denied")
	status["reason"] = grantedReason

	granted, err := json.Marshal(object)
	if err != nil {
		return nil, false
	}
	return granted, true
}

func (s *Service) logReview(ctx context.Context, cluster, outcome string, status int, user string) {
	attrs := []any{"cluster", cluster, "outcome", outcome, "status", status}
	if user != "" {
		attrs = append(attrs, "user", user)
	}
	s.logger.InfoContext(ctx, "selfsubjectaccessreview", attrs...)
	setCallerAttribute(ctx, user)
}

// reviewError logs the failed request. The caller returns the error, and the
// error handler of the proxy writes the Status body.
func (s *Service) reviewError(ctx context.Context, cluster string, err error) error {
	level := slog.LevelError
	if errors.Is(err, context.Canceled) {
		level = slog.LevelDebug
	}
	s.logger.Log(ctx, level, "selfsubjectaccessreview",
		"cluster", cluster, "outcome", outcomeError, "error", err.Error())
	return err
}
