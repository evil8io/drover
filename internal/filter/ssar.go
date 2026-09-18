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
	maxReviewBody   = 1 << 20
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

func (s *service) roundTripReview(req *http.Request, cluster string) (*http.Response, error) {
	if hasImpersonation(req.Header) {
		resp, err := s.base.RoundTrip(req)
		if err != nil {
			return nil, s.reviewError(req.Context(), cluster, err)
		}
		s.logReview(req.Context(), cluster, outcomePassthrough, resp.StatusCode)
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
		s.logReview(req.Context(), cluster, outcomeError, http.StatusRequestEntityTooLarge)
		message := serviceName + ": the request body is larger than " + strconv.Itoa(maxReviewBody) + " bytes"
		return statusResponse(req, http.StatusRequestEntityTooLarge, reasonTooLarge, message), nil
	}

	out := req.Clone(req.Context())
	setBody(out, body)

	match, replacement := namespaceListReview(req.Header.Get("Content-Type"), body)
	if !match {
		resp, err := s.base.RoundTrip(out)
		if err != nil {
			return nil, s.reviewError(req.Context(), cluster, err)
		}
		s.logReview(req.Context(), cluster, outcomePassthrough, resp.StatusCode)
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
		s.logReview(req.Context(), cluster, outcomeNative, resp.StatusCode)
		return resp, nil
	}

	answer, tooLarge, err := readLimited(resp.Body, maxReviewBody)
	if err != nil {
		_ = resp.Body.Close()
		return nil, s.reviewError(req.Context(), cluster, err)
	}
	if tooLarge {
		resp.Body = joinedBody{Reader: io.MultiReader(bytes.NewReader(answer), resp.Body), Closer: resp.Body}
		s.logReview(req.Context(), cluster, outcomeNative, resp.StatusCode)
		return resp, nil
	}
	_ = resp.Body.Close()

	granted, ok := grantReview(answer)
	if !ok {
		resp.Body = io.NopCloser(bytes.NewReader(answer))
		s.logReview(req.Context(), cluster, outcomeNative, resp.StatusCode)
		return resp, nil
	}

	resp.Body = io.NopCloser(bytes.NewReader(granted))
	resp.ContentLength = int64(len(granted))
	resp.TransferEncoding = nil
	resp.Header.Set("Content-Length", strconv.Itoa(len(granted)))
	s.logReview(req.Context(), cluster, outcomeGrant, resp.StatusCode)
	return resp, nil
}

// namespaceListReview reports whether the review asks for the list or the watch
// of the namespaces at cluster scope. A protobuf review also gets the JSON body
// that replaces it, because the filter answers the review in JSON.
func namespaceListReview(contentType string, body []byte) (match bool, replacement []byte) {
	if isProtobuf(contentType) {
		spec, err := decodeProtobufReview(body)
		if err != nil || !clusterNamespaceList(spec) {
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
	return clusterNamespaceList(spec), nil
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

func clusterNamespaceList(spec reviewSpec) bool {
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
	return attributes.Group == "" && attributes.Namespace == "" &&
		attributes.Name == "" && attributes.Subresource == ""
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

func (s *service) logReview(ctx context.Context, cluster, outcome string, status int) {
	s.logger.InfoContext(ctx, "selfsubjectaccessreview", "cluster", cluster, "outcome", outcome, "status", status)
}

// reviewError logs the failed request. The caller returns the error, and the
// error handler of the proxy writes the Status body.
func (s *service) reviewError(ctx context.Context, cluster string, err error) error {
	level := slog.LevelError
	if errors.Is(err, context.Canceled) {
		level = slog.LevelDebug
	}
	s.logger.Log(ctx, level, "selfsubjectaccessreview",
		"cluster", cluster, "outcome", outcomeError, "error", err.Error())
	return err
}
