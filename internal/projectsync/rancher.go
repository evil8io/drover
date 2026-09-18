package projectsync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

const (
	// projectLabel is the label of Rancher on a namespace of a project. Its value
	// is the project name, the part of the project id after the colon.
	projectLabel = "field.cattle.io/projectId"

	projectsPath   = "/v3/projects"
	mergePatchType = "application/merge-patch+json"
	maxPages       = 100
	maxBody        = 32 << 20
)

// project is the part of a Rancher project that the service reads.
type project struct {
	ID          string            `json:"id"`
	ClusterID   string            `json:"clusterId"`
	Name        string            `json:"name"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
}

type projectList struct {
	Data       []project `json:"data"`
	Pagination struct {
		Next string `json:"next"`
	} `json:"pagination"`
}

// namespace is the part of a Kubernetes namespace that the service reads.
type namespace struct {
	Metadata struct {
		Name        string            `json:"name"`
		Labels      map[string]string `json:"labels"`
		Annotations map[string]string `json:"annotations"`
	} `json:"metadata"`
}

type namespaceList struct {
	Items []namespace `json:"items"`
}

// statusError is an answer of Rancher with a status that the service does not expect.
type statusError struct {
	method string
	path   string
	status int
}

func (e statusError) Error() string {
	return fmt.Sprintf("%s %s returned status %d", e.method, e.path, e.status)
}

// projects returns every project that the service user sees, over all pages.
func (s *Syncer) projects(ctx context.Context, token string) ([]project, error) {
	target := s.target(projectsPath, nil)

	var projects []project
	for page := 0; page < maxPages; page++ {
		status, body, err := s.do(ctx, http.MethodGet, target, token, "", nil)
		if err != nil {
			return nil, err
		}
		if status != http.StatusOK {
			return nil, statusError{method: http.MethodGet, path: projectsPath, status: status}
		}

		var list projectList
		if err := json.Unmarshal(body, &list); err != nil {
			return nil, fmt.Errorf("decode the project list: %w", err)
		}
		projects = append(projects, list.Data...)

		if list.Pagination.Next == "" {
			return projects, nil
		}
		target, err = s.nextTarget(list.Pagination.Next)
		if err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("the project list has more than %d pages", maxPages)
}

// namespaces returns the namespaces of a cluster that have the project label.
func (s *Syncer) namespaces(ctx context.Context, token, cluster string) ([]namespace, error) {
	path := namespacesPath(cluster)
	query := url.Values{"labelSelector": []string{projectLabel}}

	status, body, err := s.do(ctx, http.MethodGet, s.target(path, query), token, "", nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, statusError{method: http.MethodGet, path: path, status: status}
	}

	var list namespaceList
	if err := json.Unmarshal(body, &list); err != nil {
		return nil, fmt.Errorf("decode the namespace list of cluster %s: %w", cluster, err)
	}
	return list.Items, nil
}

// patchNamespace sends the patch of one namespace as a JSON merge patch.
func (s *Syncer) patchNamespace(ctx context.Context, token, cluster, name string, change patch) error {
	body, err := change.body()
	if err != nil {
		return fmt.Errorf("encode the patch of namespace %s: %w", name, err)
	}

	path := namespacesPath(cluster) + "/" + name
	status, _, err := s.do(ctx, http.MethodPatch, s.target(path, nil), token, mergePatchType, body)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return statusError{method: http.MethodPatch, path: path, status: status}
	}
	return nil
}

func namespacesPath(cluster string) string {
	return "/k8s/clusters/" + cluster + "/api/v1/namespaces"
}

func (s *Syncer) target(path string, query url.Values) string {
	target := *s.rancher
	target.Path = path
	if query != nil {
		target.RawQuery = query.Encode()
	}
	return target.String()
}

// nextTarget returns the next page of Rancher on the configured host. Rancher
// builds the link from its own server URL, which is not always reachable here.
func (s *Syncer) nextTarget(next string) (string, error) {
	target, err := url.Parse(next)
	if err != nil {
		return "", fmt.Errorf("parse the next page link: %w", err)
	}
	target.Scheme = s.rancher.Scheme
	target.Host = s.rancher.Host
	return target.String(), nil
}

func (s *Syncer) do(ctx context.Context, method, target, token, contentType string, body []byte) (int, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", s.userAgent)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	answer, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return resp.StatusCode, nil, fmt.Errorf("read the answer of %s %s: %w", method, req.URL.Path, err)
	}
	return resp.StatusCode, answer, nil
}
