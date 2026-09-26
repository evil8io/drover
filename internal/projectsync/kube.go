package projectsync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
)

const (
	jsonType = "application/json"

	rbacAPIVersion = "rbac.authorization.k8s.io/v1"
	rbacGroup      = "rbac.authorization.k8s.io"
)

// objectMeta is the part of the metadata of a Kubernetes object that the
// service reads and writes.
type objectMeta struct {
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace,omitempty"`
	GenerateName      string            `json:"generateName,omitempty"`
	UID               string            `json:"uid,omitempty"`
	ResourceVersion   string            `json:"resourceVersion,omitempty"`
	DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
	OwnerReferences   []ownerReference  `json:"ownerReferences,omitempty"`
}

type ownerReference struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	UID        string `json:"uid"`
}

type roleRef struct {
	APIGroup string `json:"apiGroup"`
	Kind     string `json:"kind"`
	Name     string `json:"name"`
}

type subject struct {
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`
}

// binding is a RoleBinding or a ClusterRoleBinding.
type binding struct {
	APIVersion string     `json:"apiVersion,omitempty"`
	Kind       string     `json:"kind,omitempty"`
	Metadata   objectMeta `json:"metadata"`
	RoleRef    roleRef    `json:"roleRef"`
	Subjects   []subject  `json:"subjects"`
}

// object is a Kubernetes object of which the service reads the metadata only:
// a ServiceAccount, a Namespace, or a Project.
type object struct {
	APIVersion string     `json:"apiVersion,omitempty"`
	Kind       string     `json:"kind,omitempty"`
	Metadata   objectMeta `json:"metadata"`
	Spec       any        `json:"spec,omitempty"`
}

// maxKept bounds the subjects and the owner references that the service
// keeps of a listed binding. A tenant can edit a binding in its namespace, and
// the service only needs to see that a list has more than one entry.
const maxKept = 2

// pruneBinding keeps the fields that the service compares. The labels keep
// only the keys of the service.
func pruneBinding(item binding) binding {
	item.Metadata.Labels = pick(item.Metadata.Labels, []string{accountProjectKey, accountRoleKey})
	item.Metadata.Annotations = nil
	if len(item.Subjects) > maxKept {
		item.Subjects = item.Subjects[:maxKept]
	}
	if len(item.Metadata.OwnerReferences) > maxKept {
		item.Metadata.OwnerReferences = item.Metadata.OwnerReferences[:maxKept]
	}
	return item
}

// pruneObject keeps the name, the namespace, the uid, and the labels of the
// service.
func pruneObject(item object) object {
	return object{Metadata: objectMeta{
		Name:            item.Metadata.Name,
		Namespace:       item.Metadata.Namespace,
		UID:             item.Metadata.UID,
		ResourceVersion: item.Metadata.ResourceVersion,
		Labels:          pick(item.Metadata.Labels, []string{accountProjectKey, accountRoleKey}),
	}}
}

// listAll returns the objects at path that the label selector selects, over
// all pages. An expired continue token starts the list again from the first
// page, once.
func listAll[T any](ctx context.Context, s *Syncer, token, path, selector string, prune func(T) T) ([]T, error) {
	items, err := listPages(ctx, s, token, path, selector, prune)
	if errors.Is(err, errListExpired) {
		items, err = listPages(ctx, s, token, path, selector, prune)
	}
	return items, err
}

func listPages[T any](ctx context.Context, s *Syncer, token, path, selector string, prune func(T) T) ([]T, error) {
	var (
		items []T
		next  string
	)
	for page := 0; page < maxPages; page++ {
		query := url.Values{"limit": []string{strconv.Itoa(pageSize)}}
		if selector != "" {
			query.Set("labelSelector", selector)
		}
		if next != "" {
			query.Set("continue", next)
		}
		resp, err := s.getList(ctx, path, s.target(path, query), token)
		if err != nil {
			var status statusError
			if next != "" && errors.As(err, &status) && status.status == http.StatusGone {
				return nil, fmt.Errorf("%w: %w", errListExpired, err)
			}
			return nil, err
		}
		var meta listMeta
		err = decodePage(resp.Body, s.pageCap, "metadata", &meta, "items", collect(&items, prune))
		_ = resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("decode the list %s: %w", path, err)
		}
		if meta.Continue == "" {
			return items, nil
		}
		next = meta.Continue
	}
	return nil, fmt.Errorf("the list %s has more than %d pages", path, maxPages)
}

// getObject reads the object at path into out. It returns false and no error
// when the object does not exist.
func (s *Syncer) getObject(ctx context.Context, token, path string, out any) (bool, error) {
	status, answer, err := s.do(ctx, http.MethodGet, s.target(path, nil), token, "", nil)
	if err != nil {
		return false, err
	}
	switch status {
	case http.StatusOK:
		if err := json.Unmarshal(answer, out); err != nil {
			return false, fmt.Errorf("decode the answer of GET %s: %w", path, err)
		}
		return true, nil
	case http.StatusNotFound:
		return false, nil
	}
	return false, newStatusError(http.MethodGet, path, status, answer)
}

// send writes body to path with method, and decodes the answer into out when
// out is not nil. A PATCH body is a JSON merge patch. A status outside 2xx
// returns a statusError.
func (s *Syncer) send(ctx context.Context, token, method, path string, body, out any) error {
	var data []byte
	if body != nil {
		var err error
		if data, err = json.Marshal(body); err != nil {
			return fmt.Errorf("encode the body of %s %s: %w", method, path, err)
		}
	}
	contentType := jsonType
	if method == http.MethodPatch {
		contentType = mergePatchType
	}
	status, answer, err := s.do(ctx, method, s.target(path, nil), token, contentType, data)
	if err != nil {
		return err
	}
	if status < 200 || status > 299 {
		return newStatusError(method, path, status, answer)
	}
	if out != nil {
		if err := json.Unmarshal(answer, out); err != nil {
			return fmt.Errorf("decode the answer of %s %s: %w", method, path, err)
		}
	}
	return nil
}

// deleteObject deletes the object at path. A missing object is not an error.
func (s *Syncer) deleteObject(ctx context.Context, token, path string) error {
	err := s.send(ctx, token, http.MethodDelete, path, nil, nil)
	var status statusError
	if errors.As(err, &status) && status.status == http.StatusNotFound {
		return nil
	}
	return err
}

// hasStatus reports whether err is a statusError with one of the codes.
func hasStatus(err error, codes ...int) bool {
	var status statusError
	if !errors.As(err, &status) {
		return false
	}
	for _, code := range codes {
		if status.status == code {
			return true
		}
	}
	return false
}

func clusterPath(cluster string) string {
	return "/k8s/clusters/" + cluster
}

func namespacePath(cluster, name string) string {
	return namespacesPath(cluster) + "/" + name
}

func serviceAccountsPath(cluster, ns string) string {
	if ns == "" {
		return clusterPath(cluster) + "/api/v1/serviceaccounts"
	}
	return namespacePath(cluster, ns) + "/serviceaccounts"
}

func roleBindingsPath(cluster, ns string) string {
	if ns == "" {
		return clusterPath(cluster) + "/apis/" + rbacAPIVersion + "/rolebindings"
	}
	return clusterPath(cluster) + "/apis/" + rbacAPIVersion + "/namespaces/" + ns + "/rolebindings"
}

func clusterRoleBindingsPath(cluster string) string {
	return clusterPath(cluster) + "/apis/" + rbacAPIVersion + "/clusterrolebindings"
}
