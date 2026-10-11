package projectsync

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// dnsLabel is the form of a namespace name.
var dnsLabel = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

const maxNamespaceLength = 63

// BoundRole is a Role or a ClusterRole that the service binds to the
// ServiceAccounts of every project, next to the project roles.
type BoundRole struct {
	// Namespace is the namespace of a Role. Empty selects a ClusterRole.
	Namespace string
	// Name is the name of the Role or the ClusterRole.
	Name string
}

// String returns the name of a ClusterRole, or <namespace>/<name> of a Role.
func (r BoundRole) String() string {
	if r.Namespace == "" {
		return r.Name
	}
	return r.Namespace + "/" + r.Name
}

// kinds returns the binding kind and the role kind of r.
func (r BoundRole) kinds() (bindingKind, roleKind string) {
	if r.Namespace == "" {
		return "ClusterRoleBinding", "ClusterRole"
	}
	return "RoleBinding", "Role"
}

// checkBoundRoles validates roles: each name is a path segment, each
// namespace is a DNS label, and no role is listed twice.
func checkBoundRoles(roles []BoundRole) error {
	seen := make(map[BoundRole]bool, len(roles))
	for _, r := range roles {
		if r.Name == "" || r.Name == "." || r.Name == ".." || strings.ContainsAny(r.Name, "/%") {
			return fmt.Errorf("the bound role name %q is not a path segment", r.Name)
		}
		if r.Namespace != "" && (len(r.Namespace) > maxNamespaceLength || !dnsLabel.MatchString(r.Namespace)) {
			return fmt.Errorf("the namespace %q of the bound role %s is not a DNS label", r.Namespace, r.Name)
		}
		if seen[r] {
			return fmt.Errorf("the bound role %s is listed twice", r)
		}
		seen[r] = true
	}
	return nil
}

// boundBindingPrefix starts the name of every binding of a bound role for
// role in project: drover-<project>-<role>-clusterrole- for a ClusterRole,
// and drover-<project>-<role>-role- for a Role. The kind in the name lets the
// sweep find a binding whose role left the config.
func boundBindingPrefix(project string, role accountRole, namespaced bool) string {
	kind := "clusterrole"
	if namespaced {
		kind = "role"
	}
	return accountPrefix + project + "-" + role.name + "-" + kind + "-"
}

func boundBindingName(project string, role accountRole, bound BoundRole) string {
	return boundBindingPrefix(project, role, bound.Namespace != "") + bound.Name
}

// boundBinding returns the binding of bound for the ServiceAccount of role in
// the account namespace of project, which has the uid.
func boundBinding(project, uid string, role accountRole, bound BoundRole) binding {
	bindingKind, roleKind := bound.kinds()
	b := accountBinding(bindingKind, boundBindingName(project, role, bound), bound.Namespace, bound.Name, project, uid, role)
	b.RoleRef.Kind = roleKind
	return b
}

// isBoundBinding reports whether item is a binding of a bound role for
// project: its name has the prefix of a project role, with the kind of its
// scope.
func isBoundBinding(item binding, project string) bool {
	namespaced := item.Metadata.Namespace != ""
	return slices.ContainsFunc(accountRoles, func(role accountRole) bool {
		return strings.HasPrefix(item.Metadata.Name, boundBindingPrefix(project, role, namespaced))
	})
}

func bindingKey(namespace, name string) string {
	return namespace + "/" + name
}

func findBindingIn(items []binding, namespace, name string) *binding {
	for i := range items {
		if items[i].Metadata.Namespace == namespace && items[i].Metadata.Name == name {
			return &items[i]
		}
	}
	return nil
}

// ensureBoundRoles reconciles the bindings of the bound roles of project name,
// and deletes a binding of a bound role that left the config. clusterBindings
// and roleBindings are the listed bindings of the project.
func (s *Syncer) ensureBoundRoles(ctx context.Context, token, cluster, name, uid string, clusterBindings, roleBindings []binding, run *counters) {
	wanted := make(map[string]bool, len(accountRoles)*len(s.boundRoles))
	for _, role := range accountRoles {
		for _, bound := range s.boundRoles {
			want := boundBinding(name, uid, role, bound)
			collection, existing := clusterRoleBindingsPath(cluster), clusterBindings
			if bound.Namespace != "" {
				collection, existing = roleBindingsPath(cluster, bound.Namespace), roleBindings
			}
			wanted[bindingKey(want.Metadata.Namespace, want.Metadata.Name)] = true
			have := findBindingIn(existing, want.Metadata.Namespace, want.Metadata.Name)
			s.reconcileBinding(ctx, token, cluster, collection, want, have, name, run)
		}
	}
	for _, item := range clusterBindings {
		if isBoundBinding(item, name) && !wanted[bindingKey("", item.Metadata.Name)] {
			s.dropBinding(ctx, token, cluster, clusterRoleBindingsPath(cluster), item, name, run)
		}
	}
	for _, item := range roleBindings {
		if isBoundBinding(item, name) && !wanted[bindingKey(item.Metadata.Namespace, item.Metadata.Name)] {
			s.dropBinding(ctx, token, cluster, roleBindingsPath(cluster, item.Metadata.Namespace), item, name, run)
		}
	}
}

// warnMissingBoundNamespaces warns once per run for each bound Role whose
// namespace is not in byName, the namespaces of cluster, and counts it as an
// error of run. The create of such a RoleBinding answers 404, which the
// account writes skip at the debug level, because a tenant namespace can be
// gone. The namespace of a bound Role is a platform namespace, so a 404 there
// is a wrong flag value.
func (s *Syncer) warnMissingBoundNamespaces(ctx context.Context, cluster string, byName map[string]namespace, run *counters) {
	for _, bound := range s.boundRoles {
		if bound.Namespace == "" {
			continue
		}
		if _, ok := byName[bound.Namespace]; ok {
			continue
		}
		if run != nil {
			run.errors++
		}
		s.logger.WarnContext(ctx, "the namespace of a bound role does not exist, so its role bindings are not created",
			"cluster", cluster, "namespace", bound.Namespace, "role", bound.Name)
	}
}

// hasBoundRoleBinding reports whether a bound role is a Role, so that the
// lister needs the role bindings of a project.
func (s *Syncer) hasBoundRoleBinding() bool {
	return slices.ContainsFunc(s.boundRoles, func(r BoundRole) bool { return r.Namespace != "" })
}
