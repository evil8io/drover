package filter

import (
	"net/http"
	"strings"
)

// spanName returns the name of the span of a request: the method and the
// path template.
func spanName(r *http.Request) string {
	return r.Method + " " + pathTemplate(r.URL.Path)
}

// pathTemplate replaces the variable segments of a Kubernetes path with a
// placeholder, so the span name of a request has a bounded value set. The
// cluster id, a namespace name and an object name each vary per tenant and
// per object. The api group, the version and the resource name stay, because
// they come from the api surface of the cluster, which is bounded, and they
// say what the request asks for.
func pathTemplate(path string) string {
	parts := strings.Split(strings.TrimSuffix(path, "/"), "/")

	// parts[0] is empty, because a request path starts with a slash.
	next := 1
	if len(parts) > 3 && parts[1] == "k8s" && parts[2] == "clusters" {
		parts[3] = "{cluster}"
		next = 4
	}

	switch {
	case next+1 < len(parts) && parts[next] == "api":
		next += 2
	case next < len(parts) && parts[next] == "apis":
		next += 3
	default:
		return strings.Join(parts, "/")
	}

	if next+1 < len(parts) && parts[next] == "namespaces" {
		parts[next+1] = "{namespace}"
		next += 2
	}
	// parts[next] names the resource, and the segment after it names one
	// object of that resource.
	if next+1 < len(parts) {
		parts[next+1] = "{name}"
	}
	return strings.Join(parts, "/")
}
