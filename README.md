# rancher-namespace-filter

A filter in front of Rancher that lets a tenant list only the namespaces of its projects, with kubectl, k9s, and other Kubernetes clients.

Rancher grants a project member `get` on the namespaces of its projects, and no `list`. A `kubectl get ns` through a Rancher kubeconfig is therefore Forbidden, and only the Rancher UI shows the namespaces. This filter intercepts the namespace list on the Rancher hostname, computes the namespaces of the caller, and forwards the request to the downstream cluster with a name selector. The API server then serves the list, the watch, the selectors, the pagination, and the Table output natively. Every other request goes to Rancher unchanged.

## Status

Design. Nothing is implemented yet.
