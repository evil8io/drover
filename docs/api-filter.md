# api-filter

Rancher gives a project member the `get` right on the namespaces of the member's projects. Rancher does not give the `list` right. Thus `kubectl get ns <name>` succeeds through a Rancher kubeconfig, but `kubectl get ns` returns a 403 error. Clients such as k9s, Headlamp, and Aptakube start with a namespace list. They then show an error or an empty cluster.

The api-filter is a reverse proxy in front of Rancher that solves this problem. A Gateway sends a small set of read paths to the api-filter. The Gateway sends all other paths directly to Rancher.

## What it does

### Namespace list

1. The api-filter sends `GET /api/v1/namespaces` to Rancher with the credentials of the caller. When Rancher does not answer with a 403 error, the caller gets the answer of Rancher without a change. The api-filter thus does not change the answers for an admin or a cluster owner.
2. After a 403 error, the api-filter finds the namespaces that the caller can see. It then sends the list again with its own service token, with a filter for those namespaces.

A watch (`kubectl get ns --watch`) follows the same steps. The api-filter drops each event of a namespace that the caller cannot see. When the caller gets or loses access to a namespace, the api-filter updates the open watch.

The api-filter keeps the namespaces of a caller in a cache for the time in `--cache-ttl`, 15 seconds by default. A change of project membership thus becomes visible after a short delay.

### Access reviews

Many clients first ask the API server if the user can list namespaces. When the answer is no, the client hides its namespace view. The api-filter answers yes for a caller with one namespace or more. `kubectl auth can-i list namespaces` thus returns yes, although RBAC returns no.

### Cluster-wide reads (optional)

With `--fanout`, a tenant can also run a request such as `kubectl get pods -A`. Rancher denies the cluster-wide request. The api-filter then sends one request for each namespace of the caller and merges the answers into one list. The api-filter merges a cluster-wide watch in the same way.

The api-filter sends each of those requests with the credentials of the caller. Kubernetes RBAC checks each request. The caller thus sees only the objects that the caller can already read in each namespace.

### ServiceAccount callers

A caller can also use a ServiceAccount token of a downstream cluster. A downstream cluster is a cluster that Rancher manages. Rancher must accept such tokens for that cluster. This needs Rancher 2.9.0 or later and a `ClusterProxyConfig` with `enabled: true`.

A ServiceAccount is not a Rancher user, so it is not a member of a project. The api-filter thus reads the namespaces of the ServiceAccount from the ClusterRoleBindings of the ServiceAccount. In the rules that grant `get` on `namespaces`, the names in `resourceNames` are the namespaces of the caller.

## What to expect

- **An empty list in place of a 403 error.** With `--fanout`, a caller can list a namespaced kind cluster-wide. When RBAC denies that kind in each namespace of the caller, the answer is an empty list, not a 403 error. A client then shows an empty view. For a cluster-scoped kind such as `nodes`, the caller still gets the 403 error.
- **Limits.** The api-filter refuses a cluster-wide list with a 403 error for a caller with more than 200 namespaces. It refuses a cluster-wide watch for a caller with more than 50 namespaces. It keeps a maximum of 1000 watches open in total, and a maximum of 100 for each caller. Above those limits, a new watch gets a 503 error. The client then tries again. You can change each of the four limits with a flag.
- **A watch can end early.** When the namespaces of a caller change, the api-filter ends a watch with a `410 Expired` event in some cases. The client then lists again.
- **No bookmarks on a merged watch.** The api-filter does not send `BOOKMARK` events on a cluster-wide watch. The one exception is the bookmark that ends the initial events of a watch-list (`sendInitialEvents=true`).
- **The api-filter is on the request path.** With `--fanout`, the Gateway sends most reads of a tenant through the api-filter. Those reads thus depend on the availability of the api-filter. Writes, `exec`, logs, and port-forwards still go directly to Rancher.
- **The api-filter is privileged.** Its service token can list all namespaces. The api-filter uses that token only to list, watch, and read namespaces for a caller.

## Requirements

Each item states if the [drover chart](https://github.com/evil8io/charts/tree/main/charts/drover) satisfies it.

**Rancher**

- A Rancher service user with `get`, `list`, and `watch` on `namespaces` in each downstream cluster. A `cluster-owner` binding also gives these rights. With the chart, Helm creates this user and its Rancher roles.
- An API token of that user in a mounted file (`--token-file`). A Rancher token expires, so it must be renewed. With the chart, a CronJob runs [rotate-token](rotate-token.md) for this token.
- A network path from the api-filter to the Rancher Service in the cluster (`--upstream`). With the chart, set the value `rancher.url` to that Service. Do not use the public Rancher hostname. On that hostname, the Gateway sends the filtered paths back to the api-filter.
- For an `https` upstream with a private certificate, the CA of Rancher (`--upstream-ca-file`). The chart does not mount a CA file. When the api-filter cannot verify the certificate, set the value `rancher.insecureSkipVerify`.

**A Gateway API controller in front of Rancher**

A Gateway API implementation must serve the Rancher hostname. The chart does not contain a Gateway API controller or a Gateway. Set the value `httpRoute.parentRefs` to your Gateway. An Ingress controller is not sufficient, because the routes below match on the HTTP method. The controller must support these features:

- `HTTPRoute` rules with a method match, and with `Exact` and `PathPrefix` path matches. The controller must apply the standard precedence: an exact match first, then the longest prefix.
- Responses that stay open for minutes without a buffer, and websocket upgrades. Watches, `exec`, and log streams stay open for minutes. The routes thus need a long request timeout, or no timeout. The routes of the chart have the `HTTPRoute` request timeout `0s`. When your controller ignores that field, configure the timeout in the controller.
- A backend in another namespace (`ReferenceGrant`), when the api-filter and Rancher are in different namespaces. Helm does not create the `ReferenceGrant`.

Each downstream cluster needs one set of routes. With the chart, Kyverno creates these routes for each cluster, so the chart needs Kyverno with its cleanup controller. Without the chart, create the routes yourself.

| Match | Method | Path | Target |
| --- | --- | --- | --- |
| `Exact` | GET | `/k8s/clusters/<id>/api/v1/namespaces` | api-filter |
| `Exact` | POST | `/k8s/clusters/<id>/apis/authorization.k8s.io/v1/selfsubjectaccessreviews` | api-filter |
| `PathPrefix` | GET | `/k8s/clusters/<id>/api/v1` | api-filter (only with `--fanout`) |
| `PathPrefix` | GET | `/k8s/clusters/<id>/apis` | api-filter (only with `--fanout`) |
| `PathPrefix` | GET | `/k8s/clusters/<id>/api/v1/namespaces` | Rancher (only with `--fanout`) |

The last route keeps the namespaced reads, `exec`, `attach`, `portforward`, and the log streams on Rancher.

**For ServiceAccount callers**

- Rancher 2.9.0 or later. The chart does not install Rancher.
- A `ClusterProxyConfig` with `enabled: true` for the cluster. With the chart, Kyverno creates it for each downstream cluster.
- A ClusterRoleBinding of the ServiceAccount to a role that grants `get` on `namespaces`, with the allowed namespaces in `resourceNames`. [project-sync](project-sync.md) creates these bindings for the project ServiceAccounts. For another ServiceAccount, create the binding yourself.

## How to run it

```sh
drover api-filter --upstream https://rancher.cattle-system --token-file /var/run/drover/token
```

`drover api-filter --help` shows each flag with its default. The api-filter answers on `/healthz` and `/readyz`. It writes JSON logs to stderr. With `--otlp-endpoint`, it also sends OpenTelemetry traces and the `drover.filter.*` metrics. On SIGTERM, the api-filter closes each open watch without an error, so the clients connect again.

The request sequences, the log fields, and the metrics are in [.claude/docs/api-filter.md](../.claude/docs/api-filter.md).
