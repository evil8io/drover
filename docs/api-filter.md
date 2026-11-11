# api-filter

Rancher gives a project member `get` on the namespaces of their projects, but not `list`. So `kubectl get ns <name>` works through a Rancher kubeconfig, while `kubectl get ns` returns a 403. Tools such as k9s, Headlamp, and Aptakube start with a namespace list, so they show an error or an empty cluster.

The api-filter is a small reverse proxy in front of Rancher that fixes this. A Gateway routes a handful of read paths to the filter and sends everything else straight to Rancher.

## What it does

### Namespace list

1. The filter forwards `GET /api/v1/namespaces` to Rancher with the caller's own credentials. If Rancher answers with anything other than a 403, the caller gets that answer unchanged. Admins and cluster owners are therefore not affected.
2. On a 403, the filter looks up which namespaces the caller may see, and repeats the list with its own service token, limited to those namespaces.

A watch (`kubectl get ns --watch`) works the same way. The filter drops events for namespaces the caller may not see, and it keeps an open watch up to date when the caller gains or loses a namespace.

The lookup result is cached per caller for `--cache-ttl` (15 seconds by default), so a change in project membership shows up after a short delay.

### Access reviews

Many clients first ask the API server "may I list namespaces?" and hide the namespace view if the answer is no. The filter answers yes for a caller with at least one namespace. `kubectl auth can-i list namespaces` therefore says yes, even though RBAC alone says no.

### Cluster-wide reads (optional)

With `--fanout`, a request such as `kubectl get pods -A` also works for a tenant. Rancher denies the cluster-wide request, so the filter sends one request per namespace of the caller and merges the answers into one list. A cluster-wide watch is merged in the same way.

Each of those requests uses the caller's own credentials. Kubernetes RBAC checks every one of them, so the caller never sees more than they could already read namespace by namespace.

### ServiceAccount callers

A ServiceAccount token of a downstream cluster also works, when Rancher accepts such tokens for that cluster (Rancher 2.9.0 or later, with an enabled `ClusterProxyConfig`). A ServiceAccount is not a Rancher user and has no projects, so the filter reads its namespaces from RBAC: the `resourceNames` of the rules that grant `get` on `namespaces`, in the ClusterRoleBindings of the ServiceAccount.

## What to expect

- **Empty instead of forbidden.** With the fan-out on, a cluster-wide list of a namespaced kind that the caller cannot read in any of their namespaces returns an empty list, not a 403. Clients show an empty view instead of an error. A cluster-scoped kind such as `nodes` keeps its 403.
- **Limits.** A cluster-wide list is refused with a 403 for a caller with more than 200 namespaces, and a cluster-wide watch for a caller with more than 50. The filter serves at most 1000 open watches in total and 100 per caller; above that, a new watch gets a 503 and the client retries. All four limits are flags.
- **Watches can restart.** When a caller's set of namespaces changes, the filter sometimes has to end a watch with a `410 Expired`. Clients handle this by listing again.
- **No bookmarks on a merged watch.** A cluster-wide watch through the fan-out does not send `BOOKMARK` events, apart from the one that ends the initial events of a watch-list (`sendInitialEvents=true`).
- **The filter is on the data path.** With the fan-out on, most reads of a tenant go through the filter. Tenants depend on its availability for those reads; writes, `exec`, logs, and port-forwards still go straight to Rancher.
- **The filter is privileged.** It uses a service token that can list every namespace. It only uses that token to list, watch, and read namespaces on behalf of a caller.

## Requirements

**Rancher**

- A Rancher service user with `get`, `list`, and `watch` on `namespaces` in every downstream cluster. A `cluster-owner` binding also works.
- An API token of that user in a mounted file (`--token-file`). Rancher tokens expire, so something has to renew it; that is what [rotate-token](rotate-token.md) is for.
- Network access from the filter to the Rancher Service inside the cluster (`--upstream`). The public Rancher hostname does not work, because the Gateway routes the filtered paths on that hostname back to the filter. For an `https` upstream with a private certificate, the filter needs the CA of Rancher (`--upstream-ca-file`).

**A Gateway API controller in front of Rancher**

The Rancher hostname must be served by a Gateway API implementation. An Ingress controller is not enough, because the routes below match on the HTTP method. The controller must support:

- `HTTPRoute` rules with a method match and with `Exact` and `PathPrefix` path matches, in the standard precedence (exact before prefix, longer prefix before shorter).
- Long-running, unbuffered responses and websocket upgrades. Watches, `exec`, and log streams stay open for minutes, so the routes need a long or unlimited request timeout.
- A backend in another namespace (`ReferenceGrant`), if the filter and Rancher run in different namespaces.

One set of routes is needed per downstream cluster:

| Match | Method | Path | Target |
| --- | --- | --- | --- |
| `Exact` | GET | `/k8s/clusters/<id>/api/v1/namespaces` | api-filter |
| `Exact` | POST | `/k8s/clusters/<id>/apis/authorization.k8s.io/v1/selfsubjectaccessreviews` | api-filter |
| `PathPrefix` | GET | `/k8s/clusters/<id>/api/v1` | api-filter (fan-out only) |
| `PathPrefix` | GET | `/k8s/clusters/<id>/apis` | api-filter (fan-out only) |
| `PathPrefix` | GET | `/k8s/clusters/<id>/api/v1/namespaces` | Rancher (fan-out only) |

The last route keeps namespaced reads, `exec`, `attach`, `portforward`, and log streams on Rancher.

**For ServiceAccount callers**

- Rancher 2.9.0 or later, with a `ClusterProxyConfig` that has `enabled: true` for the cluster.
- A ClusterRoleBinding for the ServiceAccount to a role that grants `get` on `namespaces` with the allowed namespaces in `resourceNames`. [project-sync](project-sync.md) creates these for the project ServiceAccounts.

The [drover chart](https://github.com/evil8io/charts/tree/main/charts/drover) takes care of the service user, the token and its rotation, the routes for every cluster, and the `ClusterProxyConfig`. It needs Kyverno for the last two.

## Running it

```sh
drover api-filter --upstream https://rancher.cattle-system --token-file /var/run/drover/token
```

`drover api-filter --help` lists every flag with its default. The filter serves `/healthz` and `/readyz`, writes JSON logs to stderr, and exports OpenTelemetry traces and `drover.filter.*` metrics when `--otlp-endpoint` is set. On SIGTERM it closes open watches cleanly, so clients reconnect without an error.

The exact request sequences, log fields, and metrics are in [.claude/docs/api-filter.md](../.claude/docs/api-filter.md).
