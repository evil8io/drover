# drover

drover is a set of tenancy extensions for Rancher. It is one binary, with one subcommand per component.

## Subcommands

| Subcommand | Meaning |
| --- | --- |
| `namespace-filter` | A reverse proxy that answers the namespace list of a Rancher project member. |
| `rotate-token` | A command that renews the API token of the Rancher service user in a Secret. |
| `project-sync` | A loop that copies labels and annotations of a Rancher project to the namespaces of that project. |

## namespace-filter

A filter in front of Rancher that lets a tenant list only the namespaces of its projects, with kubectl, k9s, and other Kubernetes clients.

Rancher grants a project member `get` on the namespaces of its projects, and no `list`. A `kubectl get ns` request through a Rancher kubeconfig gets a 403 error, while `kubectl get ns <name>` works. This service is an HTTP reverse proxy in front of Rancher. A Gateway or an Ingress routes two paths to the service, and every other path goes to Rancher directly.

### How it works

The service handles two request patterns from a Rancher kubeconfig. It passes every other request to Rancher unchanged.

**List namespaces**

1. The service sends the request to Rancher with the caller's own credentials.
2. A status other than 403 goes back to the client unchanged.
3. On a 403 error, the service requests the caller's allowed namespaces from Steve, the Rancher API server in the cluster agent.
4. A plain list gets a new request with a service token, no cookie, and a label selector. The selector matches `field.cattle.io/projectId` on the caller's projects, when every allowed namespace has that label. It matches the allowed namespace names in every other case. That case includes the selector that matches no namespace, when the caller may see none.
5. A watch (`?watch=true`) gets a new request with a service token, no cookie, and the caller's own query, with no name filter. The request keeps every Accept entry of the caller whose media type is `application/json`, for example a table request from kubectl, and drops every other entry, for example protobuf or CBOR. It sets `application/json` when no entry remains.
6. The service sends the new request to Rancher and streams the response to the client. For a watch, an event passes only when every namespace in it is in the allowed set, or its `field.cattle.io/projectId` label matches a caller project. A server-side table event has one row per namespace, and the filter checks the name and the labels of each row.

A watch request streams over chunked HTTP. A websocket upgrade streams the same way, after the protocol switch. A namespace that Rancher grants after the watch starts becomes visible within one cache TTL, because the event filter reads the allowed set through the same cache as a plain list.

**Review access**

The service reads the body of a `selfsubjectaccessreviews` request. When the review asks about a list or a watch on the namespaces resource, the service sends the request with the caller's own credentials. A review may name a namespace, because kubectl sends the namespace of the kubeconfig context even for this cluster-scoped resource. The service ignores that attribute. A denied response gets `allowed: true` only when the caller has at least one allowed namespace. Every other review passes through unchanged. The service reads a JSON review or a Kubernetes protobuf review, and it answers an intercepted review in JSON.

### Requirements

1. A Rancher service user with a `cluster-owner` binding on every cluster whose tenants use the filter.
2. An API token of that service user. The token has no scope.
3. The token in a Secret, mounted into the service.
4. Two `Exact` path matches per cluster id, on the Rancher hostname, that route to the service.

### Configuration

| Flag | Default | Meaning |
| --- | --- | --- |
| `--listen` | `:8080` | Address the service listens on. |
| `--upstream` | (required) | URL of Rancher. Use `http://` or `https://`. The path must be empty or `/`. |
| `--upstream-ca-file` | | PEM bundle that verifies an `https` upstream. |
| `--token-file` | (required) | File with the API token of the Rancher service user. |
| `--cache-ttl` | `15s` | Lifetime of a cached allowed set. |
| `--log-level` | `info` | One of `debug`, `info`, `warn`, or `error`. |
| `--shutdown-grace` | `20s` | Grace period for the shutdown after SIGTERM or SIGINT. |
| `--otlp-endpoint` | `$OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP gRPC endpoint, `host:port` or a URL. Empty turns telemetry off. |
| `--otlp-traces` | `true` | Send traces to the OTLP endpoint. |
| `--otlp-metrics` | `true` | Send metrics to the OTLP endpoint. |
| `--service-name` | `$OTEL_SERVICE_NAME`, or `drover` | `service.name` resource attribute. |

The upstream is the Rancher Service inside the cluster, for example `http://rancher.cattle-system.svc`. The public hostname is not a valid upstream, because the route sends the two filtered paths back to the service.

The service starts with no token file and returns a 502 Status on a filtered request until the file gets a token.

`GET /healthz` returns status 200 with body `ok`, always. `GET /readyz` returns status 200 with body `ok` in normal operation, and status 503 with body `draining` after the shutdown starts.

A Helm chart for drover is published separately.

### Behaviour differences

| Difference | Detail |
| --- | --- |
| Cache delay | A role change becomes visible after the cache TTL, on top of Rancher's own delay. |
| Field selector | A field selector on a name outside the allowed set returns an empty list. A `get` on that name returns Forbidden. |
| Namespace cap | A caller with more than 20,000 allowed namespace names gets an error, not a list. |
| Self-check | `kubectl auth can-i list namespaces` returns yes when the caller has at least one allowed namespace, while RBAC returns no. |
| Trust level | The service is a privileged component. It uses the cluster-owner token for the filtered namespace list and for the watch stream. |

### Logging

The service writes JSON logs to stderr, one line per intercepted request.

A namespace list log line has these fields: `cluster`, `outcome` (`native`, `filtered`, `passthrough`, `denied`, or `error`), `status`, `count` (only on `filtered`), `watch`, and `duration_ms`.

A review log line has these fields: `cluster`, `outcome` (`passthrough`, `native`, or `granted`), and `status`.

The service never logs a token, a cookie, a header value, or a request body. It logs a namespace name at the `debug` level only.

The service writes a `warn` line when the privileged list request gets a 403 error. The cause is a `cluster-owner` binding that the service user does not have.

A log line has `trace_id` and `span_id` when the request has a span.

### Telemetry

The service continues an incoming `traceparent` on every request. It starts a span when the header is absent. A request span has a child span for the Steve call, the project list, the privileged list, and the privileged watch. With `--otlp-endpoint` set, the service also exports these metrics:

| Metric | Kind | Unit | Attributes |
| --- | --- | --- | --- |
| `drover.filter.requests` | Counter | `1` | `outcome`, `cluster`, `watch` |
| `drover.filter.request.duration` | Histogram | `s` | `outcome`, `cluster`, `watch` |
| `drover.filter.watches.open` | Up-down counter | `1` | |
| `drover.filter.events.dropped` | Counter | `1` | `cluster` |

### Shutdown

On SIGTERM or SIGINT, the service drains before it stops:

1. It marks itself not ready. `/readyz` answers 503 from that point on.
2. It ends every open watch stream with a clean end of the stream. A client re-lists and re-watches, with no error.
3. It stops the HTTP server, within the `--shutdown-grace` period.

The exit code is non-zero only when the server does not stop in time.

## rotate-token

A command that renews the API token of the Rancher service user in a Kubernetes Secret.

The command runs once: it reads the token from the Secret, and it asks Rancher for the expiry. A token that lasts longer than `--renew-before` is valid, and the run ends with no change. For a new token the command logs in as the service user, and it derives a token from that session. The login is a first step only, because Rancher ignores the TTL of a login token. Rancher also reduces a TTL above its own maximum without an error, so a different TTL in the answer gives a warning. Last, the command patches the Secret, deletes the tokens with the same description except the newest `--keep`, and ends the session with a logout.

A CronJob is the normal caller, because most runs find a valid token and exit 0.

### Configuration

| Flag | Default | Meaning |
| --- | --- | --- |
| `--rancher-url` | (required) | URL of Rancher. Use `http://` or `https://`. The path must be empty or `/`. |
| `--rancher-ca-file` | | PEM bundle that verifies an `https` Rancher URL. |
| `--credentials-dir` | (required) | Directory with the files `username` and `password` of the service user. |
| `--token-secret` | (required) | `namespace/name` of the Secret with the API token. |
| `--token-key` | `token` | Key of the token inside the Secret. |
| `--ttl` | `48h` | Lifetime of a new token. Rancher reduces a value above `auth-token-max-ttl-minutes`. |
| `--renew-before` | `24h` | Remaining lifetime that starts a rotation. The value must be shorter than `--ttl`. |
| `--keep` | `2` | Number of tokens with the description to keep. The new token counts. |
| `--description` | `drover rotate-token` | Description of the tokens of this command. It also selects the tokens to delete. |
| `--kube-url` | (in-cluster) | Kubernetes API URL. The default comes from `KUBERNETES_SERVICE_HOST` and `KUBERNETES_SERVICE_PORT`. |
| `--kube-service-account-dir` | `/var/run/secrets/kubernetes.io/serviceaccount` | Directory with the ServiceAccount token and `ca.crt`. |
| `--log-level` | `info` | One of `debug`, `info`, `warn`, or `error`. |
| `--otlp-endpoint` | `$OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP gRPC endpoint, `host:port` or a URL. Empty turns telemetry off. |
| `--otlp-traces` | `true` | Send traces to the OTLP endpoint. |
| `--otlp-metrics` | `true` | Send metrics to the OTLP endpoint. |
| `--service-name` | `$OTEL_SERVICE_NAME`, or `drover` | `service.name` resource attribute. |

The exit code is 0 after a valid token and after a rotation, 1 after a failure, and 2 after a flag error.

The command never deletes a token with another description, so a kubeconfig token of the service user stays.

### Permissions

The pod needs `get` and `patch` on the one token Secret:

```yaml
rules:
  - apiGroups: [""]
    resources: [secrets]
    resourceNames: [drover-token]
    verbs: [get, patch]
```

The Secret must exist before the first run. The command reads the ServiceAccount token for every request, because the kubelet replaces the file.

### Logging

The command writes JSON logs to stderr, one line per step. Each line has a `step` field and an `outcome` field. The command never logs a token, a token key, or the password.

### Telemetry

With `--otlp-endpoint` set, one run produces a span named `rotate`, with a child span for each step: `secret_get`, `token_check`, `login`, `token_create`, `secret_patch`, `token_prune`, and `logout`. The command flushes traces and metrics before it exits, within a 5 s grace period. It exports these metrics:

| Metric | Kind | Unit | Attributes |
| --- | --- | --- | --- |
| `drover.rotate.steps` | Counter | `1` | `step`, `outcome` |
| `drover.rotate.duration` | Histogram | `s` | |

## project-sync

This service copies labels and annotations of a Rancher project to every namespace of that project, because Rancher does not copy them. The `--labels` and `--annotations` flags are the allow list of keys. The value of the project wins, and the service overwrites a different value on the namespace. A key that the project does not have stays untouched, so the service never removes a key from a namespace. The service polls Rancher at every `--interval`, and one replica is enough, because every run is a full reconcile. A namespace list that returns status 403 means that the service user has no `cluster-owner` binding on that cluster.

The service writes a warning with the cluster id and continues with the next cluster.

### Requirements

1. A Rancher service user with a `cluster-owner` binding on every cluster whose namespaces the service syncs.
2. An API token of that service user, in a Secret that the service mounts.

### Configuration

| Flag | Default | Meaning |
| --- | --- | --- |
| `--rancher-url` | (required) | URL of Rancher. Use `http://` or `https://`. The path must be empty or `/`. |
| `--rancher-ca-file` | | PEM bundle that verifies an `https` URL. |
| `--token-file` | (required) | File with the API token of the Rancher service user. |
| `--labels` | | Comma-separated label keys of a project to copy. |
| `--annotations` | | Comma-separated annotation keys of a project to copy. |
| `--interval` | `60s` | Time between two runs. |
| `--listen` | `:8080` | Address the service listens on. |
| `--log-level` | `info` | One of `debug`, `info`, `warn`, or `error`. |
| `--otlp-endpoint` | `$OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP gRPC endpoint, `host:port` or a URL. Empty turns telemetry off. |
| `--otlp-traces` | `true` | Send traces to the OTLP endpoint. |
| `--otlp-metrics` | `true` | Send metrics to the OTLP endpoint. |
| `--service-name` | `$OTEL_SERVICE_NAME`, or `drover` | `service.name` resource attribute. |

The flags need at least one label key or one annotation key. A key under `field.cattle.io/`, `cattle.io/`, `kubernetes.io/`, or `k8s.io/` is not valid, because Rancher and Kubernetes own those prefixes.

The service starts with no token file. It skips the run until the file has a token, and it writes one warning per state change of the file.

`GET /healthz` returns status 200 with body `ok`.

### Design

The service polls `GET /v3/projects` at every interval, and it opens no watch. One call per interval returns every project that the service user sees, because Rancher filters that list by RBAC. The service needs no cluster list of its own, and no watch connection per cluster. The service never removes a key from a namespace, because a tenant sets its own keys there, and the project does not define them. A patch with only the configured keys keeps those tenant keys. The standard library is enough for the work: the service needs an HTTP client, a JSON decoder, and a ticker.

### Telemetry

With `--otlp-endpoint` set, one reconcile run produces a span named `reconcile`. The service exports these metrics:

| Metric | Kind | Unit | Attributes |
| --- | --- | --- | --- |
| `drover.sync.reconciles` | Counter | `1` | `outcome` |
| `drover.sync.namespaces.patched` | Counter | `1` | |
| `drover.sync.errors` | Counter | `1` | |
| `drover.sync.duration` | Histogram | `s` | |

## Development

Run these checks before every commit:

1. `go test -race ./...`
2. `go vet ./...`
3. `gofmt -l .` (it must print nothing)

Build the binary with `go build ./cmd/drover`.

Build the image with `docker build` or `podman build`. Pass the version with the `VERSION` build argument:

```
podman build --build-arg VERSION=0.3.0 -t drover:0.3.0 .
```

## Releases

PR titles follow Conventional Commits. The project squash-merges every pull request. release-please reads the PR titles and opens a release pull request. A merge of that pull request creates a tag, a GitHub release, and the image tags for the new version.

## License

[Apache License 2.0](LICENSE)
