# rancher-namespace-filter

A filter in front of Rancher that lets a tenant list only the namespaces of its projects, with kubectl, k9s, and other Kubernetes clients.

Rancher grants a project member `get` on the namespaces of its projects, and no `list`. A `kubectl get ns` request through a Rancher kubeconfig gets a 403 error, while `kubectl get ns <name>` works. This service is an HTTP reverse proxy in front of Rancher. A Gateway or an Ingress routes two paths to the service, and every other path goes to Rancher directly.

## How it works

The service handles two request patterns from a Rancher kubeconfig. It passes every other request to Rancher unchanged.

**List namespaces**

1. The service sends the request to Rancher with the caller's own credentials.
2. A status other than 403 goes back to the client unchanged.
3. On a 403 error, the service requests the caller's allowed namespaces from Steve, the Rancher API server in the cluster agent.
4. The service builds a new request with a service token, no cookie, and a label selector that matches only the allowed namespace names.
5. The service sends the new request to Rancher and streams the response to the client.

A watch request (`?watch=true`) streams over chunked HTTP. A websocket upgrade streams the same way, after the protocol switch.

**Review access**

The service reads the body of a `selfsubjectaccessreviews` request. When the review asks about a list or a watch on namespaces at cluster scope, the service sends the request with the caller's own credentials. A denied response then gets `allowed: true`. Every other review passes through unchanged.

## Requirements

1. A Rancher service user with a `cluster-owner` binding on every cluster whose tenants use the filter.
2. An API token of that service user. The token has no scope.
3. The token in a Secret, mounted into the service.
4. Two `Exact` path matches per cluster id, on the Rancher hostname, that route to the service.

An `Exact` match takes precedence over the `PathPrefix /` match of the Rancher route. This is a Gateway API `HTTPRoute` example for one cluster id:

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: rancher-namespace-filter
spec:
  parentRefs:
    - name: rancher
  hostnames:
    - rancher.example.com
  rules:
    - matches:
        - path:
            type: Exact
            value: /k8s/clusters/c-m-abcde12345/api/v1/namespaces
      backendRefs:
        - name: rancher-namespace-filter
          port: 8080
    - matches:
        - path:
            type: Exact
            value: /k8s/clusters/c-m-abcde12345/apis/authorization.k8s.io/v1/selfsubjectaccessreviews
      backendRefs:
        - name: rancher-namespace-filter
          port: 8080
```

## Configuration

| Flag | Default | Meaning |
| --- | --- | --- |
| `--listen` | `:8080` | Address the service listens on. |
| `--upstream` | (required) | URL of Rancher. Use `http://` or `https://`. The path must be empty or `/`. |
| `--upstream-ca-file` | | PEM bundle that verifies an `https` upstream. |
| `--token-file` | (required) | File with the API token of the Rancher service user. |
| `--cache-ttl` | `15s` | Lifetime of a cached allowed set. |
| `--log-level` | `info` | One of `debug`, `info`, `warn`, or `error`. |
| `--version` | | Print the version and exit. |

The upstream is the Rancher Service inside the cluster, for example `http://rancher.cattle-system.svc`. The public hostname is not a valid upstream, because the route sends the two filtered paths back to the service.

`GET /healthz` returns status 200 with body `ok`.

## Deployment

The image has these tags: `<version>`, `<major>.<minor>`, `latest`, and `main`.

Mount the token Secret so the file lands at `/var/run/secrets/rancher/token`.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rancher-namespace-filter
spec:
  replicas: 2
  selector:
    matchLabels:
      app: rancher-namespace-filter
  template:
    metadata:
      labels:
        app: rancher-namespace-filter
    spec:
      containers:
        - name: rancher-namespace-filter
          image: ghcr.io/evil8io/rancher-namespace-filter:0.1.0
          args:
            - --upstream=http://rancher.cattle-system.svc
            - --token-file=/var/run/secrets/rancher/token
          ports:
            - containerPort: 8080
          volumeMounts:
            - name: token
              mountPath: /var/run/secrets/rancher
              readOnly: true
          readinessProbe:
            httpGet:
              path: /healthz
              port: 8080
          livenessProbe:
            httpGet:
              path: /healthz
              port: 8080
          securityContext:
            runAsNonRoot: true
            readOnlyRootFilesystem: true
            allowPrivilegeEscalation: false
            capabilities:
              drop:
                - ALL
            seccompProfile:
              type: RuntimeDefault
      volumes:
        - name: token
          secret:
            secretName: rancher-namespace-filter-token
```

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: rancher-namespace-filter-token
type: Opaque
stringData:
  token: <api-token>
```

```yaml
apiVersion: v1
kind: Service
metadata:
  name: rancher-namespace-filter
spec:
  selector:
    app: rancher-namespace-filter
  ports:
    - port: 8080
      targetPort: 8080
```

## Behaviour differences

| Difference | Detail |
| --- | --- |
| Cache delay | A role change becomes visible after the cache TTL, on top of Rancher's own delay. |
| Field selector | A field selector on a name outside the allowed set returns an empty list. A `get` on that name returns Forbidden. |
| Self-check | `kubectl auth can-i list namespaces` returns yes, while RBAC returns no. |
| Trust level | The service is a privileged component. It uses the cluster-owner token only for the namespace list and the watch with the name selector. |

## Logging

The service writes JSON logs to stderr, one line per intercepted request.

A namespace list log line has these fields: `cluster`, `outcome` (`native`, `filtered`, `passthrough`, `denied`, or `error`), `status`, `count` (only on `filtered`), `watch`, and `duration_ms`.

A review log line has these fields: `cluster`, `outcome` (`passthrough`, `native`, or `granted`), and `status`.

The service never logs a token, a cookie, a header value, or a request body. It logs a namespace name at the `debug` level only.

The service writes a `warn` line when the privileged list request gets a 403 error. The cause is a `cluster-owner` binding that the service user does not have.

## Development

Run these checks before every commit:

1. `go test -race ./...`
2. `go vet ./...`
3. `gofmt -l .` (it must print nothing)

Build the binary with `go build ./cmd/rancher-namespace-filter`.

Build the image with `docker build` or `podman build`. Pass the version with the `VERSION` build argument:

```
podman build --build-arg VERSION=0.1.0 -t rancher-namespace-filter:0.1.0 .
```

## Releases

PR titles follow Conventional Commits. The project squash-merges every pull request. release-please reads the PR titles and opens a release pull request. A merge of that pull request creates a tag, a GitHub release, and the image tags for the new version.

## License

[Apache License 2.0](LICENSE)
