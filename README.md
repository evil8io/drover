# drover

[![ci](https://github.com/evil8io/drover/actions/workflows/ci.yml/badge.svg)](https://github.com/evil8io/drover/actions/workflows/ci.yml)
[![release](https://img.shields.io/github/v/release/evil8io/drover)](https://github.com/evil8io/drover/releases)
[![license](https://img.shields.io/github/license/evil8io/drover)](LICENSE)

drover is a set of tenancy extensions for [Rancher](https://github.com/rancher/rancher). Rancher groups the namespaces of a cluster into projects, and a project member has access inside the namespaces of its projects only. A Kubernetes client that lists namespaces, or that lists pods across namespaces, then gets a 403 error from Rancher.

drover answers those reads with the namespaces and the objects that the member may see. It also copies the labels and the annotations of a project to the namespaces of the project. A CronJob renews the API token of the Rancher service user that drover uses. drover is one Go binary with one subcommand per component, one container image, and one Helm chart.

## Components

| Component | Subcommand | Function | Docs |
| --- | --- | --- | --- |
| API filter | `api-filter` | A reverse proxy in front of Rancher. It answers the namespace list of a project member with the namespaces of its projects. With the fan-out on, it also answers a cluster-wide list or watch of a namespaced kind, for example `kubectl get pods -A`. | [docs/api-filter.md](docs/api-filter.md) |
| Project sync | `project-sync` | A service that copies an allow list of labels and annotations from a Rancher project to the namespaces of that project. It also writes the display name of the project on those namespaces. | [docs/project-sync.md](docs/project-sync.md) |
| Token rotation | `rotate-token` | A command, run by a CronJob, that renews the API token of the Rancher service user in a Secret before the token expires. It also writes the password hash that Rancher reads at a local login of that user. | [docs/rotate-token.md](docs/rotate-token.md) |

Each document has the behaviour, the flags, the permissions, the log fields, and the metrics of its component.

## How it works

```
 kubectl, k9s, Headlamp
          │
          ▼
 Gateway on the Rancher hostname
          │
          ├─ filtered read paths ──► api-filter ──┐
          │                                       ▼
          └─ every other path ─────────────────► Rancher ◄──── project-sync
                                                  ▲
                                             rotate-token
```

Every component authenticates to Rancher as one Rancher service user. The chart creates that user and its Rancher roles, and it ships two Secrets for it. The credentials Secret gets its password from the external-secrets `Password` generator, and the token Secret gets its API token from the token rotation. The API filter and the project sync read the token file on every use, so a rotation needs no restart.

A Gateway serves the Rancher hostname. A Kyverno `GeneratingPolicy` from the chart writes one `HTTPRoute` per Rancher cluster object. The route sends the namespace list and the access review of that cluster to the API filter, and every other path goes to Rancher directly. With the fan-out on, the route also sends every `GET` under the `api` and `apis` paths of the cluster to the filter, except the paths under `api/v1/namespaces`. The filter passes a request that it does not answer to Rancher unchanged, so a write, an `exec`, and a log stream never depend on it.

## Requirements

The chart installs into the cluster that runs Rancher. The table names what drover needs, and whether the chart installs it.

| Requirement | Installed by the chart | Note |
| --- | --- | --- |
| Kubernetes 1.27 or later | No | The `kubeVersion` of the chart. |
| Rancher | No | The `rancher.url` value names the Rancher Service in the cluster. |
| Gateway API, with a `Gateway` that serves the Rancher hostname | No | The `httpRoute.parentRefs` value names the Gateway. |
| Kyverno, with the `GeneratingPolicy` kind of `policies.kyverno.io/v1` | No | Kyverno generates the routes. |
| external-secrets, with the `Password` generator | No | The generator makes the password of the service user. |
| A `ReferenceGrant` in the Rancher namespace | No | Only with the fan-out on, when the Rancher Service is in another namespace than the release. |
| An OTLP gRPC endpoint for traces and metrics | No | Optional. An empty `otlp.endpoint` value turns telemetry off. |
| The Rancher service user, its `RoleTemplate`, its `GlobalRole`, and its bindings | Yes | Two Helm hook Jobs write and delete the Rancher roles and bindings. |
| The credentials Secret and the token Secret of the service user | Yes | The chart ships the `ExternalSecret` for the password and the token Secret without data. |
| The `GeneratingPolicy`, and one `HTTPRoute` per Rancher cluster | Yes | The chart ships the policy, and Kyverno generates the routes. |
| The Deployments, the CronJob, the ServiceAccounts, the RBAC objects, and the PodDisruptionBudget | Yes | |

The current release is tested against Rancher 2.14, Gateway API 1.6, Kyverno 1.19, and external-secrets 2.10.

## Installation

The Helm chart is in the [evil8io/charts](https://github.com/evil8io/charts) repository, at [charts/drover](https://github.com/evil8io/charts/tree/main/charts/drover), and it is published to `oci://ghcr.io/evil8io/charts/drover`. The [chart README](https://github.com/evil8io/charts/blob/main/charts/drover/README.md) describes the service user, the routes, and the values.

Install the chart after Rancher, the Gateway, Kyverno, and external-secrets. The install hook needs the Rancher webhook, and the chart objects need the CRDs of Kyverno, external-secrets, and the Gateway API.

```yaml
# values.yaml
rancher:
  url: https://rancher.cattle-system
  insecureSkipVerify: true

httpRoute:
  parentRefs:
    - name: rancher
      namespace: gateway

projectSync:
  nameLabel: project.name
  nameAnnotation: project.display_name
```

```sh
helm install drover oci://ghcr.io/evil8io/charts/drover \
  --namespace drover-system --create-namespace \
  --values values.yaml
```

The Rancher Service in the cluster has a certificate from a private CA, and the chart does not copy that CA, so the example skips the verification. A network policy must then limit the path to Rancher.

The container image is `ghcr.io/evil8io/drover`, built for `amd64` and `arm64` on a distroless base. An empty `image.tag` value selects the `appVersion` of the chart.

## Observability

Each component writes JSON logs to stderr, and it never logs a token or a password. With an OTLP endpoint set, each component exports traces and metrics over OTLP gRPC. The metric names start with `drover.filter`, `drover.sync`, and `drover.rotate`. The component documents list the log fields, the spans, and the metrics.

## Development

```
cmd/drover/              the binary, with one file per subcommand
internal/filter/         the API filter
internal/projectsync/    the project sync
internal/rotate/         the token rotation
internal/rancherclient/  the HTTP client for Rancher
internal/telemetry/      logs, traces, and metrics
```

drover needs the Go version in `go.mod`. Run these checks before a commit:

1. `go test -race ./...`
2. `go vet ./...`
3. `gofmt -l .` (it must print nothing)

CI runs the same checks, plus golangci-lint, on every pull request.

Build the binary with `go build ./cmd/drover`.

Build the image with `podman build` or `docker build`. Pass the version with the `VERSION` build argument:

```sh
podman build --build-arg VERSION=0.14.0 -t drover:0.14.0 .
```

## Releases

Pull request titles follow [Conventional Commits](https://www.conventionalcommits.org/), and every pull request is squash-merged. [release-please](https://github.com/googleapis/release-please) reads the titles and opens a release pull request. A merge of that pull request creates a tag, a GitHub release, and the image tags `<version>`, `<major>.<minor>`, and `latest`. A push to `main` also pushes the image tags `main` and `sha-<short sha>`. drover has no 1.0 release yet, and a breaking change raises the minor version.

## License

[Apache License 2.0](LICENSE)
