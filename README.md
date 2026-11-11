<p align="left">
  <img src="https://img.shields.io/github/license/evil8io/drover"/>
  <img src="https://img.shields.io/github/go-mod/go-version/evil8io/drover"/>
  <a href="https://github.com/evil8io/drover/releases">
    <img src="https://img.shields.io/github/v/release/evil8io/drover"/>
  </a>
  <a href="https://github.com/evil8io/drover/actions/workflows/ci.yml">
    <img src="https://github.com/evil8io/drover/actions/workflows/ci.yml/badge.svg"/>
  </a>
</p>

<p align="center">
  <img src="assets/logo.svg" width=360 />
</p>

---

# Multi-tenancy extension for SUSE Rancher

**drover** is a set of extensions for [Rancher](https://github.com/rancher/rancher).

# Why drover

Rancher provides centralised authentication, authorization, project management, and self-service for its users. Users are members of one or more Rancher projects. Users have access to their own namespaces inside those projects.

drover adds the following features:

- Allow users to list (only) their namespaces with kubectl, e.g. `kubectl get namespaces`
- Allow users to request resources across all their namespaces, e.g. `kubectl get configmaps --all-namespaces`
- Automatically propagate project labels and annotations to the namespaces of the project
- Provide a mechanism to furnish short-lived kubectl tokens to non-human entities, such as CI workflows (see [drover-login-action](https://github.com/evil8io/drover-login-action))
- Provide a set of policies that enhance the isolation of multiple tenants

# Components

drover comes as a set of helm-charts:

| Chart | Location | Management Cluster | Workload Cluster | Description |
| --- | --- | --- | --- | --- |
| drover | [charts/drover](https://github.com/evil8io/charts/tree/main/charts/drover) | ✅ | ❌ | Services and Kyverno policies |
| drover-policies | [charts/drover-policies](https://github.com/evil8io/charts/tree/main/charts/drover-policies) | ✅ | ✅ | Kyverno policies |

## Services

| Service | Subcommand | Function | Docs |
| --- | --- | --- | --- |
| API filter | `api-filter` | Reverse proxy in front of Rancher.<br>Allows users to list their project namespaces with kubectl.<br>Limits the `--all-namespaces` flag to allowed project namespaces. | [docs/api-filter.md](docs/api-filter.md) |
| OpenBao | N/A | Facilitates short-lived access tokens. | [chart README](https://github.com/evil8io/charts/tree/main/charts/drover#short-lived-credentials) |
| Project sync | `project-sync` | Propagates project metadata.<br>Automatically programs OpenBao for each Rancher cluster and project. | [docs/project-sync.md](docs/project-sync.md) |
| Token rotation | `rotate-token` | Rotates Rancher credentials used by drover itself. | [docs/rotate-token.md](docs/rotate-token.md) |

## Example

```
$ kubectl get namespaces
No resources found

$ kubectl create namespace shire
namespace/shire created

$ kubectl create namespace gondor
namespace/gondor created

$ kubectl get namespaces -L project.name
NAME     STATUS   AGE   PROJECT.NAME
shire    Active   15s   Middle-Earth
gondor   Active   10s   Middle-Earth

$ kubectl get configmap --all-namespaces
NAMESPACE   NAME               DATA   AGE
shire       kube-root-ca.crt   1      15s
gondor      kube-root-ca.crt   1      10s
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[Apache License 2.0](LICENSE)
