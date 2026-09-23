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

# What's the problem with Rancher?

Rancher excels at centralised authentication, authorization, project management, and granting self-service capabilities. However, it lacks certain quality-of-life features that make it a complete multi-tenant Kubernetes platform. For example, Rancher lacks a robust set of policies that sufficiently isolate tenants from each other. Similarly, engineers' native client applications, such as kubectl, k9s, openlens, etc, do not work well, because Rancher lists project namespaces through its own UI and rancher-cli only, and not through the Kubernetes API.

# Enter drover

Drover aims to improve the developer experience of working with Rancher managed Kubernetes clusters. It does this through a set of helm-charts:

| Chart | Location | Management Cluster | Workload Cluster | Description |
| --- | --- | --- | --- | --- |
| drover | [charts/drover](https://github.com/evil8io/charts/tree/main/charts/drover) | ✅ | ❌ | Offers a set of services that make life more enjoyable for developers |
| drover-policies | [charts/drover-policies](https://github.com/evil8io/charts/tree/main/charts/drover-policies) | ✅ | ✅ | Offers a strict baseline of Kyverno policies that isolate tenants and forces a secure configuration for workloads |

## Services

| Service | Subcommand | Function | Docs |
| --- | --- | --- | --- |
| API filter | `api-filter` | A reverse proxy in front of Rancher. It allows users to list their project namespaces with kubectl and it scopes the `--all-namespaces` flag to their project namespaces. | [docs/api-filter.md](docs/api-filter.md) |
| Project sync | `project-sync` | A service that propagates a configured set of labels and annotations from a Rancher project to the namespaces of that project. | [docs/project-sync.md](docs/project-sync.md) |
| Token rotation | `rotate-token` | A command, run by a CronJob, that rotates the password and the API token of the drover Rancher user. | [docs/rotate-token.md](docs/rotate-token.md) |

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
