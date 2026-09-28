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

Rancher provides centralised authentication, authorization, project management, and self-service for its users. However, Rancher does not have some convenience features. Without these features, Rancher is not complete as a multi-tenant Kubernetes platform. For example, Rancher does not have a set of policies that isolate tenants from each other sufficiently.

Similarly, the native client applications that engineers use, for example kubectl, k9s, and openlens, do not work well. The reason is that Rancher lists project namespaces only through its own UI and rancher-cli. Rancher does not list project namespaces through the Kubernetes API.

# What drover does

The goal of drover is to improve the developer experience with Kubernetes clusters that Rancher manages. drover does this with a set of Helm charts:

| Chart | Location | Management Cluster | Workload Cluster | Description |
| --- | --- | --- | --- | --- |
| drover | [charts/drover](https://github.com/evil8io/charts/tree/main/charts/drover) | ✅ | ❌ | A set of services that improve the developer experience. |
| drover-policies | [charts/drover-policies](https://github.com/evil8io/charts/tree/main/charts/drover-policies) | ✅ | ✅ | A strict baseline of Kyverno policies. With these policies, Kyverno isolates tenants, and it forces a secure configuration for workloads. |

## Services

| Service | Subcommand | Function | Docs |
| --- | --- | --- | --- |
| API filter | `api-filter` | A reverse proxy in front of Rancher. It lets users list their project namespaces with kubectl. It also limits the `--all-namespaces` flag to their project namespaces. | [docs/api-filter.md](docs/api-filter.md) |
| Project sync | `project-sync` | A service that copies a configured set of labels and annotations from a Rancher project to the namespaces of that project. | [docs/project-sync.md](docs/project-sync.md) |
| Token rotation | `rotate-token` | A command that rotates the API token of a Rancher service user of drover. With `--password-secret`, it also writes the password hash of that user. It reads the password from a file. A CronJob runs the command. | [docs/rotate-token.md](docs/rotate-token.md) |

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
