# rancher-namespace-filter

This chart installs rancher-namespace-filter, an HTTP reverse proxy in front of Rancher. The proxy lets a tenant list only the namespaces of its projects. A route must send two paths per cluster id to the Service of this chart. Every other path goes to Rancher directly.

## Install

```
helm install rancher-namespace-filter oci://ghcr.io/evil8io/charts/rancher-namespace-filter \
  --version <version> \
  --namespace <ns> \
  --set upstream.url=http://rancher.cattle-system.svc \
  --set token.existingSecret=<secret>
```

The chart version is equal to the image version. The chart needs `upstream.url`, and it needs `token.existingSecret` or `token.value`. The values schema rejects a release without them.

The upstream is the Rancher Service in the cluster. The public hostname is not a valid upstream, because the route sends the two filtered paths back to this service.

## High availability

The defaults keep the service available during a node drain and during an upgrade:

1. `replicaCount` is 2.
2. A PodDisruptionBudget keeps 1 pod available.
3. A spread over `kubernetes.io/hostname` is mandatory, with `whenUnsatisfiable: DoNotSchedule`.
4. A spread over `topology.kubernetes.io/zone` is optional, with `whenUnsatisfiable: ScheduleAnyway`.
5. The update strategy is `RollingUpdate` with `maxUnavailable: 0` and `maxSurge: 1`.

The chart adds a `labelSelector` with the selector labels to a spread constraint that has none. A constraint with its own `labelSelector` stays unchanged.

A cluster with one node cannot schedule the second pod. Set `replicaCount: 1` on such a cluster. An alternative is `whenUnsatisfiable: ScheduleAnyway` on the hostname constraint.

## Route

Set `httpRoute.enabled: true` to create the `HTTPRoute`. Give `httpRoute.parentRefs`, `httpRoute.hostnames`, and `httpRoute.clusterIds`. The chart renders one rule with two matches per cluster id:

1. Method `GET` on path `/k8s/clusters/<id>/api/v1/namespaces`.
2. Method `POST` on path `/k8s/clusters/<id>/apis/authorization.k8s.io/v1/selfsubjectaccessreviews`.

Both matches use the `Exact` path type. An `Exact` match takes precedence over the `PathPrefix /` match of the Rancher route. The method match keeps a namespace create (`POST`) away from the filter.

The Gateway API allows 64 matches in one rule, so one route covers 32 cluster ids. Split the cluster ids over more releases above that limit.

The route can also come from outside the chart. Keep `httpRoute.enabled: false` in that case. An Ingress with the same two paths per cluster id also works.

## Token

The service needs the API token of a Rancher service user with a `cluster-owner` binding. Use one of these two options:

1. Set `token.existingSecret` to the name of a Secret that has the token.
2. Set `token.value` to the token. The chart then creates the Secret `<fullname>-token`.

`token.key` names the key in the Secret. The default is `token`.

The service reads the token file on every use, so a new token in the Secret needs no restart. The chart therefore sets no checksum annotation on the pods. The service reads the CA bundle at start, so a change of the `upstream.caSecret` content needs a rollout.

## Values

| Key | Default | Meaning |
| --- | --- | --- |
| `image.repository` | `ghcr.io/evil8io/rancher-namespace-filter` | Image repository. |
| `image.tag` | `""` | Image tag. An empty value selects the chart `appVersion`. |
| `image.pullPolicy` | `IfNotPresent` | Image pull policy. |
| `imagePullSecrets` | `[]` | Secrets that pull the image. |
| `nameOverride` | `""` | Replaces the chart name in the resource names. |
| `fullnameOverride` | `""` | Replaces the full resource name. |
| `replicaCount` | `2` | Number of pods. Minimum 1. |
| `upstream.url` | `""` | URL of Rancher. Required. Use `http://` or `https://`. |
| `upstream.caSecret.name` | `""` | Secret with a PEM bundle for an `https` upstream. |
| `upstream.caSecret.key` | `ca.crt` | Key of the PEM bundle in that Secret. |
| `token.existingSecret` | `""` | Secret that has the API token. |
| `token.key` | `token` | Key of the token in the Secret. |
| `token.value` | `""` | API token. The chart creates the Secret when `token.existingSecret` is empty. |
| `cacheTTL` | `15s` | Lifetime of a cached allowed set. |
| `logLevel` | `info` | One of `debug`, `info`, `warn`, or `error`. |
| `extraArgs` | `[]` | Extra arguments. The chart appends them after the generated arguments. |
| `service.type` | `ClusterIP` | Service type. |
| `service.port` | `8080` | Service port. The target port is the container port 8080. |
| `httpRoute.enabled` | `false` | Creates the `HTTPRoute`. |
| `httpRoute.annotations` | `{}` | Annotations on the `HTTPRoute`. |
| `httpRoute.parentRefs` | `[]` | Gateways of the route, for example `[{name: rancher, namespace: gateway}]`. |
| `httpRoute.hostnames` | `[]` | Hostnames of the route, for example `[rancher.example.com]`. |
| `httpRoute.clusterIds` | `[]` | Cluster ids. The chart renders two matches per id. |
| `serviceAccount.create` | `true` | Creates the ServiceAccount. |
| `serviceAccount.annotations` | `{}` | Annotations on the ServiceAccount. |
| `serviceAccount.name` | `""` | Name of the ServiceAccount. An empty value selects the full resource name. |
| `automountServiceAccountToken` | `false` | Mounts the ServiceAccount token in the pods. |
| `podAnnotations` | `{}` | Annotations on the pods. |
| `podLabels` | `{}` | Extra labels on the pods. |
| `podSecurityContext` | `runAsNonRoot: true`, `seccompProfile.type: RuntimeDefault` | Security context of the pods. |
| `securityContext` | `allowPrivilegeEscalation: false`, `readOnlyRootFilesystem: true`, `capabilities.drop: [ALL]` | Security context of the container. |
| `resources` | requests `10m` CPU and `32Mi`, limit `128Mi` | Container resources. |
| `podDisruptionBudget.enabled` | `true` | Creates the PodDisruptionBudget. |
| `podDisruptionBudget.minAvailable` | `1` | Number of pods that stay available during a voluntary disruption. |
| `topologySpreadConstraints` | hostname `DoNotSchedule`, zone `ScheduleAnyway` | Spread of the pods. The chart adds a `labelSelector` to an entry that has none. |
| `strategy` | `RollingUpdate` with `maxUnavailable: 0` and `maxSurge: 1` | Update strategy of the Deployment. |
| `terminationGracePeriodSeconds` | `30` | Time for a pod to stop. |
| `priorityClassName` | `""` | PriorityClass of the pods. |
| `nodeSelector` | `{}` | Node selector of the pods. |
| `tolerations` | `[]` | Tolerations of the pods. |
| `affinity` | `{}` | Affinity of the pods. |
| `livenessProbe` | `httpGet` on `/healthz`, port `http` | Liveness probe of the container. |
| `readinessProbe` | `httpGet` on `/healthz`, port `http` | Readiness probe of the container. |
