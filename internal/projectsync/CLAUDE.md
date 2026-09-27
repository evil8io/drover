# project-sync

Human doc: `docs/project-sync.md`. Update it with a behaviour change.

- Rancher sets `field.cattle.io/projectId` on a new namespace about 3 s after the create. Act on `MODIFIED` as well as on `ADDED`, or a new namespace misses its keys until the next reconcile.
- Read the project of a namespace from the annotation `field.cattle.io/projectId`, never from the label. Rancher sets the label from the annotation, but it keeps the label when the annotation goes, and it grants the project roles by the annotation. A kubectl removal of the annotation and a project delete leave the label. The Rancher UI "no project" move removes both in one write, so that namespace leaves the label selector of the watch with a DELETED event, and only a reconcile list without a selector finds it. That DELETED event has the object from before the change, verified on Kubernetes 1.35, so read the namespace again before acting on it.
- The cluster set comes from `GET /v3/projects`, which Rancher filters by RBAC. Do not add a cluster list call. A cluster without a visible project has nothing to sync.
- Prune every decoded namespace to the keys that the sync reads. A tenant controls the size of its namespace metadata, and the pod has a fixed memory limit.
- Treat a record value above `maxRecordValue` (4096 bytes) as absent. A tenant can fill a record up to the 256 KiB annotation limit, and a record that the service writes is a short key list.
- The watch handler reads the token file per event, because a rotation replaces the token while the stream stays open. A 410 or an `ERROR` event restarts the stream without a resource version.
- The ownership record is the two namespace annotations `drover-managed-labels` and `drover-managed-annotations`, with bare keys on purpose. A tenant can edit them, so a removal touches only a key that the flags name, and every other key in the record is ignored. The flags reject the two record keys.
- The name path owns its key. A key in `--labels` and in `--name-label` at once takes the display name, never the label of the Project object.
- A display name is free text, so the sanitised label is lossy and not unique. The annotation keeps the raw name.
- A patch that answers 404, 409, or a 403 whose message names the terminating state is skipped and counts no error. The next reconcile repeats the work.
- The first run after an upgrade that adds a record key patches every namespace once. Expect one patch per namespace in the metrics after such a release.
- A deployment tool that reads back every label of a namespace shows the keys of the sync as drift. Name a new key in `docs/project-sync.md`, so a deployer can ignore it.
- The project watch reads `GET /k8s/clusters/local/apis/management.cattle.io/v3/projects?watch=true`, verified against Rancher 2.14.5. Grant the cluster-wide `list` and `watch` on `projects` with a `ClusterRoleTemplateBinding` on the `local` cluster. A per-cluster binding does not give this permission.
- Start the project watch in the reconcile run, after the first snapshot. Before that, the watch has no snapshot to compare against, and a missing token file would give a warning per attempt.
- Expect a restart without a resource version to replay every Project as an `ADDED` event. Each replay is an in-memory compare, so a restart costs no request.
- Never let the project watch start a reconcile run. A cluster member can create Projects, so a trigger there would let that member start runs at will.
- Take a token from the cluster limiter before the lister lists a project. A project owner can edit its Project in a loop, so an unlimited lister would repeat the list rapidly.
- Replace the snapshot maps of a cluster on every change. Never mutate them in place, because a worker and the reconcile run read them without a lock.
- A reconcile run that reads the project list before a Project change can overwrite that change in the snapshot, and patch the old values back. This is a known, accepted gap: the window lasts one list request, and the next event or run closes it.

## Service accounts

Verified live against Rancher 2.14.5, and against the source of Rancher 2.14.6 and its webhook 0.10.12.

- Trust an account project only by the label `drover-service-accounts` together with the annotation `field.cattle.io/creatorId` equal to the service user. A project owner can set the label on its own project. The webhook denies a change and an addition of the creator annotation on an update, also for an admin. A cluster member can create a project with the service user as creator, because no webhook compares the annotation with the requester, but Rancher then binds only the service user as owner, so the maker gets no role in it.
- Create the account project without `field.cattle.io/no-creator-rbac`. The webhook rejects that annotation together with a creator, and a project without a creator fails the trust check.
- Use an account namespace only in an account project. The name is no proof, because a tenant can create it first in its own project.
- Move a namespace in no project into the account project only when a ClusterRoleBinding of the service names it as owner by uid. The namespace webhook checks `manage-namespaces` on the new project only, and nothing on a removal of the project annotation, so a project owner can put its own namespace, with its own RoleBindings, into no project.
- Do not set `field.cattle.io/creatorId` on an account namespace. Rancher deletes a namespace with that annotation together with its project, and every token of its ServiceAccounts with it. Without it, a deleted account project leaves its namespaces in no project, and the next run moves them into a new account project.
- Never bind `view` or `edit` in an account namespace. The API filter reviews the rules of a ServiceAccount in its own namespace, and such a binding makes the allowed set unbounded. See `internal/filter/CLAUDE.md`.
- Delete an account namespace of an unknown project only after `GET /v3/projects/<cluster>:<project>` answers 404. Keep it on any other status. The project list of a run can be older than a new project.
- Delete the bindings of a project before its account namespace. A binding names a ServiceAccount by namespace and name, not by uid, so a binding that outlives the namespace grants a tenant who creates that namespace name next.
- Never set `blockOwnerDeletion` on an owner reference. It needs a right on the owner that the service user does not have.
- Rancher creates `<project>-namespaces-edit` and `<project>-namespaces-readonly` shortly after the project, also without a member or a namespace.
- Send a PATCH as `application/merge-patch+json`. The API server answers 415 to `application/json`.

## OpenBao

The answer to a missing mount comes from the source of OpenBao 2.7.0. No part of this section has a live check yet.

- Refresh the config of a cluster when less than half of the lifetime of its token remains. OpenBao keeps a written `service_account_jwt` and never renews it, and EKS limits a TokenRequest to 24 h. The other half is the time for the retries of a failed write.
- Take the expiry from `status.expirationTimestamp` of the TokenRequest answer, never from `--openbao-token-ttl`. The API server can shorten the lifetime.
- Compare the uid of the ServiceAccount `openbao` with the uid of the last write. A new ServiceAccount makes the written token invalid, and the expiry alone would wait up to half of the lifetime.
- Send the Rancher CA as `kubernetes_ca_cert` in every write, also when it is empty. The Kubernetes secrets engine of OpenBao cannot skip the TLS verification: it trusts only `kubernetes_ca_cert` when that has a value, and the system roots otherwise. An empty string in the request removes the stored CA, because the write takes every field that the request has. The sources are `builtin/logical/kubernetes/client.go` and `path_config.go` of OpenBao 2.6.3.
- Read the setting `cacerts` in every run that has a cluster to keep, not only in a run with a due token, so that a CA rotation reaches OpenBao in one run. Write nothing after a failed read. An empty CA makes OpenBao verify a private certificate with the system roots, and every token request of the engine then fails.
- The service user reads `/v3/settings/cacerts` through the GlobalRole `user-base`, which grants `get` on `settings` (Rancher 2.14.6, `pkg/data/management/role_data.go`).
- Keep `--openbao-rancher-url` apart from `--rancher-url`. The service can reach Rancher through its Service with `--rancher-insecure-skip-verify`, but OpenBao verifies the certificate of Rancher.
- OpenBao checks the policy before it routes a request. A write under a missing mount answers 404 `no handler for route "<path>". route entry not found.` only when the policy allows `update` on the path, and 403 otherwise. An older Vault server answers 400 with the same message. The sources are `internal/vault/routing/router.go` and `sdk/logical/response_util.go`.
- Use `drover-openbao` only in an account project. Move it out of no project only with the proof of an OpenBao RoleBinding in a trusted account namespace, because only the service and an admin write there. Never take a binding in another namespace as proof: a tenant can write one with that owner reference in its own namespace.
- The RoleBinding `drover-openbao` names `drover-openbao/openbao` by namespace and name. Let the namespace `drover-openbao` own it by uid, and delete every such RoleBinding when the namespace is not trusted. Otherwise a tenant who creates `drover-openbao` and a ServiceAccount `openbao` gets the tokens of every project role.
- The lister writes the OpenBao Role and RoleBinding of a project with the uid of `drover-openbao` that the last reconcile run trusted. Keep that uid only after a trusted check, and clear it on every other outcome, because the RoleBinding would otherwise name the ServiceAccount of a namespace that the service does not own. The lister creates each object first and reads it only after a 409, so it needs no list call. A stale uid costs one garbage collection of the RoleBinding, and the next run writes it again.
- Skip `drover-openbao` in the sweep of the account namespaces. It has the account prefix and no project, so the 404 check of the sweep deletes it. For the same name, a project called `openbao` gets no accounts.
- Keep the `resourceNames` of the Role inside the `serviceaccounts/token` grant of the service user. The user creates and binds the Role without `escalate` and `bind`, because it holds every rule of the Role.
- OpenBao reaches a cluster through the Rancher proxy with a ServiceAccount token. Rancher needs a `ClusterProxyConfig` per cluster for that, see `internal/filter/CLAUDE.md`.
