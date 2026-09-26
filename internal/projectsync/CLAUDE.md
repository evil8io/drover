# project-sync

Human doc: `docs/project-sync.md`. Update it with a behaviour change.

- Rancher sets `field.cattle.io/projectId` on a new namespace about 3 s after the create. Act on `MODIFIED` as well as on `ADDED`, or a new namespace misses its keys until the next reconcile.
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

Verified against Rancher 2.14.5 and its webhook.

- Trust an account project only by the label `drover-service-accounts` together with the annotation `field.cattle.io/creatorId` equal to the service user. A project owner can set the label on its own project. The webhook denies a change and an addition of the creator annotation on an update, also for an admin. A cluster member can create a project with the service user as creator, because no webhook compares the annotation with the requester, but Rancher then binds only the service user as owner, so the maker gets no role in it.
- Create the account project without `field.cattle.io/no-creator-rbac`. The webhook rejects that annotation together with a creator, and a project without a creator fails the trust check.
- Use an account namespace only in an account project. The name is no proof, because a tenant can create it first in its own project.
- Move a namespace in no project into the account project only when a ClusterRoleBinding of the service names it as owner by uid. The namespace webhook checks `manage-namespaces` on the new project only, and nothing on a removal of the project annotation, so a project owner can put its own namespace, with its own RoleBindings, into no project.
- Do not set `field.cattle.io/creatorId` on an account namespace. Rancher deletes a namespace with that annotation together with its project, and every token of its ServiceAccounts with it. Without it, a deleted account project leaves its namespaces in no project, and the next run moves them into a new account project.
- Never bind `view` or `edit` in an account namespace. The API filter reviews the rules of a ServiceAccount in its own namespace, and such a binding makes the allowed set unbounded. See `internal/filter/CLAUDE.md`.
- Delete an account namespace of an unknown project only after `GET /v3/projects/<cluster>:<project>` answers 404. Keep it on any other status. The project list of a run can be older than a new project.
- Delete the bindings of a project before its account namespace. A binding names a ServiceAccount by namespace and name, not by uid, so a binding that outlives the namespace grants a tenant who creates that namespace name next.
- Never set `blockOwnerDeletion` on an owner reference. It needs a right on the owner that the service user does not have.
- Read the project of a namespace from the annotation `field.cattle.io/projectId`, never from the label. Rancher sets the label from the annotation, but it keeps the label when the annotation goes, and it grants the project roles by the annotation. A namespace that leaves its project thus stays on the namespace watch, whose label selector still matches, and the service acts on the MODIFIED event. A DELETED event comes only when the label goes too.
- Rancher creates `<project>-namespaces-edit` and `<project>-namespaces-readonly` shortly after the project, also without a member or a namespace.
- Send a PATCH as `application/merge-patch+json`. The API server answers 415 to `application/json`.
