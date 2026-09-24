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
