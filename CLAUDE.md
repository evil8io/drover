# CLAUDE.md

drover is a set of tenancy extensions for Rancher: one Go binary, one subcommand per component. `README.md` is for humans. This file and the `CLAUDE.md` per component record only what the code does not show: rules, invariants, gotchas, external constraints, and wiring. Write a rule in the imperative, and give the reason. Delete a line when it stops being useful.

## Public repository

- This repository is public. Write no company name, no customer name, no environment name, and no issue tracker id anywhere: not in code, comments, commit messages, branch names, or pull request text. Link an issue from the tracker to the pull request by hand.
- A key that drover writes on a Kubernetes object has no organisation prefix, for the same reason. Example: `drover-managed-labels`.

## Wiring

- The Helm chart is not in this repository. It is `charts/drover` in the public [evil8io/charts](https://github.com/evil8io/charts) repository, synced from a private source. Do not add a chart here. A release here reaches a deployment only after an `appVersion` bump in that chart.
- The image entrypoint is the binary, and the chart passes the subcommand and the flags as arguments.
- Every component authenticates as one Rancher service user through one token file. `rotate-token` is the only writer of that Secret, and the other two components read the file on every use, so a rotation needs no restart. Keep that split.
- This repository has unit tests only. The integration tests run in the private repository that deploys the chart. A change that a unit test cannot cover needs a live check with a real client; name the client in the pull request.

## Components

| Subcommand | Package | Notes |
| --- | --- | --- |
| `api-filter` | `internal/filter` | `internal/filter/CLAUDE.md` |
| `project-sync` | `internal/projectsync` | `internal/projectsync/CLAUDE.md` |
| `rotate-token` | `internal/rotate` | `internal/rotate/CLAUDE.md` |

`internal/rancherclient` is the shared HTTP transport to Rancher, and `internal/telemetry` is the shared logging, tracing, and metrics setup.

## Docs

- `docs/<subcommand>.md` is the human document of a component. Read it before a change to that component. Update it in the same pull request when the behaviour, a flag, a permission, a log field, or a metric changes.
- `CONTRIBUTING.md` has the checks, the build, and the release flow. Do not repeat them here.
- Write prose in the output style `.claude/output-styles/asd-ste100.md`. A sub-agent does not get the output style, so a sub-agent that writes a commit message, a pull request body, or a doc reads that file first.

## Rancher facts

Verified against Rancher 2.14.5. Each component file has the facts of its own API surface.

- Rancher answers `400 Use HTTPS` to a login over plain HTTP. In the Rancher cluster, `rancher.<namespace>` is in the serving certificate, and `rancher.<namespace>.svc` is not.
- The CA of that serving certificate is the Secret `tls-rancher`, key `tls.crt`, in the Rancher namespace. `tls-rancher-internal-ca` is a different CA and does not verify it.
- Rancher grants a project member `get` on the namespaces of its projects and no `list`. The api-filter exists for that gap.
- A binding that grants `list` on `namespaces` to `system:cattle:authenticated` makes the native list succeed, and the filter then never filters, because the filter acts on a 403 only. Look for such a binding first when a tenant sees every namespace.
- The cluster-wide `list` and `watch` on `projects` of `management.cattle.io`, in the `local` cluster, needs a `ClusterRoleTemplateBinding` on the `local` cluster. The GlobalRole of the service user gives only the per-cluster RoleBindings, never this cluster-wide grant.

## Releases

- A release-please pull request shows a failed `ci` run and a failed `pr-title` run with zero jobs. The bot is an outside actor, so the runs start as `action_required` and expire into `failure`. Do not chase that. The `ci` run on `main` must pass.
- Before 1.0, a `feat!` bumps the minor version, because release-please runs with `bump-minor-pre-major`.

## Rejected designs

Do not propose these again:

| Design | Reason |
| --- | --- |
| A proxy between the cluster agent and the API server | On a managed cluster no certificate validates against the cluster CA, and the proxy is on the critical path of Rancher. |
| A bare path rewrite of the namespace list to Steve | Steve has no `labelSelector` and no watch. |
| A `--clusters` allow list on `api-filter` | The route set is dynamic. A policy in the chart makes one HTTPRoute per Rancher Cluster object. |
| A principal or user filter | A directory search inside the organisation is accepted, because a project owner adds members self-service. |
| Admin tokens for the filter, or a daily rotation schedule | One service user with one token, and a check-and-renew rotation every 10 minutes, are the design. |
| A widened union selector on a namespace watch | The set of extra namespaces is unbounded. See `internal/filter/CLAUDE.md`. |
| A chart in this repository | See Wiring. |
