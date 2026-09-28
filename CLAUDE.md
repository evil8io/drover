# CLAUDE.md

drover is a set of tenancy extensions for Rancher. It is one Go binary with one subcommand per component. `README.md` is for human readers. This file and the `CLAUDE.md` of each component are only for what the code does not show: rules, invariants, gotchas, external constraints, and wiring.

Write a rule in the imperative. Give the reason for the rule. Delete a line when it is no longer useful.

## Public repository

- This repository is public. Do not write a company name, a customer name, an environment name, or an issue tracker ID anywhere. This rule applies to code, comments, commit messages, branch names, and pull request text. Link an issue from the tracker to the pull request by hand.
- Do not add an organisation prefix to a key that drover writes on a Kubernetes object, for the same reason. An example of a key without a prefix is `drover-managed-labels`.

## Wiring

- The Helm chart is not in this repository. It is `charts/drover` in the public [evil8io/charts](https://github.com/evil8io/charts) repository. The chart in that repository is synced from a private source. Do not add a chart here. A release of this repository is deployed only after the `appVersion` of that chart is bumped.
- The image entrypoint is the binary. The subcommand and the flags are passed to the binary as arguments from the chart.
- The api-filter and `project-sync` authenticate as two Rancher service users. Each of the two components authenticates through its own token file. The api-filter is on the request path of every tenant, and it needs only the namespace list. With `--service-accounts`, `project-sync` writes namespaces and bindings. Keep the two users separate, so that the api-filter never uses the rights of `project-sync` for a tenant request.
- `rotate-token` runs once per service user, and only `rotate-token` writes each token Secret. The other components read the token file each time that they use the token, so a rotation needs no restart. Keep that division of work.
- The tests in this repository are unit tests only. The integration tests run in the private repository from which the chart is deployed. A change that a unit test cannot cover needs a live check with a real client. Name the client in the pull request.

## Components

| Subcommand | Package | Notes |
| --- | --- | --- |
| `api-filter` | `internal/filter` | `internal/filter/CLAUDE.md` |
| `project-sync` | `internal/projectsync` | `internal/projectsync/CLAUDE.md` |
| `rotate-token` | `internal/rotate` | `internal/rotate/CLAUDE.md` |

`internal/rancherclient` has the HTTP transport to Rancher of `api-filter` and `project-sync`, and the trace and metric wrapper of that transport. `rotate-token` uses only the wrapper, with its own transport from `internal/rotate/client.go`. `internal/telemetry` is the shared setup for logging, tracing, and metrics.

## Docs

- `docs/<subcommand>.md` is the document of a component for human readers. Read it before you change that component. Update it in the same pull request when one of these items of the component changes:
  - the behaviour
  - a flag
  - a permission
  - a log field
  - a metric
- The checks, the build, and the release flow are in `CONTRIBUTING.md`. Do not repeat them here.
- Write prose in the output style `.claude/output-styles/asd-ste100.md`. A sub-agent does not get the output style. For this reason, a sub-agent that writes a commit message, a pull request body, or a document reads that file first.

## Rancher facts

These facts were verified against Rancher 2.14.5. The facts about the API surface of each component are in the `CLAUDE.md` of that component.

- Rancher answers `400 Use HTTPS` to a login over plain HTTP. In the Rancher cluster, `rancher.<namespace>` is in the certificate that Rancher serves, and `rancher.<namespace>.svc` is not in that certificate.
- The CA of that certificate is in the key `tls.crt` of the Secret `tls-rancher`, in the Rancher namespace. `tls-rancher-internal-ca` is a different CA. A client cannot verify the certificate with that CA.
- Rancher grants a project member `get` on the namespaces of the member's projects. Rancher does not grant `list` on these namespaces. The api-filter exists for this reason.
- When a binding grants `list` on `namespaces` to `system:cattle:authenticated`, the native list succeeds. The api-filter then never filters, because it acts on a 403 only. Look for such a binding first when a tenant sees every namespace.
- In the `local` cluster, the cluster-wide `list` and `watch` on `projects` of `management.cattle.io` need a `ClusterRoleTemplateBinding` on the `local` cluster. The service user gets only the per-cluster RoleBindings from its GlobalRole. The service user never gets this cluster-wide right from the GlobalRole.

## Releases

- On a release-please pull request, GitHub shows a failed `ci` run and a failed `pr-title` run with zero jobs. The release-please bot is an outside actor. For this reason, the runs start as `action_required`, and they change to `failure` when they expire. Ignore these failed runs. The `ci` run on `main` must pass.
- Before 1.0, release-please bumps the minor version for a `feat!` commit, because it runs with `bump-minor-pre-major`.

## Rejected designs

Do not propose these designs again:

| Design | Reason |
| --- | --- |
| A proxy between the cluster agent and the API server | On a managed cluster, no certificate for the proxy is valid against the cluster CA. The proxy is also on the critical path of Rancher. |
| A bare path rewrite of the namespace list to Steve | Steve is the Rancher API server in the cluster agent. Steve has no `labelSelector`, and it has no watch. |
| A `--clusters` allow list on `api-filter` | The route set is dynamic. Kyverno creates one HTTPRoute per Rancher Cluster object from a policy in the chart. |
| A principal or user filter | A directory search inside the organisation is accepted, because a project owner adds members as a self-service task. |
| Admin tokens for the api-filter, or a daily rotation schedule | The api-filter uses one service user with one token. The token rotation is check-and-renew, and it runs every 10 minutes. |
| A widened union selector on a namespace watch | The set of extra namespaces is unbounded. See `internal/filter/CLAUDE.md`. |
| A chart in this repository | See the Wiring section. |
