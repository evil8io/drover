# rotate-token

`rotate-token` keeps the Rancher API token of a drover service user fresh. The api-filter and project-sync read that token from a Kubernetes Secret; this command replaces it before it expires.

The command runs once and exits, so it is meant for a CronJob. Most runs find a valid token and do nothing.

## What a run does

1. Read the token from the Secret and ask Rancher when it expires.
2. If it expires later than `--renew-before` (24 hours by default), stop. Nothing changes.
3. Otherwise, or when the token is missing, rejected, or expired, log in to Rancher as the service user, create a new token with a lifetime of `--ttl` (48 hours by default), and write it to the Secret.
4. Delete old tokens, and log out.

With `--password-secret`, a run first writes the hash of the password from `--credentials-dir` into the Secret that Rancher reads at login. Replacing the password file is then enough to rotate the password as well. The password needs at least 12 characters.

## Safety

- **No restart needed.** The other components read the token file on every use. The run keeps the previous token valid (`--keep`, 2 by default), because a pod sees a changed Secret with a delay.
- **No double rotation.** A token that is younger than 5 minutes and already due for renewal is not rotated again. The run fails instead, so a retry cannot delete the token a pod is still reading.
- **Only its own tokens.** The run deletes only tokens that it created, recognised by their description (`--description`). Other tokens of the service user stay.
- **HTTPS only.** The password goes into the login request, so `--rancher-url` must be `https`, and the command does not follow redirects.
- **TTL caps.** Rancher silently shortens a TTL above its own maximum. The run logs a warning, and fails after the rotation when the shortened TTL would make the token rotate again within the hour.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | The token is valid, or the rotation succeeded. |
| 1 | A step failed, the token was too young to rotate, or Rancher shortened the TTL too far. |
| 2 | A flag, a credentials file, a CA file, or the telemetry setup is wrong. |

## Requirements

- Rancher reachable on an `https` URL (`--rancher-url`), and its CA for a private certificate (`--rancher-ca-file`).
- A local Rancher user for the service user, with its `username` and `password` as files in `--credentials-dir`.
- A Kubernetes ServiceAccount for the pod with `get` and `patch` on the token Secret (`--token-secret`). With `--password-secret`, also `get` and `patch` on that Secret, which Rancher keeps in the namespace `cattle-local-user-passwords`. Both Secrets must exist before the first run.
- Something that runs the command on a schedule shorter than `--renew-before`, usually a CronJob.

The [drover chart](https://github.com/evil8io/charts/tree/main/charts/drover) creates the users, the Secrets, the RBAC, and the CronJob. It needs external-secrets to generate the passwords.

`drover rotate-token --help` lists every flag with its default. The command writes JSON logs to stderr, and never logs a token or the password. The details of each step are in [.claude/docs/rotate-token.md](../.claude/docs/rotate-token.md).
