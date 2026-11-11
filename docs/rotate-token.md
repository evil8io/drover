# rotate-token

`rotate-token` renews the Rancher API token of a drover service user before the token expires. The api-filter and project-sync read that token from a Kubernetes Secret.

The command does one run and then exits. Use a CronJob to start it. In most runs, the command finds a valid token. In such a run, the command does not change the token.

## What a run does

1. The command reads the token from the Secret. It asks Rancher for the expiry time of the token.
2. When the time until the expiry is longer than `--renew-before`, 24 hours by default, the run stops. The command does not make a change.
3. In all other cases, the command logs in to Rancher as the service user. These cases include a token that is absent, rejected, or expired. The command creates a new token with the lifetime of `--ttl`, 48 hours by default. It writes the new token to the Secret.
4. The command deletes the old tokens. Then it logs out.

With `--password-secret`, the command first writes the hash of the password in `--credentials-dir` to the Secret that Rancher reads at a login. To change the password of the service user, replace the password file. The password must have 12 characters or more.

## Safety

- **A restart is not necessary.** The other components read the token file each time that they use the token. A pod sees a changed Secret after a delay, so the command keeps the previous token valid. Set the number of tokens that the command keeps with `--keep`, 2 by default.
- **No second rotation.** When a token is inside `--renew-before` and is younger than 5 minutes, the command does not rotate it. The run fails. In a retry, the command thus cannot delete the token that a pod still reads.
- **Only its own tokens.** The command deletes only the tokens that it created. It finds them by their description, the value of `--description`. The other tokens of the service user stay.
- **HTTPS only.** The login request contains the password. `--rancher-url` must thus be an `https` URL. The command does not follow a redirect.
- **The TTL limit of Rancher.** Rancher shortens a TTL above its own maximum and does not return an error. The command then logs a warning. When the shortened TTL is less than `--renew-before` plus 1 hour, the run fails after the rotation.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | The token is valid, or the rotation is complete. |
| 1 | A step failed, the token was too young for a rotation, or Rancher shortened the TTL too much. |
| 2 | A flag, a credentials file, a CA file, or the telemetry setup is not correct. |

## Requirements

Each item contains a note on whether the [drover chart](https://github.com/evil8io/charts/tree/main/charts/drover) satisfies it.

- Rancher on an `https` URL (`--rancher-url`). With the chart, set the value `rancher.url`.
- For a private certificate, the CA of Rancher (`--rancher-ca-file`). With the chart, a CA file is not mounted. When the command cannot verify the certificate, set the value `rancher.insecureSkipVerify`.
- A local Rancher user as the service user. With the chart, Helm creates this user.
- The name and the password of that user in the files `username` and `password` in `--credentials-dir`. With the chart, external-secrets generates the password. The chart does not contain external-secrets.
- A Kubernetes ServiceAccount for the pod, with `get` and `patch` on the token Secret (`--token-secret`). With `--password-secret`, the ServiceAccount also needs `get` and `patch` on that Secret. Rancher keeps that Secret in the namespace `cattle-local-user-passwords`. With the chart, Helm creates the ServiceAccount and its rights.
- Both Secrets, before the first run. With the chart, Helm creates them.
- A schedule for the command with an interval that is shorter than `--renew-before`. With the chart, a CronJob starts the command at the schedule in `tokenRotation.schedule`, each 10 minutes by default.

## How to run it

`drover rotate-token --help` shows each flag with its default. The command writes JSON logs to stderr. It does not log a token or the password. The steps of a run are in [.claude/docs/rotate-token.md](../.claude/docs/rotate-token.md).
