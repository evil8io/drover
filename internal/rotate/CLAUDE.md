# rotate-token

The document for human readers is `docs/rotate-token.md`. Read it first, and update it together with a behaviour change. Do not repeat its content here.

These facts about Rancher tokens were verified against Rancher 2.14.5:

- Rancher ignores `ttl` on `POST /v3-public/localProviders/local?action=login`. The session length is the value of `auth-user-session-ttl-minutes`. A token with a chosen lifetime needs `POST /v3/tokens` with `ttl` in milliseconds.
- With `responseType: cookie` on the login, Rancher returns an empty body. Use the token response.
- `expiresAt` is empty right after Rancher creates the token. Derive the expiry from `ttl` and the creation time.
- Rancher answers 400 to a delete request for the current session token. End the session with `?action=logout`.
- Rancher answers `400 Use HTTPS` to a login over plain HTTP.

Follow these rules:

- Keep the exit code 1 for a TTL that Rancher reduces below `--renew-before` plus 1 hour. The Job then ends as failed and shows the setting error. An exit code 0 shows the error only in a log line.
- Never delete the token that the run read from the Secret. This rule also applies when its description is `<description> login`, and when newer tokens exist. A token of an earlier run with a failed patch is newer than the mounted token.
- Fail the run for a token of the Secret that expires inside `--renew-before` and is younger than 5 minutes. A Job retry must not end as successful while the TTL setting is wrong. The kubelet sync period of a mounted Secret is 60 s by default.
- In the password step, `rotate-token` writes the password hash. The code of this step is in `password.go`.
  - Compare the hashes with the stored salt. Do not rewrite a current hash, because a rewrite changes nothing and only adds writes to the Secret.
  - A Rancher password changes only through this hash write or through the admin `setpassword` action. An external secret store cannot set a Rancher password.
  - Rancher does not do the length check of the `setpassword` action when `rotate-token` writes the hash. So `rotate-token` does the check. Rancher counts the length with `utf8.RuneCountInString` (verified against Rancher 2.14.5).
- Do not end the run on a failed password step. A permanent failure of the hash write would otherwise leave every component without a Rancher token after the TTL.
- Never follow a redirect. A client that follows a 307 or a 308 answer sends the login body, with the password, again to the target of the redirect.
- Reject `--rancher-ca-file` together with `--rancher-insecure-skip-verify`. Go ignores the CA pool when the verification is off, so the CA file has no effect.
