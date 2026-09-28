# rotate-token

The document for human readers is `docs/rotate-token.md`. When you change the behaviour of `rotate-token`, update that document at the same time.

These facts about Rancher tokens were verified against Rancher 2.14.5:

- Rancher ignores `ttl` on `POST /v3-public/localProviders/local?action=login`. The session length is the value of `auth-user-session-ttl-minutes`. A token with a chosen lifetime needs `POST /v3/tokens` with `ttl` in milliseconds, and Rancher reduces a value above `auth-token-max-ttl-minutes` without an error. Warn when the returned TTL differs from the requested TTL. Fail the run after the rotation when the granted TTL is not longer than `--renew-before`. The reason is that the new token is inside the renew window at once.
- With `responseType: cookie` on the login, Rancher returns an empty body. Use the token response.
- `expiresAt` is empty right after Rancher creates the token. Derive the expiry from `ttl` and the creation time.
- Rancher answers 400 to a delete request for the current session token. End the session with `?action=logout`.
- Rancher answers `400 Use HTTPS` to a login over plain HTTP.

Follow these rules:

- `rotate-token` selects the tokens to delete by their description. Never delete a token with a different description, because a kubeconfig token of the service user has a different description.
- Keep `--keep` at 2 or more. The reason is that a pod reads a mounted Secret with a delay after the patch. The old token must work during that delay.
- Never delete the token that the run read from the Secret, even when newer tokens with the description exist. A token of an earlier run with a failed patch is newer than the mounted token. Also, a pod reads the mounted Secret with a delay after the patch.
- The run is check-and-renew. For this reason, a CronJob that runs every 10 minutes is cheap, and a missed run is harmless. Do not change the run to a fixed rotation on a schedule.
- Read the ServiceAccount token for each request, because the kubelet replaces the file.
- In the password step, `rotate-token` writes the password hash. The code of this step is in `password.go`.
  - Rancher reads the Secret `cattle-local-user-passwords/<User name>` at each local login. The hash format is PBKDF2-HMAC-SHA3-512 with 210000 iterations, a 32-byte salt, and a 32-byte digest. The annotation `cattle.io/password-hash: pbkdf2sha3512` is on the Secret.
  - Use the stored salt when you compare the hashes. Do not rewrite a current hash. A rewrite changes nothing, and it only adds unnecessary writes to the Secret.
  - A Rancher password changes only through this hash write or through the admin `setpassword` action. An external secret store cannot set a Rancher password.
  - Reject a password shorter than 12 characters when the password step is on. The reason is that Rancher does not do the check of the `setpassword` action when `rotate-token` writes the hash. Rancher counts the length with `utf8.RuneCountInString`. This fact was verified against Rancher 2.14.5.
- Do not end the run on a failed password step. After a failed password step, the token steps still run, and the run exits 1 after them. The reason is that a permanent failure of the hash write otherwise leaves every component without a Rancher token after the TTL.
- Require `https` for the Rancher URL. Never follow a redirect. The reason for these two rules is that the login body contains the password. A client that follows a 307 or 308 response sends the body again to the target of the redirect.
