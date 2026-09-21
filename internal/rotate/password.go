package rotate

import (
	"context"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha3"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

// Rancher reads the password of a local user from a Secret. These constants are
// the format that Rancher expects.
const (
	passwordHashAnnotation = "cattle.io/password-hash"
	passwordHashName       = "pbkdf2sha3512"
	passwordIterations     = 210000
	passwordKeyLength      = 32
	passwordSaltLength     = 32
)

func (r *rotator) passwordRef() string {
	return r.cfg.PasswordNamespace + "/" + r.cfg.PasswordSecret
}

func (r *rotator) passwordPath() string {
	return "/api/v1/namespaces/" + url.PathEscape(r.cfg.PasswordNamespace) +
		"/secrets/" + url.PathEscape(r.cfg.PasswordSecret)
}

// syncPassword writes the hash of the password into the Secret that Rancher
// reads at a local login. A Secret that already has the hash of this password
// stays unchanged. No log line and no error contains the password, the salt, or
// the digest.
func (r *rotator) syncPassword(ctx context.Context) (err error) {
	ctx, end := r.step(ctx, stepPasswordSync)
	var outcome string
	defer func() { end(outcome, err) }()

	salt, digest, err := r.passwordHash(ctx)
	if err != nil {
		return err
	}
	if len(salt) > 0 && len(digest) > 0 {
		var current []byte
		if current, err = r.derivePassword(salt); err != nil {
			return err
		}
		if subtle.ConstantTimeCompare(current, digest) == 1 {
			outcome = outcomeValid
			r.logger.InfoContext(ctx, "the password hash is current",
				"step", stepPasswordSync, "outcome", outcome, "secret", r.passwordRef())
			return nil
		}
	}

	salt = make([]byte, passwordSaltLength)
	if _, err = rand.Read(salt); err != nil {
		return fmt.Errorf("create the salt for the Secret %s: %w", r.passwordRef(), err)
	}
	if digest, err = r.derivePassword(salt); err != nil {
		return err
	}
	if err = r.patchPassword(ctx, salt, digest); err != nil {
		return err
	}

	outcome = outcomeRotate
	r.logger.InfoContext(ctx, "wrote the password hash",
		"step", stepPasswordSync, "outcome", outcome, "secret", r.passwordRef())
	return nil
}

// passwordHash returns the salt and the digest of the Secret. Both are empty
// when the Secret has no hash yet.
func (r *rotator) passwordHash(ctx context.Context) (salt, digest []byte, err error) {
	status, data, err := r.kubeDo(ctx, http.MethodGet, r.passwordPath(), "", nil)
	if err != nil {
		return nil, nil, fmt.Errorf("read the Secret %s: %w", r.passwordRef(), err)
	}
	switch status {
	case http.StatusOK:
	case http.StatusNotFound:
		return nil, nil, fmt.Errorf("the Secret %s does not exist, and the install of drover creates it", r.passwordRef())
	default:
		return nil, nil, fmt.Errorf("read the Secret %s: status %d", r.passwordRef(), status)
	}

	var secret struct {
		Data map[string]string `json:"data"`
	}
	if err := json.Unmarshal(data, &secret); err != nil {
		return nil, nil, fmt.Errorf("decode the Secret %s: %w", r.passwordRef(), err)
	}
	if salt, err = r.decodePasswordKey(secret.Data, "salt"); err != nil {
		return nil, nil, err
	}
	if digest, err = r.decodePasswordKey(secret.Data, "password"); err != nil {
		return nil, nil, err
	}
	return salt, digest, nil
}

func (r *rotator) decodePasswordKey(data map[string]string, key string) ([]byte, error) {
	encoded := data[key]
	if encoded == "" {
		return nil, nil
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode the key %s of the Secret %s: %w", key, r.passwordRef(), err)
	}
	return raw, nil
}

func (r *rotator) derivePassword(salt []byte) ([]byte, error) {
	digest, err := pbkdf2.Key(sha3.New512, r.cfg.Password, salt, passwordIterations, passwordKeyLength)
	if err != nil {
		return nil, fmt.Errorf("derive the password hash for the Secret %s: %w", r.passwordRef(), err)
	}
	return digest, nil
}

func (r *rotator) patchPassword(ctx context.Context, salt, digest []byte) error {
	body := map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]string{passwordHashAnnotation: passwordHashName},
		},
		"data": map[string]string{
			"password": base64.StdEncoding.EncodeToString(digest),
			"salt":     base64.StdEncoding.EncodeToString(salt),
		},
	}
	status, _, err := r.kubeDo(ctx, http.MethodPatch, r.passwordPath(), mergePatchType, body)
	if err != nil {
		return fmt.Errorf("patch the Secret %s: %w", r.passwordRef(), err)
	}
	if status != http.StatusOK {
		return fmt.Errorf("patch the Secret %s: status %d", r.passwordRef(), status)
	}
	return nil
}
