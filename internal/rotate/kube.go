package rotate

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
)

const mergePatchType = "application/merge-patch+json"

func (r *rotator) secretRef() string {
	return r.cfg.Namespace + "/" + r.cfg.Secret
}

func (r *rotator) secretPath() string {
	return "/api/v1/namespaces/" + url.PathEscape(r.cfg.Namespace) + "/secrets/" + url.PathEscape(r.cfg.Secret)
}

func (r *rotator) kubeDo(ctx context.Context, method, path, contentType string, body any) (int, []byte, error) {
	bearer, err := r.kubeToken()
	if err != nil {
		return 0, nil, err
	}
	data, err := marshal(body)
	if err != nil {
		return 0, nil, err
	}
	return r.do(ctx, call{
		client:      r.cfg.KubeClient,
		method:      method,
		url:         r.cfg.Kube.String() + path,
		bearer:      bearer,
		contentType: contentType,
		body:        data,
	})
}

func (r *rotator) kubeToken() (string, error) {
	data, err := os.ReadFile(r.cfg.KubeTokenFile)
	if err != nil {
		return "", fmt.Errorf("read the ServiceAccount token: %w", err)
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", fmt.Errorf("the ServiceAccount token file %s is empty", r.cfg.KubeTokenFile)
	}
	return token, nil
}

// secretToken returns the token in the Secret. The result is empty when the key
// is absent or empty.
func (r *rotator) secretToken(ctx context.Context) (string, error) {
	status, data, err := r.kubeDo(ctx, http.MethodGet, r.secretPath(), "", nil)
	if err != nil {
		return "", fmt.Errorf("read the Secret %s: %w", r.secretRef(), err)
	}
	switch status {
	case http.StatusOK:
	case http.StatusNotFound:
		return "", fmt.Errorf("the Secret %s does not exist, and the install of drover creates it", r.secretRef())
	default:
		return "", fmt.Errorf("read the Secret %s: status %d", r.secretRef(), status)
	}

	var secret struct {
		Data map[string]string `json:"data"`
	}
	if err := json.Unmarshal(data, &secret); err != nil {
		return "", fmt.Errorf("decode the Secret %s: %w", r.secretRef(), err)
	}

	token := ""
	if encoded := secret.Data[r.cfg.Key]; encoded != "" {
		raw, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return "", fmt.Errorf("decode the key %s of the Secret %s: %w", r.cfg.Key, r.secretRef(), err)
		}
		token = strings.TrimSpace(string(raw))
	}

	outcome := "empty"
	if token != "" {
		outcome = "present"
	}
	r.logger.InfoContext(ctx, "read the token Secret", "step", stepSecretGet, "outcome", outcome, "secret", r.secretRef())
	return token, nil
}

func (r *rotator) patchSecret(ctx context.Context, token string) error {
	body := map[string]map[string]string{"stringData": {r.cfg.Key: token}}
	status, _, err := r.kubeDo(ctx, http.MethodPatch, r.secretPath(), mergePatchType, body)
	if err != nil {
		return fmt.Errorf("patch the Secret %s: %w", r.secretRef(), err)
	}
	if status != http.StatusOK {
		return fmt.Errorf("patch the Secret %s: status %d", r.secretRef(), status)
	}
	r.logger.InfoContext(ctx, "patched the token Secret", "step", stepSecretPatch, "outcome", outcomeOK, "secret", r.secretRef())
	return nil
}
