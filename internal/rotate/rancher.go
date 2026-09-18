package rotate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

const (
	loginPath  = "/v3-public/localProviders/local?action=login"
	tokensPath = "/v3/tokens"
	logoutPath = "/v3/tokens?action=logout"
)

// tokenItem is the part of a Rancher token that this command reads. A list and a
// get answer never contain token.
type tokenItem struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Created     string `json:"created"`
	ExpiresAt   string `json:"expiresAt"`
	TTL         int64  `json:"ttl"`
	IsDerived   bool   `json:"isDerived"`
	Current     bool   `json:"current"`
	Expired     bool   `json:"expired"`
	Token       string `json:"token"`
}

func (t tokenItem) itemName() string {
	if t.Name != "" {
		return t.Name
	}
	return t.ID
}

func (t tokenItem) createdAt() time.Time {
	at, err := time.Parse(time.RFC3339, t.Created)
	if err != nil {
		return time.Time{}
	}
	return at
}

// expiry returns the end of the lifetime of the token. A token with the TTL zero
// never expires, and forever is then true.
func (t tokenItem) expiry() (at time.Time, forever bool, err error) {
	if t.ExpiresAt != "" {
		at, err := time.Parse(time.RFC3339, t.ExpiresAt)
		if err != nil {
			return time.Time{}, false, fmt.Errorf("parse expiresAt of the token %s: %w", t.itemName(), err)
		}
		return at, false, nil
	}
	if t.TTL == 0 {
		return time.Time{}, true, nil
	}
	created, err := time.Parse(time.RFC3339, t.Created)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("parse created of the token %s: %w", t.itemName(), err)
	}
	return created.Add(time.Duration(t.TTL) * time.Millisecond), false, nil
}

type session struct {
	token string
	id    string
}

type createdToken struct {
	value string
	name  string
}

// tokenIsValid reports whether the token in the Secret lasts past the renew
// window. A token that Rancher rejects is not valid.
func (r *rotator) tokenIsValid(ctx context.Context, token string) (bool, error) {
	name, _, found := strings.Cut(token, ":")
	if !found || name == "" {
		r.logRotate(ctx, "malformed")
		return false, nil
	}

	status, data, err := r.rancherDo(ctx, http.MethodGet, tokensPath+"/"+url.PathEscape(name), token, nil)
	if err != nil {
		return false, fmt.Errorf("read the token %s: %w", name, err)
	}
	switch status {
	case http.StatusOK:
	case http.StatusUnauthorized, http.StatusNotFound:
		r.logRotate(ctx, "rejected")
		return false, nil
	default:
		return false, fmt.Errorf("read the token %s: status %d", name, status)
	}

	var item tokenItem
	if err := json.Unmarshal(data, &item); err != nil {
		return false, fmt.Errorf("decode the token %s: %w", name, err)
	}
	if item.Expired {
		r.logRotate(ctx, "expired")
		return false, nil
	}

	at, forever, err := item.expiry()
	if err != nil {
		return false, err
	}
	if !forever && !at.After(r.now().Add(r.cfg.RenewBefore)) {
		r.logger.InfoContext(ctx, "the token expires inside the renew window",
			"step", stepTokenCheck, "outcome", outcomeRotate, "reason", "window",
			"token_name", name, "expires_at", at.UTC().Format(time.RFC3339))
		return false, nil
	}

	expiresAt := ""
	if !forever {
		expiresAt = at.UTC().Format(time.RFC3339)
	}
	r.logger.InfoContext(ctx, "token is valid",
		"step", stepTokenCheck, "outcome", outcomeValid, "token_name", name, "expires_at", expiresAt)
	return true, nil
}

func (r *rotator) logRotate(ctx context.Context, reason string) {
	r.logger.InfoContext(ctx, "the token needs a rotation",
		"step", stepTokenCheck, "outcome", outcomeRotate, "reason", reason)
}

// login starts a session as the service user. Rancher ignores the TTL of a login
// token, so the session ends after auth-user-session-ttl-minutes.
func (r *rotator) login(ctx context.Context) (session, error) {
	body := map[string]string{
		"username":     r.cfg.Username,
		"password":     r.cfg.Password,
		"responseType": "json",
		"description":  r.loginDescription(),
	}
	status, data, err := r.rancherDo(ctx, http.MethodPost, loginPath, "", body)
	if err != nil {
		return session{}, fmt.Errorf("log in as %s: %w", r.cfg.Username, err)
	}
	switch status {
	case http.StatusCreated:
	case http.StatusUnauthorized:
		return session{}, fmt.Errorf("log in as %s: the user or the password is wrong", r.cfg.Username)
	case http.StatusForbidden:
		return session{}, fmt.Errorf("log in as %s: the user is disabled", r.cfg.Username)
	default:
		return session{}, fmt.Errorf("log in as %s: status %d", r.cfg.Username, status)
	}

	var answer struct {
		Token string `json:"token"`
		ID    string `json:"id"`
	}
	if err := json.Unmarshal(data, &answer); err != nil {
		return session{}, fmt.Errorf("decode the login answer: %w", err)
	}
	if answer.Token == "" || answer.ID == "" {
		return session{}, errors.New("the login answer has no token")
	}
	r.logger.InfoContext(ctx, "logged in", "step", stepLogin, "outcome", outcomeOK,
		"user", r.cfg.Username, "session_name", answer.ID)
	return session{token: answer.Token, id: answer.ID}, nil
}

// createToken derives the API token from the session. Rancher reduces a TTL
// above auth-token-max-ttl-minutes without an error.
func (r *rotator) createToken(ctx context.Context, s session) (createdToken, error) {
	want := r.cfg.TTL.Milliseconds()
	body := map[string]any{"type": "token", "ttl": want, "description": r.cfg.Description}
	status, data, err := r.rancherDo(ctx, http.MethodPost, tokensPath, s.token, body)
	if err != nil {
		return createdToken{}, fmt.Errorf("create a token: %w", err)
	}
	if status != http.StatusCreated {
		return createdToken{}, fmt.Errorf("create a token: status %d", status)
	}

	var item tokenItem
	if err := json.Unmarshal(data, &item); err != nil {
		return createdToken{}, fmt.Errorf("decode the new token: %w", err)
	}
	name := item.ID
	if name == "" {
		name = item.Name
	}
	if item.Token == "" || name == "" {
		return createdToken{}, errors.New("the answer of the token create has no token")
	}
	if item.TTL != want {
		r.logger.WarnContext(ctx, "rancher clamped the ttl", "step", stepTokenCreate, "outcome", outcomeClamped,
			"token_name", name, "requested_ttl_ms", want, "granted_ttl_ms", item.TTL)
	}
	r.logger.InfoContext(ctx, "created a token", "step", stepTokenCreate, "outcome", outcomeOK,
		"token_name", name, "ttl_ms", item.TTL)
	return createdToken{value: item.Token, name: name}, nil
}

// prune deletes the tokens of the service user that this command created before,
// except the newest Keep tokens. It also deletes a login token of an earlier run.
func (r *rotator) prune(ctx context.Context, created createdToken, s session) error {
	status, data, err := r.rancherDo(ctx, http.MethodGet, tokensPath, created.value, nil)
	if err != nil {
		return fmt.Errorf("list the tokens: %w", err)
	}
	if status != http.StatusOK {
		return fmt.Errorf("list the tokens: status %d", status)
	}
	var collection struct {
		Data []tokenItem `json:"data"`
	}
	if err := json.Unmarshal(data, &collection); err != nil {
		return fmt.Errorf("decode the token list: %w", err)
	}

	mine, sessions := r.selectTokens(collection.Data, s)
	keep := min(len(mine), r.cfg.Keep)
	var errs []error
	deleted := 0
	for _, item := range slices.Concat(mine[keep:], sessions) {
		if err := r.deleteToken(ctx, created.value, item.itemName()); err != nil {
			errs = append(errs, err)
			continue
		}
		deleted++
	}

	outcome := outcomeOK
	level := slog.LevelInfo
	if len(errs) > 0 {
		outcome, level = outcomeFailed, slog.LevelWarn
	}
	r.logger.Log(ctx, level, "deleted the old tokens", "step", stepTokenPrune, "outcome", outcome,
		"new_token", created.name, "found", len(mine), "kept", keep,
		"sessions", len(sessions), "deleted", deleted, "failed", len(errs))
	return errors.Join(errs...)
}

// selectTokens returns the tokens with the description of this command, newest
// first, and the login tokens of earlier runs. A token with another description
// is never in the result.
func (r *rotator) selectTokens(items []tokenItem, s session) (mine, sessions []tokenItem) {
	for _, item := range items {
		switch item.Description {
		case r.cfg.Description:
			mine = append(mine, item)
		case r.loginDescription():
			if item.Current || item.itemName() == s.id {
				continue
			}
			sessions = append(sessions, item)
		}
	}
	slices.SortStableFunc(mine, func(a, b tokenItem) int {
		if order := b.createdAt().Compare(a.createdAt()); order != 0 {
			return order
		}
		return strings.Compare(a.itemName(), b.itemName())
	})
	return mine, sessions
}

func (r *rotator) deleteToken(ctx context.Context, bearer, name string) error {
	if name == "" {
		return errors.New("a token in the list has no name")
	}
	status, _, err := r.rancherDo(ctx, http.MethodDelete, tokensPath+"/"+url.PathEscape(name), bearer, nil)
	if err != nil {
		return fmt.Errorf("delete the token %s: %w", name, err)
	}
	switch status {
	case http.StatusNoContent, http.StatusOK, http.StatusNotFound:
		return nil
	}
	return fmt.Errorf("delete the token %s: status %d", name, status)
}

// logout ends the session. Rancher refuses a delete of the current session
// token, so the logout action is the only way to remove it.
func (r *rotator) logout(ctx context.Context, s session) {
	status, _, err := r.rancherDo(ctx, http.MethodPost, logoutPath, s.token, map[string]any{})
	switch {
	case err != nil:
		r.logger.WarnContext(ctx, "the logout failed", "step", stepLogout, "outcome", outcomeFailed, "error", err.Error())
	case status != http.StatusOK && status != http.StatusNoContent && status != http.StatusCreated:
		r.logger.WarnContext(ctx, "the logout failed", "step", stepLogout, "outcome", outcomeFailed, "status", status)
	default:
		r.logger.InfoContext(ctx, "logged out", "step", stepLogout, "outcome", outcomeOK)
	}
}

func (r *rotator) loginDescription() string {
	return r.cfg.Description + " login"
}
