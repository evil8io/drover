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

// tokenName returns the name part of a token value. It returns an empty string
// for a malformed value.
func tokenName(value string) string {
	name, _, found := strings.Cut(value, ":")
	if !found {
		return ""
	}
	return name
}

// tokenIsValid reports whether the token in the Secret lasts past the renew
// window. A token that Rancher rejects is not valid.
func (r *rotator) tokenIsValid(ctx context.Context, token string) (_ bool, err error) {
	ctx, end := r.step(ctx, stepTokenCheck)
	var outcome string
	defer func() { end(outcome, err) }()

	name := tokenName(token)
	if name == "" {
		outcome = r.logRotate(ctx, "malformed")
		return false, nil
	}

	status, data, err := r.rancherDo(ctx, http.MethodGet, tokensPath+"/"+url.PathEscape(name), token, nil)
	if err != nil {
		return false, fmt.Errorf("read the token %s: %w", name, err)
	}
	switch status {
	case http.StatusOK:
	case http.StatusUnauthorized, http.StatusNotFound:
		outcome = r.logRotate(ctx, "rejected")
		return false, nil
	default:
		return false, fmt.Errorf("read the token %s: status %d", name, status)
	}

	var item tokenItem
	if unmarshalErr := json.Unmarshal(data, &item); unmarshalErr != nil {
		return false, fmt.Errorf("decode the token %s: %w", name, unmarshalErr)
	}
	if item.Expired {
		outcome = r.logRotate(ctx, "expired")
		return false, nil
	}

	at, forever, expiryErr := item.expiry()
	if expiryErr != nil {
		return false, expiryErr
	}
	if !forever && !at.After(r.now().Add(r.cfg.RenewBefore)) {
		r.logger.InfoContext(ctx, "the token expires inside the renew window",
			"step", stepTokenCheck, "outcome", outcomeRotate, "reason", "window",
			"token_name", name, "expires_at", at.UTC().Format(time.RFC3339))
		outcome = outcomeRotate
		return false, nil
	}

	expiresAt := ""
	if !forever {
		expiresAt = at.UTC().Format(time.RFC3339)
	}
	r.logger.InfoContext(ctx, "token is valid",
		"step", stepTokenCheck, "outcome", outcomeValid, "token_name", name, "expires_at", expiresAt)
	outcome = outcomeValid
	return true, nil
}

// logRotate writes the line for a token that needs a rotation, with its
// reason. It returns the outcome for the step metric.
func (r *rotator) logRotate(ctx context.Context, reason string) string {
	r.logger.InfoContext(ctx, "the token needs a rotation",
		"step", stepTokenCheck, "outcome", outcomeRotate, "reason", reason)
	return outcomeRotate
}

// login starts a session as the service user. Rancher ignores the TTL of a login
// token, so the session ends after auth-user-session-ttl-minutes.
func (r *rotator) login(ctx context.Context) (_ session, err error) {
	ctx, end := r.step(ctx, stepLogin)
	defer func() { end(outcomeOK, err) }()

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
	if unmarshalErr := json.Unmarshal(data, &answer); unmarshalErr != nil {
		return session{}, fmt.Errorf("decode the login answer: %w", unmarshalErr)
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
func (r *rotator) createToken(ctx context.Context, s session) (_ createdToken, err error) {
	ctx, end := r.step(ctx, stepTokenCreate)
	defer func() { end(outcomeOK, err) }()

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
	if unmarshalErr := json.Unmarshal(data, &item); unmarshalErr != nil {
		return createdToken{}, fmt.Errorf("decode the new token: %w", unmarshalErr)
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

// prune deletes the tokens of the service user that this command created before.
// It keeps the new token, the token named secretName, and the newest other
// tokens up to Keep in total. It also deletes a login token of an earlier run.
func (r *rotator) prune(ctx context.Context, created createdToken, secretName string, s session) (err error) {
	ctx, end := r.step(ctx, stepTokenPrune)
	outcome := outcomeOK
	defer func() { end(outcome, err) }()

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
	if unmarshalErr := json.Unmarshal(data, &collection); unmarshalErr != nil {
		return fmt.Errorf("decode the token list: %w", unmarshalErr)
	}

	mine, sessions := r.selectTokens(collection.Data, s)
	// The new token and the token of the Secret stay, whatever their position
	// in the list. A tie on the creation time, or an unparsed time, must never
	// delete the new token. A pod reads the token of the Secret until the
	// kubelet updates the mounted Secret.
	protected := 1
	secretKept := ""
	mine = slices.DeleteFunc(mine, func(item tokenItem) bool {
		name := item.itemName()
		switch {
		case name == created.name:
			return true
		case secretName != "" && name == secretName:
			protected++
			secretKept = name
			return true
		}
		return false
	})
	keep := min(len(mine), max(0, r.cfg.Keep-protected))
	var errs []error
	deleted := 0
	for _, item := range slices.Concat(mine[keep:], sessions) {
		if delErr := r.deleteToken(ctx, created.value, item.itemName()); delErr != nil {
			errs = append(errs, delErr)
			continue
		}
		deleted++
	}

	level := slog.LevelInfo
	if len(errs) > 0 {
		outcome, level = outcomeFailed, slog.LevelWarn
	}
	r.logger.Log(ctx, level, "deleted the old tokens", "step", stepTokenPrune, "outcome", outcome,
		"new_token", created.name, "secret_token", secretKept, "found", len(mine)+protected,
		"kept", keep+protected, "sessions", len(sessions), "deleted", deleted, "failed", len(errs))
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
	ctx, end := r.step(ctx, stepLogout)
	var err error
	outcome := outcomeOK
	defer func() { end(outcome, err) }()

	status, _, doErr := r.rancherDo(ctx, http.MethodPost, logoutPath, s.token, map[string]any{})
	switch {
	case doErr != nil:
		err = doErr
		outcome = outcomeFailed
		r.logger.WarnContext(ctx, "the logout failed", "step", stepLogout, "outcome", outcomeFailed, "error", err.Error())
	case status != http.StatusOK && status != http.StatusNoContent && status != http.StatusCreated:
		err = fmt.Errorf("the logout failed: status %d", status)
		outcome = outcomeFailed
		r.logger.WarnContext(ctx, "the logout failed", "step", stepLogout, "outcome", outcomeFailed, "status", status)
	default:
		r.logger.InfoContext(ctx, "logged out", "step", stepLogout, "outcome", outcomeOK)
	}
}

func (r *rotator) loginDescription() string {
	return r.cfg.Description + " login"
}
