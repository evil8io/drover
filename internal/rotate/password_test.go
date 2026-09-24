package rotate

import (
	"bytes"
	"crypto/pbkdf2"
	"crypto/sha3"
	"slices"
	"strings"
	"testing"
)

// withPasswordSecret points the run at the password Secret of the fake API.
func withPasswordSecret(h *harness) {
	h.cfg.PasswordNamespace = testPasswordNamespace
	h.cfg.PasswordSecret = testPasswordSecret
}

func mustDerive(t *testing.T, password string, salt []byte) []byte {
	t.Helper()
	digest, err := pbkdf2.Key(sha3.New512, password, salt, passwordIterations, passwordKeyLength)
	if err != nil {
		t.Fatalf("derive the password hash: %v", err)
	}
	return digest
}

// assertPasswordHash derives the digest again, from the salt of the Secret, and
// compares it with the digest of the Secret.
func assertPasswordHash(t *testing.T, kube *fakeKube, password string) {
	t.Helper()
	salt := []byte(kube.passwordValue("salt"))
	if len(salt) != passwordSaltLength {
		t.Fatalf("the salt is %d bytes, and the test expects %d", len(salt), passwordSaltLength)
	}
	if got, want := kube.passwordValue("password"), string(mustDerive(t, password, salt)); got != want {
		t.Error("the digest of the Secret does not match the password and the salt")
	}
}

func TestRunWritesThePasswordHash(t *testing.T) {
	t.Parallel()
	kube := newFakeKube(t, nil)
	rancher := newFakeRancher(t)
	h := newHarness(t, kube, rancher)
	withPasswordSecret(h)

	if err := h.run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	assertRoutes(t, kube.routes(), []string{
		"GET " + testPasswordPath,
		"PATCH " + testPasswordPath,
		"GET " + testSecretPath,
		"PATCH " + testSecretPath,
	})
	assertRoutes(t, rancher.routes(), rotationRoutes)
	if got := kube.passwordAnnotation(passwordHashAnnotation); got != passwordHashName {
		t.Errorf("the annotation %s is %q, and the test expects %q", passwordHashAnnotation, got, passwordHashName)
	}
	assertPasswordHash(t, kube, testPassword)
	h.assertLog(t, "wrote the password hash", `"step":"password_sync"`, `"outcome":"rotate"`)
	h.assertNoSecret(t)
}

func TestRunKeepsACurrentPasswordHash(t *testing.T) {
	t.Parallel()
	kube := newFakeKube(t, nil)
	salt := bytes.Repeat([]byte{7}, passwordSaltLength)
	kube.password.data["salt"] = string(salt)
	kube.password.data["password"] = string(mustDerive(t, testPassword, salt))
	rancher := newFakeRancher(t)
	h := newHarness(t, kube, rancher)
	withPasswordSecret(h)

	if err := h.run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	assertRoutes(t, kube.routes(), []string{
		"GET " + testPasswordPath,
		"GET " + testSecretPath,
		"PATCH " + testSecretPath,
	})
	if got := kube.passwordValue("salt"); got != string(salt) {
		t.Error("the salt of the Secret changed")
	}
	h.assertLog(t, "the password hash is current", `"outcome":"valid"`)
	h.assertNoSecret(t)
}

func TestRunReplacesTheHashOfAnotherPassword(t *testing.T) {
	t.Parallel()
	kube := newFakeKube(t, nil)
	salt := bytes.Repeat([]byte{3}, passwordSaltLength)
	kube.password.data["salt"] = string(salt)
	kube.password.data["password"] = string(mustDerive(t, "another-password", salt))
	rancher := newFakeRancher(t)
	h := newHarness(t, kube, rancher)
	withPasswordSecret(h)

	if err := h.run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if !slices.Contains(kube.routes(), "PATCH "+testPasswordPath) {
		t.Fatalf("the calls are %v, and the test expects a patch of the password Secret", kube.routes())
	}
	if got := kube.passwordValue("salt"); got == string(salt) {
		t.Error("the Secret still has the old salt")
	}
	assertPasswordHash(t, kube, testPassword)
	h.assertNoSecret(t)
}

func TestRunMissingPasswordSecret(t *testing.T) {
	t.Parallel()
	kube := newFakeKube(t, nil)
	kube.password.missing = true
	rancher := newFakeRancher(t)
	h := newHarness(t, kube, rancher)
	withPasswordSecret(h)

	err := h.run()
	if err == nil {
		t.Fatal("Run returned no error")
	}
	if !strings.Contains(err.Error(), "the Secret "+testPasswordNamespace+"/"+testPasswordSecret+" does not exist") {
		t.Errorf("the error is %q, and the test expects the absent Secret", err)
	}
	assertRoutes(t, kube.routes(), []string{
		"GET " + testPasswordPath,
		"GET " + testSecretPath,
		"PATCH " + testSecretPath,
	})
	assertRoutes(t, rancher.routes(), rotationRoutes)
}

func TestRunContinuesAfterAPasswordSyncFailure(t *testing.T) {
	t.Parallel()
	kube := newFakeKube(t, nil)
	kube.password.missing = true
	rancher := newFakeRancher(t)
	h := newHarness(t, kube, rancher)
	withPasswordSecret(h)

	err := h.run()
	if err == nil {
		t.Fatal("Run returned no error")
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("the error is %q, and the test expects the absent Secret", err)
	}

	assertRoutes(t, rancher.routes(), rotationRoutes)
	if kube.value(testKey) == "" {
		t.Error("the Secret has no token, and the test expects the token rotation to have run")
	}
	h.assertLog(t, `"step":"password_sync"`, `"outcome":"failed"`,
		"the password sync failed, and the run continues with the token")
	h.assertNoSecret(t)
}

func TestNewRejectsAPartialPasswordSecret(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		namespace string
		secret    string
	}{
		{"only the namespace", testPasswordNamespace, ""},
		{"only the name", "", testPasswordSecret},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			kube := newFakeKube(t, nil)
			rancher := newFakeRancher(t)
			h := newHarness(t, kube, rancher)
			h.cfg.PasswordNamespace = test.namespace
			h.cfg.PasswordSecret = test.secret

			err := h.run()
			if err == nil || !strings.Contains(err.Error(), "the password Secret needs") {
				t.Fatalf("Run = %v, and the test expects a password Secret error", err)
			}
		})
	}
}
