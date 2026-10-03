package projectsync

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	// maxTrustValue is the byte limit of the trust annotation.
	maxTrustValue = 16384
	// maxTrustStatusValue bounds the status annotation that the service keeps
	// in memory. The status that the service writes is far shorter.
	maxTrustStatusValue = 128 << 10
	// oversizeStatus replaces a status annotation above maxTrustStatusValue.
	// It is not JSON, so it never equals the status that the service writes.
	oversizeStatus = "oversize"
	// maxStatusName bounds the name of a statement in the status, because an
	// invalid name is tenant text of any length.
	maxStatusName = 63

	maxStatementsCap = 100
	minLoginTokenTTL = time.Minute
	maxLoginTokenTTL = 24 * time.Hour
	forbiddenClaim   = "aud"
	trustHashPrefix  = "sha256:"
)

// The reason codes of the trust status. The order of the statement codes is
// the order of the checks.
const (
	reasonInvalidName          = "InvalidName"
	reasonDuplicateName        = "DuplicateName"
	reasonInvalidAuth          = "InvalidAuth"
	reasonUnknownIssuer        = "UnknownIssuer"
	reasonInvalidClaims        = "InvalidClaims"
	reasonMissingRequiredClaim = "MissingRequiredClaim"
	reasonClaimNotAllowed      = "ClaimNotAllowed"
	reasonInvalidARN           = "InvalidARN"
	reasonAccountNotAllowed    = "AccountNotAllowed"
	reasonMethodDisabled       = "MethodDisabled"
	reasonInvalidRole          = "InvalidRole"
	reasonTooManyStatements    = "TooManyStatements"
	reasonWriteFailed          = "WriteFailed"

	reasonTooLarge        = "TooLarge"
	reasonInvalidJSON     = "InvalidJSON"
	reasonInvalidDocument = "InvalidDocument"
)

// trustMessages has the fixed message of each reason. A message never
// contains tenant input, so that the status depends on the document only.
var trustMessages = map[string]string{
	reasonInvalidName:          "the name is not 1 to 32 lowercase letters, digits, and dashes, with a letter or a digit at each end",
	reasonDuplicateName:        "an earlier statement has the same name",
	reasonInvalidAuth:          "the statement does not have exactly one of jwt and aws, or it has an unknown key",
	reasonUnknownIssuer:        "the JWT issuer is not configured",
	reasonInvalidClaims:        "the claims are not an object of non-empty strings or non-empty string lists, or a claim is aud",
	reasonMissingRequiredClaim: "a required claim of the issuer is not bound",
	reasonClaimNotAllowed:      "a bound value is not in the allowed values of its claim",
	reasonInvalidARN:           "the ARN is not the ARN of an IAM role without wildcards",
	reasonAccountNotAllowed:    "the account is not in the allowed accounts",
	reasonMethodDisabled:       "the login method is not enabled",
	reasonInvalidRole:          "the role is not project-owner, project-member, or read-only",
	reasonTooManyStatements:    "the document has more statements than the limit",
	reasonWriteFailed:          "the login role write in OpenBao failed",
	reasonTooLarge:             "the annotation value is larger than 16384 bytes",
	reasonInvalidJSON:          "the annotation value is not valid JSON",
	reasonInvalidDocument:      "the document is not an object with only a statements list of objects",
}

var (
	statementName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,30}[a-z0-9])?$`)
	roleARN       = regexp.MustCompile(`^arn:aws:iam::[0-9]{12}:role/[A-Za-z0-9+=,.@_/-]+$`)
	awsAccountID  = regexp.MustCompile(`^[0-9]{12}$`)
	// issuerName keeps an issuer name usable as one segment of an OpenBao
	// path.
	issuerName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)
)

// trustRules are the platform rules of the trust documents, from the file of
// --trust-file.
type trustRules struct {
	annotation       string
	statusAnnotation string
	maxStatements    int
	audience         string
	loginTokenTTL    string
	issuers          map[string]trustIssuer
	awsEnabled       bool
	awsAccounts      []string
}

// trustIssuer is the rules of one JWT issuer. The first required claim is the
// user claim of every login role of the issuer.
type trustIssuer struct {
	required []string
	allowed  map[string][]string
}

// trustRulesFile is the JSON form of trustRules. Unknown keys are ignored, so
// that the chart can render more keys.
type trustRulesFile struct {
	Annotation       string `json:"annotation"`
	StatusAnnotation string `json:"statusAnnotation"`
	MaxStatements    int    `json:"maxStatements"`
	Audience         string `json:"audience"`
	LoginTokenTTL    string `json:"loginTokenTTL"`
	JWTIssuers       map[string]struct {
		RequiredClaims []string            `json:"requiredClaims"`
		AllowedClaims  map[string][]string `json:"allowedClaims"`
	} `json:"jwtIssuers"`
	AWS struct {
		Enabled         bool     `json:"enabled"`
		AllowedAccounts []string `json:"allowedAccounts"`
	} `json:"aws"`
}

// loadTrustRules reads and checks the rules file at path. reserved are the
// annotation keys that the service copies to a namespace, and the trust keys
// must not be one of them.
func loadTrustRules(path string, reserved []string) (*trustRules, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read the trust rules file: %w", err)
	}
	rules, err := parseTrustRules(data)
	if err != nil {
		return nil, fmt.Errorf("the trust rules file %s: %w", path, err)
	}
	for _, key := range []string{rules.annotation, rules.statusAnnotation} {
		if slices.Contains(reserved, key) {
			return nil, fmt.Errorf("the trust rules file %s: the annotation %q is also a key that the service copies to a namespace", path, key)
		}
	}
	return rules, nil
}

func parseTrustRules(data []byte) (*trustRules, error) {
	var file trustRulesFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	for _, key := range []string{file.Annotation, file.StatusAnnotation} {
		if err := checkKey(key); err != nil {
			return nil, fmt.Errorf("the trust annotation: %w", err)
		}
	}
	if file.Annotation == file.StatusAnnotation {
		return nil, errors.New("annotation and statusAnnotation are the same key")
	}
	if file.MaxStatements < 1 || file.MaxStatements > maxStatementsCap {
		return nil, fmt.Errorf("maxStatements %d is not 1 to %d", file.MaxStatements, maxStatementsCap)
	}
	if file.Audience == "" {
		return nil, errors.New("audience is empty")
	}
	ttl, err := time.ParseDuration(file.LoginTokenTTL)
	if err != nil {
		return nil, fmt.Errorf("loginTokenTTL: %w", err)
	}
	if ttl < minLoginTokenTTL || ttl > maxLoginTokenTTL {
		return nil, fmt.Errorf("loginTokenTTL %s is not %s to %s", ttl, minLoginTokenTTL, maxLoginTokenTTL)
	}

	rules := &trustRules{
		annotation:       file.Annotation,
		statusAnnotation: file.StatusAnnotation,
		maxStatements:    file.MaxStatements,
		audience:         file.Audience,
		loginTokenTTL:    file.LoginTokenTTL,
		issuers:          make(map[string]trustIssuer, len(file.JWTIssuers)),
		awsEnabled:       file.AWS.Enabled,
	}
	for _, name := range slices.Sorted(maps.Keys(file.JWTIssuers)) {
		item := file.JWTIssuers[name]
		if !issuerName.MatchString(name) {
			return nil, fmt.Errorf("the issuer name %q is not a lowercase DNS label", name)
		}
		if len(item.RequiredClaims) == 0 {
			return nil, fmt.Errorf("the issuer %s has no required claim", name)
		}
		for _, claim := range item.RequiredClaims {
			if claim == "" || claim == forbiddenClaim {
				return nil, fmt.Errorf("the issuer %s has the required claim %q, which a statement cannot bind", name, claim)
			}
		}
		for claim, values := range item.AllowedClaims {
			if claim == "" || claim == forbiddenClaim || len(values) == 0 || slices.Contains(values, "") {
				return nil, fmt.Errorf("the allowed claim %q of the issuer %s is not a claim with a list of non-empty values", claim, name)
			}
		}
		rules.issuers[name] = trustIssuer{required: slices.Clone(item.RequiredClaims), allowed: maps.Clone(item.AllowedClaims)}
	}
	for _, account := range file.AWS.AllowedAccounts {
		if !awsAccountID.MatchString(account) {
			return nil, fmt.Errorf("the allowed account %q is not a 12-digit AWS account id", account)
		}
	}
	rules.awsAccounts = slices.Clone(file.AWS.AllowedAccounts)
	return rules, nil
}

// trustValue is the trust annotation of a project. text is empty when the
// value is larger than maxTrustValue, because the parser rejects such a
// value without its text. The zero value is an absent or empty annotation.
type trustValue struct {
	text     string
	hash     string
	tooLarge bool
}

func newTrustValue(value string) trustValue {
	if value == "" {
		return trustValue{}
	}
	sum := sha256.Sum256([]byte(value))
	out := trustValue{hash: trustHashPrefix + hex.EncodeToString(sum[:])}
	if len(value) > maxTrustValue {
		out.tooLarge = true
	} else {
		out.text = value
	}
	return out
}

func (v trustValue) present() bool { return v.hash != "" }

// pruneTrustStatus returns the status annotation that the service keeps of
// value.
func pruneTrustStatus(value string) string {
	if len(value) > maxTrustStatusValue {
		return oversizeStatus
	}
	return value
}

// trustDocument is a parsed trust annotation. reason is the reason of a
// document error, and "" when the statements parsed.
type trustDocument struct {
	reason     string
	statements []trustStatement
}

// valid reports whether the document and every statement are valid.
func (d trustDocument) valid() bool {
	return d.firstReason() == ""
}

// firstReason returns the document reason, or the reason of the first
// invalid statement, or "".
func (d trustDocument) firstReason() string {
	if d.reason != "" {
		return d.reason
	}
	for _, item := range d.statements {
		if item.reason != "" {
			return item.reason
		}
	}
	return ""
}

// trustStatement is one statement of a document. name is the name when it is
// a string. reason is "" for a valid statement, which has jwt or aws, and
// role.
type trustStatement struct {
	name   string
	reason string
	role   string
	jwt    *jwtTrust
	aws    *awsTrust
}

// jwtTrust is the jwt part of a valid statement. A claim value is a string or
// a []string.
type jwtTrust struct {
	issuer string
	claims map[string]any
}

type awsTrust struct {
	arn string
}

// parseTrustValue parses the trust annotation value against rules.
func parseTrustValue(value trustValue, rules *trustRules) trustDocument {
	if value.tooLarge {
		return trustDocument{reason: reasonTooLarge}
	}
	return parseTrust(value.text, rules)
}

// parseTrust parses and checks the trust document value against rules.
func parseTrust(value string, rules *trustRules) trustDocument {
	if len(value) > maxTrustValue {
		return trustDocument{reason: reasonTooLarge}
	}
	if !json.Valid([]byte(value)) {
		return trustDocument{reason: reasonInvalidJSON}
	}
	top, ok := jsonObject([]byte(value))
	if !ok || len(top) != 1 {
		return trustDocument{reason: reasonInvalidDocument}
	}
	items, ok := jsonArray(top["statements"])
	if !ok {
		return trustDocument{reason: reasonInvalidDocument}
	}
	objects := make([]map[string]json.RawMessage, 0, len(items))
	for _, item := range items {
		object, ok := jsonObject(item)
		if !ok {
			return trustDocument{reason: reasonInvalidDocument}
		}
		objects = append(objects, object)
	}

	doc := trustDocument{statements: make([]trustStatement, 0, len(objects))}
	seen := make(map[string]bool, len(objects))
	for index, object := range objects {
		doc.statements = append(doc.statements, checkStatement(index, object, rules, seen))
	}
	return doc
}

// checkStatement checks one statement at index in the order of the reason
// codes. seen has the valid names of the earlier statements.
func checkStatement(index int, object map[string]json.RawMessage, rules *trustRules, seen map[string]bool) trustStatement {
	var item trustStatement
	name, isString := jsonString(object["name"])
	if isString {
		item.name = name
	}
	if !isString || !statementName.MatchString(name) {
		item.reason = reasonInvalidName
		return item
	}
	if seen[name] {
		item.reason = reasonDuplicateName
		return item
	}
	seen[name] = true

	item.reason = item.checkAuth(object, rules)
	if item.reason != "" {
		item.jwt, item.aws = nil, nil
		return item
	}
	role, ok := jsonString(object["role"])
	if _, known := accountRoleOf(role); !ok || !known {
		item.reason = reasonInvalidRole
		item.jwt, item.aws = nil, nil
		return item
	}
	item.role = role
	if index >= rules.maxStatements {
		item.reason = reasonTooManyStatements
		item.jwt, item.aws = nil, nil
	}
	return item
}

// checkAuth checks the keys of the statement and its jwt or aws part, and
// stores that part.
func (item *trustStatement) checkAuth(object map[string]json.RawMessage, rules *trustRules) string {
	for key := range object {
		if !slices.Contains([]string{"name", "jwt", "aws", "role"}, key) {
			return reasonInvalidAuth
		}
	}
	rawJWT, hasJWT := object["jwt"]
	rawAWS, hasAWS := object["aws"]
	if hasJWT == hasAWS {
		return reasonInvalidAuth
	}
	if hasJWT {
		return item.checkJWT(rawJWT, rules)
	}
	return item.checkAWS(rawAWS, rules)
}

func (item *trustStatement) checkJWT(raw json.RawMessage, rules *trustRules) string {
	fields, ok := jsonObject(raw)
	if !ok || !onlyKeys(fields, "issuer", "claims") {
		return reasonInvalidAuth
	}
	name, _ := jsonString(fields["issuer"])
	issuer, ok := rules.issuers[name]
	if !ok {
		return reasonUnknownIssuer
	}
	claims, ok := parseClaims(fields["claims"])
	if !ok {
		return reasonInvalidClaims
	}
	for _, claim := range issuer.required {
		if _, ok := claims[claim]; !ok {
			return reasonMissingRequiredClaim
		}
	}
	for claim, allowed := range issuer.allowed {
		bound, ok := claims[claim]
		if !ok {
			continue
		}
		for _, value := range claimValues(bound) {
			if !slices.Contains(allowed, value) {
				return reasonClaimNotAllowed
			}
		}
	}
	item.jwt = &jwtTrust{issuer: name, claims: claims}
	return ""
}

func (item *trustStatement) checkAWS(raw json.RawMessage, rules *trustRules) string {
	fields, ok := jsonObject(raw)
	if !ok || !onlyKeys(fields, "arn") {
		return reasonInvalidAuth
	}
	arn, ok := jsonString(fields["arn"])
	if !ok || !roleARN.MatchString(arn) || strings.HasSuffix(arn, "/") {
		return reasonInvalidARN
	}
	account := strings.Split(arn, ":")[4]
	if len(rules.awsAccounts) > 0 && !slices.Contains(rules.awsAccounts, account) {
		return reasonAccountNotAllowed
	}
	if !rules.awsEnabled {
		return reasonMethodDisabled
	}
	item.aws = &awsTrust{arn: arn}
	return ""
}

// parseClaims returns the claims object of a statement, with a string or a
// []string per claim, and false when it breaks a rule.
func parseClaims(raw json.RawMessage) (map[string]any, bool) {
	fields, ok := jsonObject(raw)
	if !ok || len(fields) == 0 {
		return nil, false
	}
	claims := make(map[string]any, len(fields))
	for key, value := range fields {
		if key == "" || key == forbiddenClaim {
			return nil, false
		}
		if text, ok := jsonString(value); ok {
			if text == "" {
				return nil, false
			}
			claims[key] = text
			continue
		}
		items, ok := jsonArray(value)
		if !ok || len(items) == 0 {
			return nil, false
		}
		values := make([]string, 0, len(items))
		for _, item := range items {
			text, ok := jsonString(item)
			if !ok || text == "" {
				return nil, false
			}
			values = append(values, text)
		}
		claims[key] = values
	}
	return claims, true
}

// claimValues returns the values of a claim as a list.
func claimValues(value any) []string {
	switch v := value.(type) {
	case string:
		return []string{v}
	case []string:
		return v
	}
	return nil
}

func onlyKeys(fields map[string]json.RawMessage, keys ...string) bool {
	for key := range fields {
		if !slices.Contains(keys, key) {
			return false
		}
	}
	return true
}

// jsonObject decodes raw when it is a JSON object. A null is not an object.
func jsonObject(raw json.RawMessage) (map[string]json.RawMessage, bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] != '{' {
		return nil, false
	}
	var out map[string]json.RawMessage
	if json.Unmarshal(raw, &out) != nil {
		return nil, false
	}
	return out, true
}

func jsonArray(raw json.RawMessage) ([]json.RawMessage, bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] != '[' {
		return nil, false
	}
	var out []json.RawMessage
	if json.Unmarshal(raw, &out) != nil {
		return nil, false
	}
	return out, true
}

func jsonString(raw json.RawMessage) (string, bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] != '"' {
		return "", false
	}
	var out string
	if json.Unmarshal(raw, &out) != nil {
		return "", false
	}
	return out, true
}

// accountRoleOf returns the project role with the name name.
func accountRoleOf(name string) (accountRole, bool) {
	for _, role := range accountRoles {
		if role.name == name {
			return role, true
		}
	}
	return accountRole{}, false
}

// trustStatus is the status annotation. The field order is the key order of
// the JSON document. Statements is nil for a document error.
type trustStatus struct {
	ObservedHash string             `json:"observedHash"`
	ObservedAt   string             `json:"observedAt"`
	Statements   *[]statementStatus `json:"statements,omitempty"`
	Error        *trustReason       `json:"error,omitempty"`
}

type trustReason struct {
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

type statementStatus struct {
	Name    string    `json:"name"`
	Ready   bool      `json:"ready"`
	Reason  string    `json:"reason,omitempty"`
	Message string    `json:"message,omitempty"`
	Login   *loginRef `json:"login,omitempty"`
}

// loginRef is the auth mount path and the role name of a login role.
type loginRef struct {
	Path string `json:"path"`
	Role string `json:"role"`
}

func newTrustReason(reason string) *trustReason {
	return &trustReason{Reason: reason, Message: trustMessages[reason]}
}

// notReady returns the status of a statement with reason.
func notReady(name, reason string) statementStatus {
	return statementStatus{Name: statusName(name), Reason: reason, Message: trustMessages[reason]}
}

// statusName cuts an invalid name to maxStatusName bytes, at a rune boundary.
func statusName(name string) string {
	if len(name) <= maxStatusName {
		return name
	}
	cut := maxStatusName
	for cut > 0 && !utf8.RuneStart(name[cut]) {
		cut--
	}
	return name[:cut]
}

// sameTrustStatus reports whether the status current equals want, without
// observedAt. A current value that is not a JSON object differs.
func sameTrustStatus(current string, want trustStatus) bool {
	var have map[string]any
	if json.Unmarshal([]byte(current), &have) != nil || have == nil {
		return false
	}
	data, err := json.Marshal(want)
	if err != nil {
		return false
	}
	var wanted map[string]any
	if json.Unmarshal(data, &wanted) != nil {
		return false
	}
	delete(have, "observedAt")
	delete(wanted, "observedAt")
	return reflect.DeepEqual(have, wanted)
}

// previousLogins returns the login roles of the ready statements of the
// status value. A value that does not parse lists none.
func previousLogins(value string) []loginRef {
	var status struct {
		Statements []struct {
			Ready bool      `json:"ready"`
			Login *loginRef `json:"login"`
		} `json:"statements"`
	}
	if json.Unmarshal([]byte(value), &status) != nil {
		return nil
	}
	var out []loginRef
	for _, item := range status.Statements {
		if item.Ready && item.Login != nil {
			out = append(out, *item.Login)
		}
	}
	return out
}
