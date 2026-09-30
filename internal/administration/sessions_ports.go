package administration

import (
	"context"
	"time"
)

// Administrative browser session policy (platform.md).
const (
	SessionIdleTimeout     = time.Hour
	SessionAbsoluteTimeout = 12 * time.Hour
	// SessionRefreshInterval throttles idle-expiry refreshes.
	SessionRefreshInterval = 5 * time.Minute
	// SessionCapacity bounds concurrently valid sessions; a full store
	// refuses new logins and never evicts.
	SessionCapacity = 100
)

// SessionCreateResult is the bounded outcome of the audited login
// transition.
type SessionCreateResult string

const (
	SessionCreated           SessionCreateResult = "created"
	SessionCapacityExceeded  SessionCreateResult = "capacity_exceeded"
	SessionAuthUninitialized SessionCreateResult = "auth_uninitialized"
	SessionCollision         SessionCreateResult = "collision"
	SessionCreateWrongType   SessionCreateResult = "wrong_type"
	// SessionCreateUnavailable: no session may be assumed; the cookie is
	// never issued.
	SessionCreateUnavailable SessionCreateResult = "dependency_unavailable"
)

// SessionCreate is the login transition's result. Expired counts the
// expired sessions the transition removed, for their best-effort audit.
type SessionCreate struct {
	Result            SessionCreateResult
	CreatedMs         int64
	IdleExpiresMs     int64
	AbsoluteExpiresMs int64
	Expired           int
}

// SessionAuthResult is the bounded outcome of session authentication.
type SessionAuthResult string

const (
	SessionValid            SessionAuthResult = "valid"
	SessionInvalid          SessionAuthResult = "invalid"
	SessionAuthWrongType    SessionAuthResult = "wrong_type"
	SessionAuthUnavailable  SessionAuthResult = "dependency_unavailable"
	SessionReasonExpired                      = "expired"
	SessionReasonAbsent                       = "absent"
	SessionReasonMalformed                    = "malformed"
	SessionReasonGeneration                   = "generation_changed"
)

// SessionAuth is the authentication result; the token and expiries are set
// for SessionValid, Reason for SessionInvalid.
type SessionAuth struct {
	Result            SessionAuthResult
	Reason            string
	CSRFToken         string
	IdleExpiresMs     int64
	AbsoluteExpiresMs int64
}

// SessionDeleteResult is the bounded outcome of logout.
type SessionDeleteResult string

const (
	SessionDeleted           SessionDeleteResult = "deleted"
	SessionDeleteAbsent      SessionDeleteResult = "absent"
	SessionDeleteWrongType   SessionDeleteResult = "wrong_type"
	SessionDeleteUnavailable SessionDeleteResult = "dependency_unavailable"
)

// SessionStore is the browser-session storage the Valkey adapter
// implements. Sessions are addressed only by the SHA-256 digest of their
// token; the token itself never reaches storage.
type SessionStore interface {
	// CreateSession removes expired sessions, enforces capacity, and
	// stores the session with its login audit in one operation.
	CreateSession(ctx context.Context, digest, csrfToken, eventID, requestID string) SessionCreate
	// AuthenticateSession validates the session and refreshes its idle
	// expiry at most once per SessionRefreshInterval.
	AuthenticateSession(ctx context.Context, digest string) SessionAuth
	// DeleteSession atomically revokes the session and removes its index member.
	// The caller audits an actual logout best effort after confirmed deletion.
	DeleteSession(ctx context.Context, digest string) SessionDeleteResult
	// ExpireSessions removes at most limit expired sessions and reports
	// how many it removed and how many remain indexed.
	ExpireSessions(ctx context.Context, limit int) (expired int, indexed int64, err error)
}
