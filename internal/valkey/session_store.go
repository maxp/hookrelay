package valkey

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/maxp/hookrelay/internal/administration"
)

type sessionStore struct{ a *Adapter }

// NewSessionStore returns the administrative browser-session storage.
func NewSessionStore(a *Adapter) administration.SessionStore { return &sessionStore{a: a} }

// msArg is a duration as a script's integer milliseconds argument.
func msArg(d time.Duration) string { return strconv.FormatInt(d.Milliseconds(), 10) }

// CreateSession runs session_create_v1.
func (s *sessionStore) CreateSession(ctx context.Context, digest, csrfToken, eventID, requestID string) administration.SessionCreate {
	res, err := s.a.RunScript(ctx, "session_create_v1", []string{adminAuthKey, adminSessionsKey, auditKey},
		[]string{digest, csrfToken, msArg(administration.SessionIdleTimeout), msArg(administration.SessionAbsoluteTimeout),
			strconv.Itoa(administration.SessionCapacity), eventID, requestID, "hr1"})
	if err != nil {
		// The session may exist, but its token is never issued: an orphan
		// session only holds capacity until it expires.
		return administration.SessionCreate{Result: administration.SessionCreateUnavailable}
	}
	f := res.Fields
	switch res.Status {
	case "created":
		created, err1 := f[0].AsInt64()
		idle, err2 := f[1].AsInt64()
		abs, err3 := f[2].AsInt64()
		expired, err4 := f[3].AsInt64()
		if errors.Join(err1, err2, err3, err4) != nil {
			return administration.SessionCreate{Result: administration.SessionCreateUnavailable}
		}
		return administration.SessionCreate{Result: administration.SessionCreated, CreatedMs: created, IdleExpiresMs: idle,
			AbsoluteExpiresMs: abs, Expired: int(expired)}
	case "capacity_exceeded":
		expired, _ := f[0].AsInt64()
		return administration.SessionCreate{Result: administration.SessionCapacityExceeded, Expired: int(expired)}
	default:
		return administration.SessionCreate{Result: administration.SessionCreateResult(res.Status)}
	}
}

// AuthenticateSession runs session_authenticate_v1.
func (s *sessionStore) AuthenticateSession(ctx context.Context, digest string) administration.SessionAuth {
	res, err := s.a.RunScript(ctx, "session_authenticate_v1", []string{adminAuthKey, adminSessionsKey},
		[]string{digest, msArg(administration.SessionIdleTimeout), msArg(administration.SessionRefreshInterval), "hr1"})
	if err != nil {
		return administration.SessionAuth{Result: administration.SessionAuthUnavailable}
	}
	f := res.Fields
	switch res.Status {
	case "valid":
		csrf, err1 := f[0].ToString()
		idle, err2 := f[1].AsInt64()
		abs, err3 := f[2].AsInt64()
		if errors.Join(err1, err2, err3) != nil {
			return administration.SessionAuth{Result: administration.SessionAuthUnavailable}
		}
		return administration.SessionAuth{Result: administration.SessionValid, CSRFToken: csrf, IdleExpiresMs: idle, AbsoluteExpiresMs: abs}
	case "invalid":
		reason, _ := f[0].ToString()
		return administration.SessionAuth{Result: administration.SessionInvalid, Reason: reason}
	default:
		return administration.SessionAuth{Result: administration.SessionAuthWrongType}
	}
}

// DeleteSession runs session_delete_v2. Logout audit is appended by the
// service after confirmed deletion so audit failure cannot prevent revocation.
func (s *sessionStore) DeleteSession(ctx context.Context, digest string) administration.SessionDeleteResult {
	res, err := s.a.RunScript(ctx, "session_delete_v2", []string{adminSessionsKey}, []string{digest, "hr1"})
	if err != nil {
		return administration.SessionDeleteUnavailable
	}
	return administration.SessionDeleteResult(res.Status)
}

// ExpireSessions runs expire_sessions_v1.
func (s *sessionStore) ExpireSessions(ctx context.Context, limit int) (int, int64, error) {
	res, err := s.a.RunScript(ctx, "expire_sessions_v1", []string{adminSessionsKey}, []string{strconv.Itoa(limit), "hr1"})
	if err != nil {
		return 0, 0, err
	}
	if res.Status == "wrong_type" {
		return 0, 0, administration.ErrStoredWrongType
	}
	n, err1 := res.Fields[0].AsInt64()
	indexed, err2 := res.Fields[1].AsInt64()
	if err := errors.Join(err1, err2); err != nil {
		return 0, 0, fmt.Errorf("valkey: expire_sessions_v1: result shape: %w", err)
	}
	return int(n), indexed, nil
}
