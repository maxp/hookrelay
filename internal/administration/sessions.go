package administration

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/maxp/hookrelay/internal/observability"
	"github.com/maxp/hookrelay/internal/ratelimit"
)

// SessionCookieName is the administrative session cookie (admin-api.md).
const SessionCookieName = "hookrelay_admin"

// Session token and CSRF token sizes in random bytes: a 43- and a
// 22-character base64url value.
const (
	sessionTokenBytes = 32
	csrfTokenBytes    = 16
)

// sessionTokenPattern is the shape of a session token; any other cookie
// value cannot name a session and is never hashed or looked up.
var sessionTokenPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)

// DefaultLoginLimits are the accepted login token buckets: 5 per minute
// with burst 5 per source address, 60 per minute with burst 20 globally.
var DefaultLoginLimits = ratelimit.Limits{GlobalRate: 1, GlobalBurst: 20, KeyRate: 5.0 / 60, KeyBurst: 5}

// Bounded login outcomes for hookrelay_admin_login_attempts_total.
const (
	loginSuccess          = "success"
	loginFailure          = "failure"
	loginRateLimited      = "rate_limited"
	loginCapacityExceeded = "capacity_exceeded"
	loginUnavailable      = "unavailable"
)

// Bounded audit operations of the session slice.
const (
	opAdminLogin          = "admin_login"
	opAdminLogout         = "admin_logout"
	opAdminSessionExpired = "admin_session_expired"
)

// Bounded reasons for hookrelay_admin_csrf_rejections_total.
const (
	csrfReasonOrigin = "origin"
	csrfReasonToken  = "token"
)

// LoginRequest is the strict login body. The secret is never logged or
// audited.
type LoginRequest struct {
	AdminSecret *string `json:"admin_secret"`
}

// SessionView is the GET /admin/v1/session response.
type SessionView struct {
	Authenticated     bool   `json:"authenticated"`
	IdleExpiresMs     int64  `json:"idle_expires_ms"`
	AbsoluteExpiresMs int64  `json:"absolute_expires_ms"`
	CSRFToken         string `json:"csrf_token"`
}

// sessionDigest is the lowercase hex SHA-256 of a session token: the only
// form in which a token reaches storage.
func sessionDigest(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// sessionCookie builds the session cookie; maxAge < 0 clears it.
func (s *Service) sessionCookie(token string, maxAge int) *http.Cookie {
	return &http.Cookie{Name: SessionCookieName, Value: token, Path: "/", MaxAge: maxAge, HttpOnly: true,
		SameSite: http.SameSiteStrictMode, Secure: s.cookieSecure}
}

func (s *Service) clearSessionCookie(w http.ResponseWriter) {
	http.SetCookie(w, s.sessionCookie("", -1))
}

// originAllowed reports whether the request's Origin is exactly the
// configured administrative origin. Forwarded headers are never consulted.
func (s *Service) originAllowed(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	return s.adminOrigin != "" && len(r.Header.Values("Origin")) == 1 &&
		subtle.ConstantTimeCompare([]byte(origin), []byte(s.adminOrigin)) == 1
}

// sourceAddress is the login rate-limit key: the connection's peer address.
func sourceAddress(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// sessionRoute wraps the session routes, which authenticate themselves.
func (s *Service) sessionRoute(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = s.startRequest(w, r)
		next(w, r.WithContext(context.WithValue(r.Context(), actorKey, actorAdminSession)))
	})
}

func (s *Service) forbidden(w http.ResponseWriter, reason, requestID string) {
	s.metrics.csrfRejections.WithLabelValues(reason).Inc()
	writeError(w, http.StatusForbidden, "forbidden", "the request origin or CSRF token is not accepted", requestID)
}

// handleLogin creates a browser session from the Admin Secret. The cookie
// is issued only after the session and its login audit are confirmed.
func (s *Service) handleLogin(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFrom(r.Context())
	if !s.originAllowed(r) {
		s.forbidden(w, csrfReasonOrigin, requestID)
		return
	}
	if ok, retryAfter := s.loginLimiter.Allow(sourceAddress(r), s.now()); !ok {
		s.metrics.loginAttempts.WithLabelValues(loginRateLimited).Inc()
		w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
		writeError(w, http.StatusTooManyRequests, "rate_limit_exceeded", "too many login attempts", requestID)
		return
	}
	var req LoginRequest
	if err := decodeBody(r, &req); err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	if req.AdminSecret == nil {
		writeAPIError(w, BadRequestError{msg: "admin_secret is required"}, requestID)
		return
	}
	if !s.secretMatches(*req.AdminSecret) {
		s.metrics.loginAttempts.WithLabelValues(loginFailure).Inc()
		s.appendBestEffort(r.Context(), AuditEvent{Actor: actorAdminSession, Operation: opAdminLogin, Target: "session",
			RequestID: requestID, Outcome: outcomeFailure})
		writeError(w, http.StatusUnauthorized, "unauthenticated", "missing or invalid Admin Secret", requestID)
		return
	}
	token, err1 := s.gen.Base64URL(sessionTokenBytes)
	csrf, err2 := s.gen.Base64URL(csrfTokenBytes)
	if err1 != nil || err2 != nil {
		s.metrics.loginAttempts.WithLabelValues(loginUnavailable).Inc()
		writeError(w, http.StatusInternalServerError, "internal_error", "unexpected internal error", requestID)
		return
	}
	eventID := s.gen.UUIDv7()
	c := s.sessions.CreateSession(r.Context(), sessionDigest(token), csrf, eventID, requestID)
	s.auditExpiredSessions(r.Context(), c.Expired)
	switch c.Result {
	case SessionCreated:
		s.metrics.loginAttempts.WithLabelValues(loginSuccess).Inc()
		s.metrics.auditEvents.WithLabelValues(opAdminLogin, outcomeSuccess).Inc()
		s.logAuditAs(actorAdminSession, eventID, opAdminLogin, "session", requestID, outcomeSuccess)
		observability.LogEvent(s.log, slog.LevelInfo, "admin_session_created", "administrative session created",
			"request_id", requestID, "idle_expires_ms", c.IdleExpiresMs, "absolute_expires_ms", c.AbsoluteExpiresMs)
		http.SetCookie(w, s.sessionCookie(token, int(SessionAbsoluteTimeout/time.Second)))
		w.WriteHeader(http.StatusNoContent)
	case SessionCapacityExceeded:
		s.metrics.loginAttempts.WithLabelValues(loginCapacityExceeded).Inc()
		writeError(w, http.StatusTooManyRequests, "session_capacity_exceeded",
			"the administrative session capacity is exhausted: log out elsewhere or wait for a session to expire", requestID)
	default:
		s.metrics.loginAttempts.WithLabelValues(loginUnavailable).Inc()
		observability.LogEvent(s.log, slog.LevelError, "admin_session_create_failed", "administrative session not created",
			"request_id", requestID, "error_code", "dependency_unavailable", "reason_code", string(c.Result))
		writeAPIError(w, DependencyError{detail: "the session could not be confirmed; no session was issued"}, requestID)
	}
}

// auditExpiredSessions appends one best-effort expiry event per session a
// transition removed; expiry never waits on audit.
func (s *Service) auditExpiredSessions(ctx context.Context, n int) {
	for range n {
		s.appendBestEffort(ctx, AuditEvent{Actor: "maintenance", Operation: opAdminSessionExpired, Target: "session", Outcome: outcomeSuccess})
	}
}

// cookieSession authenticates the request's session cookie. present is
// false without a cookie; a cookie that cannot name a session is present
// but invalid without any storage access.
func (s *Service) cookieSession(r *http.Request) (auth SessionAuth, digest string, present bool) {
	c, err := r.Cookie(SessionCookieName)
	if err != nil {
		return SessionAuth{}, "", false
	}
	if !sessionTokenPattern.MatchString(c.Value) {
		return SessionAuth{Result: SessionInvalid, Reason: SessionReasonMalformed}, "", true
	}
	digest = sessionDigest(c.Value)
	auth = s.sessions.AuthenticateSession(r.Context(), digest)
	if auth.Result == SessionInvalid && auth.Reason == SessionReasonExpired {
		s.auditExpiredSessions(r.Context(), 1)
	}
	return auth, digest, true
}

// handleGetSession reports the session's expiries and its CSRF token.
func (s *Service) handleGetSession(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFrom(r.Context())
	auth, _, present := s.cookieSession(r)
	switch {
	case present && auth.Result == SessionValid:
		writeJSON(w, http.StatusOK, SessionView{Authenticated: true, IdleExpiresMs: auth.IdleExpiresMs,
			AbsoluteExpiresMs: auth.AbsoluteExpiresMs, CSRFToken: auth.CSRFToken})
	case !present || auth.Result == SessionInvalid:
		s.clearSessionCookie(w)
		writeError(w, http.StatusUnauthorized, "unauthenticated", "no valid administrative session", requestID)
	default:
		writeAPIError(w, DependencyError{}, requestID)
	}
}

// handleLogout revokes the session. A missing or already invalid session
// is logged out; a valid one needs the Origin and CSRF checks. Revocation
// is claimed only after it is confirmed.
func (s *Service) handleLogout(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFrom(r.Context())
	auth, digest, present := s.cookieSession(r)
	if !present || auth.Result == SessionInvalid {
		s.clearSessionCookie(w)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if auth.Result != SessionValid {
		writeAPIError(w, DependencyError{detail: "revocation could not be confirmed"}, requestID)
		return
	}
	if reason := s.csrfFailure(r, auth); reason != "" {
		s.forbidden(w, reason, requestID)
		return
	}
	switch s.sessions.DeleteSession(r.Context(), digest) {
	case SessionDeleted:
		s.appendBestEffort(r.Context(), AuditEvent{Actor: actorAdminSession, Operation: opAdminLogout, Target: "session",
			RequestID: requestID, Outcome: outcomeSuccess})
		fallthrough
	case SessionDeleteAbsent:
		s.clearSessionCookie(w)
		w.WriteHeader(http.StatusNoContent)
	default:
		writeAPIError(w, DependencyError{detail: "revocation could not be confirmed"}, requestID)
	}
}

// csrfFailure checks a cookie-authenticated state-changing request: the
// exact configured Origin and the session's CSRF token. It returns the
// bounded rejection reason, or "" when both match.
func (s *Service) csrfFailure(r *http.Request, auth SessionAuth) string {
	if !s.originAllowed(r) {
		return csrfReasonOrigin
	}
	token := r.Header.Get("X-CSRF-Token")
	if len(r.Header.Values("X-CSRF-Token")) != 1 || auth.CSRFToken == "" ||
		subtle.ConstantTimeCompare([]byte(token), []byte(auth.CSRFToken)) != 1 {
		return csrfReasonToken
	}
	return ""
}

// sessionExpiryBatch bounds the sessions one maintenance round expires.
const sessionExpiryBatch = 100

// MaintainSessions is the maintenance round hook: it removes expired
// sessions with their best-effort expiry audit and updates the session
// gauge. Expiry never waits on audit, and an expired session is invalid
// whether or not this has run.
func (s *Service) MaintainSessions(ctx context.Context) error {
	if s.sessions == nil {
		return nil
	}
	n, indexed, err := s.sessions.ExpireSessions(ctx, sessionExpiryBatch)
	if err != nil {
		return err
	}
	s.auditExpiredSessions(ctx, n)
	s.metrics.sessions.Set(float64(indexed))
	return nil
}
