package valkey

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"

	"github.com/google/uuid"

	"github.com/maxp/hookrelay/internal/gen"
)

const (
	adminAuthKey     = "hr1:admin_auth"
	adminSessionsKey = "hr1:admin_sessions"
	// adminGenerationContext separates the generation tag from any other
	// HMAC keyed by the Admin Secret.
	adminGenerationContext = "hookrelay-admin-session-generation-v1"
	adminSaltBytes         = 32
)

// AdminAuthOutcome is the result of the startup Admin Secret generation
// check: current (unchanged), initialized, or rotated with the number of
// revoked indexed sessions.
type AdminAuthOutcome struct {
	Result  string
	Revoked int64
}

// errAdminAuthInconsistent marks a generation record the check cannot
// safely act on; readiness waits for operator reconciliation.
var errAdminAuthInconsistent = errors.New("valkey: admin_auth inconsistent")

// adminGenerationTag is HMAC-SHA-256 keyed by the Admin Secret over the
// decoded salt followed by the fixed context, in lowercase hex.
func adminGenerationTag(secret string, salt []byte) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write(salt)
	m.Write([]byte(adminGenerationContext))
	return hex.EncodeToString(m.Sum(nil))
}

// EnsureAdminAuth detects an Admin Secret change without storing the
// secret: it initializes hr1:admin_auth when missing, does nothing when the
// stored tag matches the configured secret, and otherwise rotates the
// generation, revoking every indexed browser session with the mandatory
// audit. A malformed record, a record that keeps changing underneath, or an
// uncertain script result is an error, so readiness is withheld. Neither
// the secret, the salt, nor the tag is ever logged or returned.
func (a *Adapter) EnsureAdminAuth(ctx context.Context, secret string, g gen.Gen) (AdminAuthOutcome, error) {
	if secret == "" {
		return AdminAuthOutcome{}, fmt.Errorf("%w: no Admin Secret configured", errAdminAuthInconsistent)
	}
	for range 2 {
		rec, err := a.client.Do(ctx, a.client.B().Hgetall().Key(adminAuthKey).Build()).AsStrMap()
		if err != nil {
			if isWrongType(err) {
				return AdminAuthOutcome{}, fmt.Errorf("%w: wrong_type", errAdminAuthInconsistent)
			}
			return AdminAuthOutcome{}, fmt.Errorf("valkey: admin_auth read: %w", err)
		}
		if len(rec) == 0 {
			salt, err := g.Base64URL(adminSaltBytes)
			if err != nil {
				return AdminAuthOutcome{}, fmt.Errorf("valkey: admin_auth salt: %w", err)
			}
			raw, _ := base64.RawURLEncoding.DecodeString(salt)
			res, err := a.runAdminAuth(ctx, "initialize", "", g.UUIDv7(), salt, adminGenerationTag(secret, raw), g.UUIDv7())
			if err != nil {
				return AdminAuthOutcome{}, err
			}
			switch res.Status {
			case "initialized":
				return AdminAuthOutcome{Result: "initialized"}, nil
			case "exists":
				continue
			default:
				return AdminAuthOutcome{}, fmt.Errorf("%w: %s", errAdminAuthInconsistent, res.Status)
			}
		}
		salt, err := base64.RawURLEncoding.DecodeString(rec["generation_salt"])
		id, idErr := uuid.Parse(rec["generation_id"])
		updated, updatedErr := strconv.ParseInt(rec["updated_ms"], 10, 64)
		tagBytes, tagErr := hex.DecodeString(rec["generation_tag"])
		if err != nil || len(salt) != adminSaltBytes || tagErr != nil || len(tagBytes) != sha256.Size ||
			hex.EncodeToString(tagBytes) != rec["generation_tag"] || idErr != nil || id.Version() != 7 ||
			updatedErr != nil || updated <= 0 || strconv.FormatInt(updated, 10) != rec["updated_ms"] {
			return AdminAuthOutcome{}, fmt.Errorf("%w: malformed record", errAdminAuthInconsistent)
		}
		tag := adminGenerationTag(secret, salt)
		if hmac.Equal([]byte(tag), []byte(rec["generation_tag"])) {
			return AdminAuthOutcome{Result: "current"}, nil
		}
		res, err := a.runAdminAuth(ctx, "rotate", rec["generation_id"], g.UUIDv7(), rec["generation_salt"], tag, g.UUIDv7())
		if err != nil {
			return AdminAuthOutcome{}, err
		}
		switch res.Status {
		case "rotated":
			n, err := res.Fields[0].AsInt64()
			if err != nil {
				return AdminAuthOutcome{}, fmt.Errorf("valkey: admin_auth_v1: rotated shape: %w", err)
			}
			return AdminAuthOutcome{Result: "rotated", Revoked: n}, nil
		case "current":
			return AdminAuthOutcome{Result: "current"}, nil
		case "changed", "absent":
			continue
		default:
			return AdminAuthOutcome{}, fmt.Errorf("%w: %s", errAdminAuthInconsistent, res.Status)
		}
	}
	return AdminAuthOutcome{}, fmt.Errorf("%w: the record changed concurrently", errAdminAuthInconsistent)
}

func (a *Adapter) runAdminAuth(ctx context.Context, mode, expectedID, newID, salt, tag, eventID string) (*Result, error) {
	res, err := a.RunScript(ctx, "admin_auth_v1", []string{adminAuthKey, adminSessionsKey, auditKey},
		[]string{mode, expectedID, newID, salt, tag, eventID, "hr1"})
	if err != nil {
		// A lost reply may hide a completed rotation: the next pass rereads.
		return nil, fmt.Errorf("valkey: admin_auth_v1 %s: %w", mode, err)
	}
	return res, nil
}
