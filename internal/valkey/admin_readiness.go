package valkey

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// checkAdminRecords validates the persistent administration structures in
// bounded keyspace pages. These records are authoritative: unlike delivery's
// derived indexes, an incompatible endpoint or bot membership cannot be
// guessed back into shape during reconciliation.
func (a *Adapter) checkAdminRecords(ctx context.Context) error {
	if err := a.scanKeys(ctx, "hr1:wh:*", 100, func(key string) error {
		parts := strings.Split(strings.TrimPrefix(key, "hr1:wh:"), ":")
		if len(parts) != 2 || parts[0] != "telegram" || !validAdminIdentifier(parts[1]) {
			return fmt.Errorf("valkey: invalid endpoint key %q", key)
		}
		if err := a.expectKeyType(ctx, key, "hash"); err != nil {
			return err
		}
		fields, err := a.client.Do(ctx, a.client.B().Hgetall().Key(key).Build()).AsStrMap()
		if err != nil {
			return fmt.Errorf("valkey: endpoint %s: %w", key, err)
		}
		botID := fields["bot_id"]
		if !validAdminBotID(botID) || (fields["enabled"] != "0" && fields["enabled"] != "1") ||
			fields["credential_kind"] != "secret_token" || !validAdminCredential(fields["credential_value"]) {
			return fmt.Errorf("valkey: endpoint %s has invalid identity or credential encoding", key)
		}
		id, err := uuid.Parse(fields["generation_id"])
		if err != nil || id.Version() != 7 {
			return fmt.Errorf("valkey: endpoint %s has invalid generation_id", key)
		}
		created, err1 := strconv.ParseInt(fields["created_ms"], 10, 64)
		updated, err2 := strconv.ParseInt(fields["updated_ms"], 10, 64)
		version, err3 := strconv.ParseInt(fields["config_version"], 10, 64)
		if err1 != nil || err2 != nil || err3 != nil || created <= 0 || updated < created || version <= 0 ||
			strconv.FormatInt(created, 10) != fields["created_ms"] ||
			strconv.FormatInt(updated, 10) != fields["updated_ms"] ||
			strconv.FormatInt(version, 10) != fields["config_version"] {
			return fmt.Errorf("valkey: endpoint %s has invalid timestamps or config_version", key)
		}
		member := parts[0] + ":" + parts[1]
		botKey := "hr1:bot:telegram:" + botID + ":webhooks"
		if err := a.expectKeyType(ctx, botKey, "set"); err != nil {
			return err
		}
		inBot, err := a.client.Do(ctx, a.client.B().Sismember().Key(botKey).Member(member).Build()).AsBool()
		if err != nil || !inBot {
			return fmt.Errorf("valkey: endpoint %s missing bot membership: %v", key, err)
		}
		score, err := a.client.Do(ctx, a.client.B().Zscore().Key("hr1:webhooks").Member(member).Build()).AsInt64()
		if err != nil || score != created {
			return fmt.Errorf("valkey: endpoint %s missing or invalid listing score: %v", key, err)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := a.scanKeys(ctx, "hr1:bot:*:webhooks", 100, func(key string) error {
		parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(key, "hr1:bot:"), ":webhooks"), ":")
		if len(parts) != 2 || parts[0] != "telegram" || !validAdminBotID(parts[1]) {
			return fmt.Errorf("valkey: invalid bot identity key %q", key)
		}
		if err := a.expectKeyType(ctx, key, "set"); err != nil {
			return err
		}
		n, err := a.client.Do(ctx, a.client.B().Scard().Key(key).Build()).AsInt64()
		if err != nil || n > 100 {
			return fmt.Errorf("valkey: bot set %s invalid size: %v", key, err)
		}
		members, err := a.client.Do(ctx, a.client.B().Smembers().Key(key).Build()).AsStrSlice()
		if err != nil {
			return fmt.Errorf("valkey: bot set %s: %w", key, err)
		}
		for _, member := range members {
			ident := strings.Split(member, ":")
			if len(ident) != 2 || ident[0] != "telegram" || !validAdminIdentifier(ident[1]) {
				return fmt.Errorf("valkey: bot set %s has invalid member", key)
			}
			endpoint := "hr1:wh:" + member
			if err := a.expectKeyType(ctx, endpoint, "hash"); err != nil {
				return err
			}
			botID, err := a.client.Do(ctx, a.client.B().Hget().Key(endpoint).Field("bot_id").Build()).ToString()
			if err != nil || botID != parts[1] {
				return fmt.Errorf("valkey: bot set %s has orphan or mismatched member: %v", key, err)
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return a.scanMembers(ctx, "hr1:webhooks", 100, func(members []string) error {
		for _, member := range members {
			if err := a.expectKeyType(ctx, "hr1:wh:"+member, "hash"); err != nil {
				return err
			}
		}
		return nil
	})
}

func (a *Adapter) expectKeyType(ctx context.Context, key, want string) error {
	t, err := a.keyType(ctx, key)
	if err != nil {
		return err
	}
	if t != want {
		return fmt.Errorf("valkey: key %s has unexpected type %q, want %s", key, t, want)
	}
	return nil
}

func validAdminIdentifier(s string) bool {
	return len(s) >= 1 && len(s) <= 128 && validAdminIdentifierPart(s)
}

func validAdminBotID(s string) bool {
	if len(s) < 1 || len(s) > 20 || s[0] < '1' || s[0] > '9' {
		return false
	}
	for i := 1; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func validAdminCredential(s string) bool {
	if len(s) < 1 || len(s) > 256 {
		return false
	}
	return validAdminIdentifierPart(s)
}

func validAdminIdentifierPart(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}
