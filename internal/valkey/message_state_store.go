package valkey

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/maxp/hookrelay/internal/administration"
	"github.com/maxp/hookrelay/internal/model"
)

type messageStateStore struct {
	a *Adapter
}

// NewMessageStateStore returns the delivery-state read used by the Admin
// API.
func NewMessageStateStore(a *Adapter) administration.MessageStateRepository {
	return &messageStateStore{a: a}
}

// DeliveryState locates the message's Recipient from its blob (the queue
// key needs it) and classifies the message with delivery_state_v1, which
// re-reads every authoritative key atomically.
func (s *messageStateStore) DeliveryState(ctx context.Context, messageID string) (administration.DeliveryState, error) {
	c := s.a.client
	rid := ""
	blob, err := c.Do(ctx, c.B().Get().Key("hr1:m:"+messageID).Build()).ToString()
	switch {
	case err == nil:
		var m struct {
			Recipient struct {
				Scope       model.Scope `json:"scope"`
				BotPlatform string      `json:"bot_platform"`
				BotID       string      `json:"bot_id"`
				ChatID      string      `json:"chat_id"`
				UserID      string      `json:"user_id"`
			} `json:"recipient"`
		}
		if json.Unmarshal([]byte(blob), &m) == nil {
			r := model.Recipient{Scope: m.Recipient.Scope, BotPlatform: m.Recipient.BotPlatform, BotID: m.Recipient.BotID,
				ChatID: m.Recipient.ChatID, UserID: m.Recipient.UserID}
			if r.Validate() == nil {
				rid = r.Identity()
			}
		}
	case isNil(err), isWrongType(err):
		// No readable blob: the script classifies the message without it.
	default:
		return administration.DeliveryState{}, err
	}
	res, err := s.a.RunScript(ctx, "delivery_state_v1", nil, []string{messageID, rid, "hr1"})
	if err != nil {
		return administration.DeliveryState{}, err
	}
	switch res.Status {
	case "found":
		state, err1 := res.Fields[0].ToString()
		cycle, err2 := res.Fields[1].AsInt64()
		position, err3 := res.Fields[2].ToString()
		if err1 != nil || err2 != nil || err3 != nil {
			return administration.DeliveryState{}, fmt.Errorf("valkey: delivery_state_v1: result shape")
		}
		return administration.DeliveryState{Found: true, State: state, DeliveryCycle: cycle, QueuePosition: position}, nil
	case "inconsistent":
		reason, err := res.Fields[0].ToString()
		if err != nil {
			return administration.DeliveryState{}, fmt.Errorf("valkey: delivery_state_v1: result shape")
		}
		return administration.DeliveryState{Inconsistent: reason}, nil
	default:
		return administration.DeliveryState{}, nil
	}
}
