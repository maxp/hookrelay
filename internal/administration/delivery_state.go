package administration

import (
	"context"
	"net/http"
)

// DeliveryState is one message's bounded delivery-state classification.
// Inconsistent carries the reason when stored state cannot be classified.
type DeliveryState struct {
	Found         bool
	State         string // queued | leased | retry_wait | dead_lettered | acknowledged
	DeliveryCycle int64
	QueuePosition string // head | behind_head; "" when not queued
	Inconsistent  string
}

// MessageStateRepository reads delivery state; the Valkey adapter
// implements it.
type MessageStateRepository interface {
	DeliveryState(ctx context.Context, messageID string) (DeliveryState, error)
}

// DeliveryStateView is the delivery-state response: never a payload or
// Delivery Token.
type DeliveryStateView struct {
	MessageID     string `json:"message_id"`
	DeliveryCycle int64  `json:"delivery_cycle"`
	State         string `json:"state"`
	QueuePosition string `json:"queue_position,omitempty"`
}

func messageNotFound() error {
	return StatusError{Status: http.StatusNotFound, Code: "message_not_found",
		Msg: "no delivery state is retained for this message"}
}

// GetDeliveryState classifies one message so a replay with a lost response
// can be reconciled.
func (s *Service) GetDeliveryState(ctx context.Context, messageID string) (DeliveryStateView, error) {
	if !messageIDPattern.MatchString(messageID) {
		return DeliveryStateView{}, messageNotFound()
	}
	st, err := s.messages.DeliveryState(ctx, messageID)
	switch {
	case err != nil:
		return DeliveryStateView{}, DependencyError{}
	case st.Inconsistent != "":
		return DeliveryStateView{}, StatusError{Status: http.StatusConflict, Code: "recipient_state_ambiguous",
			Msg: "stored delivery state is inconsistent (" + st.Inconsistent + "): follow the reconciliation runbook"}
	case !st.Found:
		return DeliveryStateView{}, messageNotFound()
	}
	return DeliveryStateView{MessageID: messageID, DeliveryCycle: st.DeliveryCycle, State: st.State, QueuePosition: st.QueuePosition}, nil
}

func (s *Service) handleDeliveryState(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFrom(r.Context())
	v, err := s.GetDeliveryState(r.Context(), r.PathValue("message_id"))
	if err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	writeJSON(w, http.StatusOK, v)
}
