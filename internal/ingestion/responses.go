package ingestion

import "net/http"

// Outcome is the bounded result of one webhook request. It is also the
// outcome label of hookrelay_webhook_requests_total.
type Outcome string

const (
	OutcomeAccepted             Outcome = "accepted"
	OutcomeDuplicate            Outcome = "duplicate"
	OutcomeUnknownEndpoint      Outcome = "unknown_endpoint"
	OutcomeMethodNotAllowed     Outcome = "method_not_allowed"
	OutcomeVerificationFailure  Outcome = "verification_failure"
	OutcomeInvalidJSON          Outcome = "invalid_json"
	OutcomeOversizedBody        Outcome = "oversized_body"
	OutcomeUnsupportedMediaType Outcome = "unsupported_media_type"
	OutcomeRequestTimeout       Outcome = "request_timeout"
	OutcomeBodyReadFailed       Outcome = "body_read_failed"
	// OutcomeOverloaded: the in-flight webhook limit is reached.
	OutcomeOverloaded Outcome = "overloaded"
	// OutcomeRateLimited: the global or per-endpoint token bucket is empty.
	OutcomeRateLimited           Outcome = "rate_limited"
	OutcomeRecipientBlocked      Outcome = "recipient_blocked"
	OutcomeCapacityRejection     Outcome = "capacity_rejection"
	OutcomeDependencyUnavailable Outcome = "dependency_unavailable"
	OutcomeInternalError         Outcome = "internal_error"
)

// ResponseMapper maps a bounded outcome to a platform-compatible status.
// Webhook responses never carry bodies beyond the platform minimum.
type ResponseMapper interface {
	Status(o Outcome) (status int, retryAfter bool)
}

// DefaultResponses is the common mapping from the message contract; the
// Telegram adapter uses it unchanged (verification failure is 403).
type DefaultResponses struct{}

func (DefaultResponses) Status(o Outcome) (int, bool) {
	switch o {
	case OutcomeAccepted, OutcomeDuplicate:
		return http.StatusOK, false
	case OutcomeUnknownEndpoint:
		return http.StatusNotFound, false
	case OutcomeMethodNotAllowed:
		return http.StatusMethodNotAllowed, false
	case OutcomeVerificationFailure:
		return http.StatusForbidden, false
	case OutcomeInvalidJSON, OutcomeBodyReadFailed:
		return http.StatusBadRequest, false
	case OutcomeRequestTimeout:
		return http.StatusRequestTimeout, false
	case OutcomeOversizedBody:
		return http.StatusRequestEntityTooLarge, false
	case OutcomeUnsupportedMediaType:
		return http.StatusUnsupportedMediaType, false
	case OutcomeRateLimited:
		return http.StatusTooManyRequests, true
	case OutcomeRecipientBlocked, OutcomeCapacityRejection, OutcomeOverloaded, OutcomeDependencyUnavailable:
		return http.StatusServiceUnavailable, true
	default:
		return http.StatusInternalServerError, false
	}
}
