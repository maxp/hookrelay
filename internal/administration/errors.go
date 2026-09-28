package administration

import "fmt"

// BadRequestError maps to 400 invalid_request (or the given bounded code).
type BadRequestError struct {
	msg  string
	code string
}

func (e BadRequestError) Error() string { return e.msg }
func (e BadRequestError) ErrorCode() string {
	if e.code != "" {
		return e.code
	}
	return "invalid_request"
}

// NotFoundError maps to 404 webhook_endpoint_not_found.
type NotFoundError struct{}

func (NotFoundError) Error() string     { return "webhook endpoint not found" }
func (NotFoundError) ErrorCode() string { return "webhook_endpoint_not_found" }

// ConflictError maps to 409 with its bounded code.
type ConflictError struct {
	msg  string
	code string
}

func (e ConflictError) Error() string { return e.msg }
func (e ConflictError) ErrorCode() string {
	if e.code != "" {
		return e.code
	}
	return "internal_error"
}

// DependencyError maps to 503 dependency_unavailable. detail never carries
// credential or key material.
type DependencyError struct {
	detail string
}

func (e DependencyError) Error() string {
	if e.detail != "" {
		return e.detail
	}
	return "dependency unavailable"
}
func (DependencyError) ErrorCode() string { return "dependency_unavailable" }

// UnexpectedError maps to 500 internal_error.
type UnexpectedError struct{ Err error }

func (e UnexpectedError) Error() string   { return fmt.Sprintf("internal error: %v", e.Err) }
func (UnexpectedError) ErrorCode() string { return "internal_error" }
