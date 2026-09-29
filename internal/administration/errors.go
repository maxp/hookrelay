package administration

import (
	"fmt"
	"net/http"
)

// apiError is a use-case or transport failure with its bounded error code
// and HTTP status. Every error type below implements it, so the transport
// maps failures in one place.
type apiError interface {
	error
	ErrorCode() string
	HTTPStatus() int
}

// BadRequestError maps to 400 invalid_request, or to the given bounded code
// and status (413 request_too_large, 415 unsupported_media_type).
type BadRequestError struct {
	msg    string
	code   string
	status int
}

func (e BadRequestError) Error() string { return e.msg }
func (e BadRequestError) ErrorCode() string {
	if e.code != "" {
		return e.code
	}
	return "invalid_request"
}
func (e BadRequestError) HTTPStatus() int {
	if e.status != 0 {
		return e.status
	}
	return http.StatusBadRequest
}

// NotFoundError maps to 404 webhook_endpoint_not_found.
type NotFoundError struct{}

func (NotFoundError) Error() string     { return "webhook endpoint not found" }
func (NotFoundError) ErrorCode() string { return "webhook_endpoint_not_found" }
func (NotFoundError) HTTPStatus() int   { return http.StatusNotFound }

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
func (ConflictError) HTTPStatus() int { return http.StatusConflict }

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
func (DependencyError) HTTPStatus() int   { return http.StatusServiceUnavailable }

// UnexpectedError maps to 500 internal_error.
type UnexpectedError struct{ Err error }

func (e UnexpectedError) Error() string   { return fmt.Sprintf("internal error: %v", e.Err) }
func (UnexpectedError) ErrorCode() string { return "internal_error" }
func (UnexpectedError) HTTPStatus() int   { return http.StatusInternalServerError }

// StatusError maps to the given status and bounded code.
type StatusError struct {
	Status int
	Code   string
	Msg    string
}

func (e StatusError) Error() string     { return e.Msg }
func (e StatusError) ErrorCode() string { return e.Code }
func (e StatusError) HTTPStatus() int   { return e.Status }
