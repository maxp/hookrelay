// Package jsonbody implements the strict JSON request-body discipline shared
// by the Admin and Consumer APIs: application/json media type, a 16 KiB
// limit, maximum nesting depth 40, a single top-level object, unknown fields
// and trailing data rejected. Duplicate keys follow encoding/json
// last-value-wins behavior.
package jsonbody

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"strings"
)

// Limits shared by the Admin and Consumer APIs.
const (
	MaxBytes = 16 << 10
	MaxDepth = 40
)

// Failure classes; callers map them to their bounded error codes.
var (
	ErrUnsupportedMediaType = errors.New("Content-Type must be application/json")
	ErrTooLarge             = errors.New("request body larger than 16 KiB")
	ErrInvalid              = errors.New("invalid request body")
)

// Error carries a failure class and a safe message.
type Error struct {
	Class   error
	Message string
}

func (e *Error) Error() string { return e.Message }
func (e *Error) Unwrap() error { return e.Class }

func fail(class error, message string) error { return &Error{Class: class, Message: message} }

// RequireJSON accepts only application/json, optionally with a UTF-8
// charset, parsed as a media type.
func RequireJSON(contentType string) error {
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil || mediaType != "application/json" {
		return fail(ErrUnsupportedMediaType, ErrUnsupportedMediaType.Error())
	}
	for name, value := range params {
		if name != "charset" || !strings.EqualFold(value, "utf-8") {
			return fail(ErrUnsupportedMediaType, ErrUnsupportedMediaType.Error())
		}
	}
	return nil
}

// Decode reads at most MaxBytes+1 bytes and strictly decodes one JSON object
// into dst.
func Decode(body io.Reader, dst any) error {
	data, err := io.ReadAll(io.LimitReader(body, MaxBytes+1))
	if err != nil {
		return fail(ErrInvalid, "request body could not be read")
	}
	if len(data) > MaxBytes {
		return fail(ErrTooLarge, ErrTooLarge.Error())
	}
	if err := checkShape(data); err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fail(ErrInvalid, describe(err))
	}
	if dec.More() {
		return fail(ErrInvalid, "trailing data after JSON body")
	}
	if _, err := dec.Token(); err != io.EOF {
		return fail(ErrInvalid, "trailing data after JSON body")
	}
	return nil
}

// checkShape enforces a top-level object and the maximum nesting depth.
func checkShape(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	depth := 0
	first := true
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fail(ErrInvalid, ErrInvalid.Error())
		}
		if first {
			if d, ok := tok.(json.Delim); !ok || d != '{' {
				return fail(ErrInvalid, "request body must be a JSON object")
			}
			first = false
		}
		if d, ok := tok.(json.Delim); ok {
			switch d {
			case '{', '[':
				depth++
				if depth > MaxDepth {
					return fail(ErrInvalid, "maximum JSON nesting depth exceeded")
				}
			case '}', ']':
				depth--
			}
		}
	}
}

// describe turns a decoder error into a safe message that names the field
// but never echoes submitted values.
func describe(err error) string {
	var typeErr *json.UnmarshalTypeError
	switch {
	case errors.As(err, &typeErr) && typeErr.Field != "":
		return "field " + typeErr.Field + " has the wrong type"
	case strings.HasPrefix(err.Error(), "json: unknown field "):
		return strings.TrimPrefix(err.Error(), "json: ")
	default:
		return ErrInvalid.Error()
	}
}
