package gorpc

import (
	"context"
	"errors"
	"fmt"
)

// Remote error codes used by the built-in server and helpers.
const (
	ErrorCodeCanceled         = "canceled"
	ErrorCodeDeadlineExceeded = "deadline_exceeded"
	ErrorCodeInternal         = "internal"
	ErrorCodeInvalidRequest   = "invalid_request"
	ErrorCodeNotFound         = "not_found"
	ErrorCodeUnauthorized     = "unauthorized"
	ErrorCodeUnavailable      = "unavailable"
)

// Common GoRPC errors.
var (
	ErrClosed            = errors.New("gorpc: closed")
	ErrAuthentication    = errors.New("gorpc: authentication failed")
	ErrDuplicateFunction = errors.New("gorpc: duplicate function")
	ErrInvalidFunction   = errors.New("gorpc: invalid function")
	ErrInvalidHandler    = errors.New("gorpc: invalid handler")
	ErrInvalidResponse   = errors.New("gorpc: invalid response")
	ErrUnavailable       = errors.New("gorpc: unavailable")
)

// RemoteError is sent in FrameError payloads and returned by callers when the
// server handled the request but rejected or failed it.
type RemoteError struct {
	Code    string         `msgpack:"code" json:"code"`
	Message string         `msgpack:"message" json:"message"`
	Details map[string]any `msgpack:"details,omitempty" json:"details,omitempty"`
}

func (e *RemoteError) Error() string {
	if e == nil {
		return "remote error"
	}
	if e.Code == "" {
		return e.Message
	}
	if e.Message == "" {
		return fmt.Sprintf("remote error: %s", e.Code)
	}

	return fmt.Sprintf("remote error %s: %s", e.Code, e.Message)
}

// NewRemoteError creates a structured error suitable for returning from a handler.
func NewRemoteError(code, message string, details map[string]any) *RemoteError {
	return &RemoteError{
		Code:    code,
		Message: message,
		Details: details,
	}
}

func remoteErrorFromError(err error) RemoteError {
	if err == nil {
		return RemoteError{}
	}

	var remoteErr *RemoteError
	if errors.As(err, &remoteErr) && remoteErr != nil {
		return *remoteErr
	}

	switch {
	case errors.Is(err, context.Canceled):
		return RemoteError{Code: ErrorCodeCanceled, Message: err.Error()}
	case errors.Is(err, context.DeadlineExceeded):
		return RemoteError{Code: ErrorCodeDeadlineExceeded, Message: err.Error()}
	default:
		return RemoteError{Code: ErrorCodeInternal, Message: err.Error()}
	}
}
