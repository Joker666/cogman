package repo

import "errors"

var (
	ErrTaskNotFound       = errors.New("mongo: task not found")
	ErrErrorRequired      = errors.New("error message required")
	ErrDurationRequired   = errors.New("task duration required")
	ErrRetryLimitExceeded = errors.New("retry limit exceeded")
)
