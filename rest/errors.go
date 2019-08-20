package rest

import "errors"

var (
	ErrInvalidData              = errors.New("cogman rest: Invalid data")
	ErrBothStartEndTimeRequired = errors.New("cogman rest: both start & end time required")
	ErrInvalidTimeRange         = errors.New("cogman rest: invalid time ranges")
)
