package rest

import (
	"errors"

	"github.com/Tapfury/cogman/rest/resp"
)

var (
	ErrInvalidData              resp.CogmanError = errors.New("cogman rest: Invalid data")
	ErrTaskIDRequired           resp.CogmanError = errors.New("cogman rest: task_id required")
	ErrTaskNotFound             resp.CogmanError = errors.New("cogman rest: task not found")
	ErrBothStartEndTimeRequired resp.CogmanError = errors.New("cogman rest: both start & end time required")
	ErrInvalidTimeRange         resp.CogmanError = errors.New("cogman rest: invalid time ranges")
)
