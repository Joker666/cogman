package rest

import (
	"errors"

	"github.com/Joker666/cogman/rest/resp"
)

// list cogman rest api  related errors
var (
	ErrInvalidData              resp.CogmanError = errors.New("cogman rest: Invalid data")
	ErrInvalidMethod            resp.CogmanError = errors.New("cogman rest: Invalid method")
	ErrTaskIDRequired           resp.CogmanError = errors.New("cogman rest: task_id required")
	ErrTaskNotFound             resp.CogmanError = errors.New("cogman rest: task not found")
	ErrBothStartEndTimeRequired resp.CogmanError = errors.New("cogman rest: both start & end time required")
	ErrInvalidTimeRange         resp.CogmanError = errors.New("cogman rest: invalid time ranges")
)
