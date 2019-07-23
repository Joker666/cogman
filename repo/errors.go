package repo

import "errors"

var (
	ErrTaskNotFound = errors.New("mongo: task not found")
)
