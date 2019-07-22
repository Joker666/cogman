package infra

import "errors"

var (
	ErrNotConnected = errors.New("redis: client not connected")
	ErrTaskNotFound = errors.New("mongo: task not found")
)
