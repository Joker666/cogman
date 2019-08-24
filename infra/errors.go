package infra

import "errors"

// list of infra errors.
var (
	ErrNotConnected = errors.New("redis: client not connected")
	ErrNotFound     = errors.New("mongo: not found")
)
