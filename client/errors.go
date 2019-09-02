package client

import "errors"

// list of client side errors
var (
	ErrInvalidPriority   = errors.New("cogman: task priority invalid")
	ErrNotConnected      = errors.New("cogman: client not connected")
	ErrNotPublished      = errors.New("cogman: task not published")
	ErrInvalidConfig     = errors.New("cogman: invalid client config")
	ErrRequestTimeout    = errors.New("cogman: request timeout")
	ErrConnectionTimeout = errors.New("cogman: connection timeout")
	ErrQueueNotAvailable = errors.New("cogman: queue not available")
)
