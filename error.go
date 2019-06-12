package cogman

import "errors"

var (
	ErrDuplicateTaskName = errors.New("cogman: duplicate task name")
	ErrRunningServer     = errors.New("cogman: server is already running")
	ErrStoppedServer     = errors.New("cogman: server is already stopped")
)
