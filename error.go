package cogman

import "errors"

var (
	ErrDuplicateTaskName = errors.New("cogman: duplicate task name")
	ErrRunningServer     = errors.New("cogman: server is already running")
	ErrStoppedServer     = errors.New("cogman: server is already stopped")
	ErrNoTask            = errors.New("cogman: server has no task")
)

type TaskHandlerMissingError string

func (t TaskHandlerMissingError) Error() string {
	return "cogman: task handler missing: " + string(t)
}
