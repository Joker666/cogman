package cogman

import "errors"

var (
	ErrRequestTimeout    = errors.New("cogman: request timeout")
	ErrConnectionTimeout = errors.New("cogman: connection timeout")
	ErrDuplicateTaskName = errors.New("cogman: duplicate task name")
	ErrRunningServer     = errors.New("cogman: server is already running")
	ErrStoppedServer     = errors.New("cogman: server is already stopped")
	ErrNoTask            = errors.New("cogman: server has no task")
	ErrTaskHeadless      = errors.New("cogman: headless task")
	ErrTaskUnidentified  = errors.New("cogman: unidentified task")
	ErrTaskUnhandled     = errors.New("cogman: unhandled task")
	ErrNoTaskID          = errors.New("cogman: no task id")
)

type TaskHandlerMissingError string

func (t TaskHandlerMissingError) Error() string {
	return "cogman: task handler missing: " + string(t)
}
