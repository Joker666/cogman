package repo

import "errors"

var (
	ErrTaskNotFound      = errors.New("mongo: task not found")
	ErrMongoNoConnection = errors.New("mongo: no connection")
	ErrRedisNoConnection = errors.New("redis: no connection")
	ErrErrorRequired     = errors.New("error message required")
	ErrDurationRequired  = errors.New("task duration required")
)
