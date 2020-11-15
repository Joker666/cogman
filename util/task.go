package util

import "time"

// Status is the status of the task
type Status string

// list of task status
const (
	StatusRetry      Status = "retry"
	StatusInitiated  Status = "initiated"
	StatusQueued     Status = "queued"
	StatusInProgress Status = "in_progress"
	StatusFailed     Status = "failed"
	StatusSuccess    Status = "success"
)

// TaskPriority is the priority of the task
type TaskPriority string

// list of TaskPriority
var (
	TaskPriorityHigh TaskPriority = "High"
	TaskPriorityLow  TaskPriority = "Low"
)

// Valid check the task priority
func (p TaskPriority) Valid() bool {
	return p == TaskPriorityHigh || p == TaskPriorityLow
}

// Task field hold the necessary field
type Task struct {
	TaskID         string
	PrimaryKey     string
	Name           string
	OriginalTaskID string
	Retry          int
	Payload        []byte
	Priority       TaskPriority
	Status         Status
	FailError      string
	Duration       *float64
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// CheckStatusOrder check if status st can be updated by status p
func (p Status) CheckStatusOrder(st Status) bool {
	val := map[Status]int{
		StatusInitiated:  1,
		StatusQueued:     2,
		StatusInProgress: 3,
		StatusRetry:      4,
		StatusFailed:     5,
		StatusSuccess:    5,
	}

	return val[p] > val[st]
}

// TaskDateRangeCount holds necessary values for task and it's statuses in a date range
type TaskDateRangeCount struct {
	ID              time.Time
	Total           int
	CountRetry      int
	CountInitiated  int
	CountQueued     int
	CountInProgress int
	CountSuccess    int
	CountFailed     int
}
