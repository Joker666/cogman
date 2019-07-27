package util

import "time"

type Status string

const (
	StatusRetry      Status = "retry"
	StatusInitiated  Status = "initiated"
	StatusQueued     Status = "queued"
	StatusInProgress Status = "in_progress"
	StatusFailed     Status = "failed"
	StatusSuccess    Status = "success"
)

type PriorityType string

var (
	PriorityTypeHigh PriorityType = "High"
	PriorityTypeLow  PriorityType = "Low"
)

func (p PriorityType) Valid() bool {
	return (p == PriorityTypeHigh || p == PriorityTypeLow)
}

type Task struct {
	TaskID         string
	Name           string
	OriginalTaskID string
	Retry          int
	Payload        []byte
	Priority       PriorityType
	Status         Status
	FailError      string
	Duration       *float64
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// CheckStatusOrder check if status st can be updated by status p
func (p Status) CheckStatusOrder(st Status) bool {
	val := map[Status]int{
		StatusRetry:      0,
		StatusInitiated:  1,
		StatusQueued:     2,
		StatusInProgress: 3,
		StatusFailed:     4,
		StatusSuccess:    4,
	}

	return val[p] > val[st]
}
