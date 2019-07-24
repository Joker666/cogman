package util

import "time"

type Status string

const (
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
	ID             string
	Name           string
	PreviousTaskID string
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
		StatusFailed:     0,
		StatusInitiated:  0,
		StatusQueued:     0,
		StatusInProgress: 0,
		StatusSuccess:    4,
	}

	return val[p] >= val[st]
}
