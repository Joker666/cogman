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
	ID        string
	Name      string
	Payload   []byte
	Priority  PriorityType
	Status    Status
	FailError string
	CreatedAt time.Time
	UpdatedAt time.Time
}
