package util

const (
	LowPriorityQueue  = "low_priority_queue"
	HighPriorityQueue = "high_priority_queue"
)

type PriorityType string

var (
	PriorityTypeHigh PriorityType = "High"
	PriorityTypeLow  PriorityType = "Low"
)

func (p PriorityType) Valid() bool {
	return (p == PriorityTypeHigh || p == PriorityTypeLow)
}
