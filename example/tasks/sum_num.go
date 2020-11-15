package exampletasks

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/Joker666/cogman/util"
)

// SumTask holds necessary fields for multiply task
type SumTask struct {
	Name string
}

// NewSumTask returns instance of SumTask
func NewSumTask() util.Handler {
	return SumTask{
		Name: TaskAddition,
	}
}

// Do handles task
func (t SumTask) Do(ctx context.Context, payload []byte) error {
	var body TaskBody
	if err := json.Unmarshal(payload, &body); err != nil {
		log.Print("Sum task process error", err)
		return err
	}

	time.Sleep(time.Millisecond * 300)

	log.Printf("num1: %d num2: %d sum: %d", body.Num1, body.Num2, body.Num1+body.Num2)
	return nil
}
