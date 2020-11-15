package exampletasks

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/Joker666/cogman/util"
)

// SubTask holds necessary fields for subtract task
type SubTask struct {
	Name string
}

// NewSubTask returns instance of SubTask
func NewSubTask() util.Handler {
	return SubTask{
		TaskSubtraction,
	}
}

// Do handles task
func (t SubTask) Do(ctx context.Context, payload []byte) error {
	var body TaskBody
	if err := json.Unmarshal(payload, &body); err != nil {
		log.Print("Subtraction task process error", err)
		return err
	}

	time.Sleep(time.Millisecond * 300)

	log.Printf("num1: %d num2: %d sub: %d", body.Num1, body.Num2, body.Num1-body.Num2)
	return nil
}
