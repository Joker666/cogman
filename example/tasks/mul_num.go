package exampletasks

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/Joker666/cogman/util"
)

// MulTask holds necessary fields for multiply task
type MulTask struct {
	Name string
}

// NewMulTask returns instance of MulTask
func NewMulTask() util.Handler {
	return MulTask{
		Name: TaskMultiplication,
	}
}

// Do handles task
func (t MulTask) Do(ctx context.Context, payload []byte) error {
	var body TaskBody
	if err := json.Unmarshal(payload, &body); err != nil {
		log.Print("Multiplication task process error", err)
		return err
	}

	time.Sleep(time.Millisecond * 300)

	log.Printf("num1: %d num2: %d mul: %d", body.Num1, body.Num2, body.Num1*body.Num2)
	return nil
}
