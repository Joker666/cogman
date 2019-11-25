package exampletasks

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/Tapfury/cogman/util"
)

type SubTask struct {
	Name string
}

func NewSubTask() util.Handler {
	return SubTask{
		TaskSubtraction,
	}
}

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
