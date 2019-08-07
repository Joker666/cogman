package exampletasks

import (
	"context"
	"encoding/json"
	"log"
)

type SubTask struct {
	Name string
}

func NewSubTask() SubTask {
	return SubTask{
		TaskSubtraction,
	}
}

func (t SubTask) Do(ctx context.Context, payload []byte) error {
	var body TaskBody
	if err := json.Unmarshal(payload, &body); err != nil {
		log.Fatal("Subtraction task process error", err)
	}

	log.Printf("num1: %d num2: %d sub: %d", body.Num1, body.Num2, body.Num1-body.Num2)
	return nil
}
