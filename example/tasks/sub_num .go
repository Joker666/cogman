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
		log.Print("Task process error", err)
		return nil
	}

	log.Print("num1: ", body.Num1, "num2: ", body.Num2, "Sub: ", body.Num1-body.Num2)
	return nil
}
