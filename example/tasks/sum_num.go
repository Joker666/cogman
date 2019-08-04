package exampletasks

import (
	"context"
	"encoding/json"
	"log"
)

type SumTask struct {
	Name string
}

func NewSumTask() SumTask {
	return SumTask{
		Name: TaskAddition,
	}
}

func (t SumTask) Do(ctx context.Context, payload []byte) error {
	var body TaskBody
	if err := json.Unmarshal(payload, &body); err != nil {
		log.Print("Task process error", err)
		return nil
	}

	log.Print("num1: ", body.Num1, "num2: ", body.Num2, "Sum: ", body.Num1+body.Num2)
	return nil
}
