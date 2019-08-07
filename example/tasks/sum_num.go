package exampletasks

import (
	"context"
	"encoding/json"
	"log"

	"github.com/Tapfury/cogman/util"
)

type SumTask struct {
	Name string 
}

func NewSumTask() util.Handler {
	return SumTask{
		Name: TaskAddition,
	}
}

func (t SumTask) Do(ctx context.Context, payload []byte) error {
	var body TaskBody
	if err := json.Unmarshal(payload, &body); err != nil {
		log.Print("Sum task process error", err)
		return err
	}

	log.Printf("num1: %d num2: %d sum: %d", body.Num1, body.Num2, body.Num1+body.Num2)
	return nil
}
