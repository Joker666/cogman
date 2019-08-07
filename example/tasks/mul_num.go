package exampletasks

import (
	"context"
	"encoding/json"
	"log"

	"github.com/Tapfury/cogman/util"
)

type MulTask struct {
	Name string
}

func NewMulTask() util.Handler {
	return MulTask{
		Name: TaskMultiplication,
	}
}

func (t MulTask) Do(ctx context.Context, payload []byte) error {
	var body TaskBody
	if err := json.Unmarshal(payload, &body); err != nil {
		log.Fatal("Multiplication task process error", err)
		return nil
	}

	log.Printf("num1: %d num2: %d mul: %d", body.Num1, body.Num2, body.Num1*body.Num2)
	return nil
}
