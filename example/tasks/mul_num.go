package exampletasks

import (
	"context"
	"encoding/json"
	"log"
	"time"

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
		log.Print("Multiplication task process error", err)
		return err
	}

	time.Sleep(time.Millisecond * 300)

	log.Printf("num1: %d num2: %d mul: %d", body.Num1, body.Num2, body.Num1*body.Num2)
	return nil
}
