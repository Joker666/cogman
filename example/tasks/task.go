package exampletasks

import (
	"github.com/Tapfury/cogman/util"
	"log"
	"encoding/json"
)

// Task list
var (
	TaskAddition       = "add_num"
	TaskSubtraction    = "sub_num"
	TaskMultiplication = "mul_num"
	TaskErrorGenerator = "error_generator"
)

func TaskList() []string {
	return []string{
		TaskAddition,
		TaskSubtraction,
		TaskMultiplication,
		TaskErrorGenerator,
	}
}

type TaskBody struct {
	Num1 int
	Num2 int
}


func GetAdditionTask(numA, numB int) (*util.Task, error) {
	body :=  TaskBody{
		Num1: numA,
		Num2: numB,
	}

	pld, err := parseBody(body)
	if err != nil {
		log.Print("Parse: ", err)
		return nil, err
	}

	task := &util.Task{
		Name:      TaskAddition,
		Payload:  pld,
		Priority: util.TaskPriorityHigh,
		Retry:    0,
	}

	return task, nil
}

func GetMultiplicationTask(numA, numB int) (*util.Task, error) {
	body :=  TaskBody{
		Num1: numA,
		Num2: numB,
	}

	pld, err := parseBody(body)
	if err != nil {
		log.Print("Parse: ", err)
		return nil, err
	}

	task := &util.Task{
		Name:      TaskMultiplication,
		Payload:  pld,
		Priority: util.TaskPriorityHigh,
		Retry:    0,
	}

	return task, nil
}

func GetSubtractionTask(numA, numB int) (*util.Task, error) {
	body :=  TaskBody{
		Num1: numA,
		Num2: numB,
	}

	pld, err := parseBody(body)
	if err != nil {
		log.Print("Parse: ", err)
		return nil, err
	}

	task := &util.Task{
		Name:      TaskSubtraction,
		Payload:  pld,
		Priority: util.TaskPriorityLow,
		Retry:    1,
	}

	return task, nil
}

func parseBody(body interface{}) ([]byte, error) {
	pld, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	return pld, nil
}
