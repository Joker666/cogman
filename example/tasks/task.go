package exampletasks

import (
	"encoding/json"
	"log"

	"github.com/Joker666/cogman/util"
)

// Task list
var (
	TaskAddition       = "add_num"
	TaskSubtraction    = "sub_num"
	TaskMultiplication = "mul_num"
	TaskErrorGenerator = "error_generator"
)

// TaskList returns list of task names
func TaskList() []string {
	return []string{
		TaskAddition,
		TaskSubtraction,
		TaskMultiplication,
		TaskErrorGenerator,
	}
}

// TaskBody holds fields for task body
type TaskBody struct {
	Num1 int
	Num2 int
}

// GetAdditionTask returns addition task
func GetAdditionTask(numA, numB int, p util.TaskPriority, retryCount int) (*util.Task, error) {
	body := TaskBody{
		Num1: numA,
		Num2: numB,
	}

	pld, err := parseBody(body)
	if err != nil {
		log.Print("Parse: ", err)
		return nil, err
	}

	task := &util.Task{
		Name:     TaskAddition,
		Payload:  pld,
		Priority: p,
		Retry:    retryCount,
	}

	return task, nil
}

// GetMultiplicationTask returns multiplication task
func GetMultiplicationTask(numA, numB int, p util.TaskPriority, retryCount int) (*util.Task, error) {
	body := TaskBody{
		Num1: numA,
		Num2: numB,
	}

	pld, err := parseBody(body)
	if err != nil {
		log.Print("Parse: ", err)
		return nil, err
	}

	task := &util.Task{
		Name:     TaskMultiplication,
		Payload:  pld,
		Priority: p,
		Retry:    retryCount,
	}

	return task, nil
}

// GetSubtractionTask returns subtraction task
func GetSubtractionTask(numA, numB int, p util.TaskPriority, retryCount int) (*util.Task, error) {
	body := TaskBody{
		Num1: numA,
		Num2: numB,
	}

	pld, err := parseBody(body)
	if err != nil {
		log.Print("Parse: ", err)
		return nil, err
	}

	task := &util.Task{
		Name:     TaskSubtraction,
		Payload:  pld,
		Priority: p,
		Retry:    retryCount,
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
