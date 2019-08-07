package main

import (
	"encoding/json"
	"log"

	exampletasks "github.com/Tapfury/cogman/example/tasks"
	"github.com/Tapfury/cogman/util"
)

func getAdditionTask(numA, numB int) (*util.Task, error) {
	body := exampletasks.TaskBody{
		Num1: numA,
		Num2: numB,
	}

	pld, err := parseBody(body)
	if err != nil {
		log.Print("Parse: ", err)
		return nil, err
	}

	task := &util.Task{
		Name:     exampletasks.TaskAddition,
		Payload:  pld,
		Priority: util.TaskPriorityHigh,
		Retry:    0,
	}

	return task, nil
}

func getMultiplicationTask(numA, numB int) (*util.Task, error) {
	body := exampletasks.TaskBody{
		Num1: numA,
		Num2: numB,
	}

	pld, err := parseBody(body)
	if err != nil {
		log.Print("Parse: ", err)
		return nil, err
	}

	task := &util.Task{
		Name:     exampletasks.TaskMultiplication,
		Payload:  pld,
		Priority: util.TaskPriorityHigh,
		Retry:    0,
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
