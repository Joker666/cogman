package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/Tapfury/cogman/client"
	"github.com/Tapfury/cogman/config"
	exampletasks "github.com/Tapfury/cogman/example/tasks"
	"github.com/Tapfury/cogman/util"
)

var (
	clnt *client.Session
)

func main() {
	cfg := config.Client{
		ConnectionTimeout: time.Minute * 10,
		RequestTimeout:    time.Second * 5,
		AMQP: config.AMQP{
			URI:                    "amqp://localhost:5672",
			HighPriorityQueueCount: 5,
			LowPriorityQueueCount:  5,
			Exchange:               "",
		},
		Mongo: config.Mongo{URI: "mongodb://root:secret@localhost:27017"},
		Redis: config.Redis{URI: "redis://localhost:6379/0"},
	}

	var err error

	log.Print("creating new client session")
	clnt, err = client.NewSession(cfg)
	if err != nil {
		log.Print("NewSession: ", err)
		return
	}

	log.Print("connecting client")
	if err := clnt.Connect(); err != nil {
		log.Print("connect: ", err)
		return
	}

	task, err := getMultiplicationTask(5, 10)
	if err == nil {
		sendTask(task)
	}

	task, err = getSubtractionTask(123, 456)
	if err == nil {
		sendTask(task)
	}

	task, err = getAdditionTask(7, 21)
	if err == nil {
		sendTask(task)
	}

	log.Print("closing channel...")
	close := time.After(time.Second * 5)
	<-close

	clnt.Close()

	log.Print("channel closed...")
}

func sendTask(t *util.Task) {
	log.Print("sending task: ", t.Name)

	err := clnt.SendTask(*t)
	if err != nil {
		log.Print("task: ", err)
		return
	}

	log.Print(t.Name, " send successfully")
}

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

func getSubtractionTask(numA, numB int) (*util.Task, error) {
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
		Name:     exampletasks.TaskSubtraction,
		Payload:  pld,
		Priority: util.TaskPriorityLow,
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
		Priority: util.TaskPriorityLow,
		Retry:    2,
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
