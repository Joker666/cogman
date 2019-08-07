package main

import (
	"log"
	"time"

	"github.com/Tapfury/cogman"
	exampletasks "github.com/Tapfury/cogman/example/tasks"
)

func main() {

	cfg := getConfig()

	// StartBackgroud will initiate a client & a server together.
	// Both end will retry if a task fails.
	// Task will be re-enqueued (ReEnqueue: true) from client end
	// if for any reason client can not deliver it to amqp.

	log.Print("initiate client & server together")
	if err := cogman.StartBackground(cfg); err != nil {
		log.Fatal(err)
	}

	// Send task required a task signature and a handler.
	// If a task register by a handler, task with same name can
	// use that handler without sending it again.
	// task can be register before hand using register api.

	task, err := getMultiplicationTask(21, 7)
	if err != nil {
		log.Fatal(err)
	}
	handler := exampletasks.NewMulTask()
	cogman.SendTask(*task, handler)

	task, err = getAdditionTask(12, 31)
	if err != nil {
		log.Fatal(err)
	}
	handler = exampletasks.NewSumTask()
	cogman.Register(task.Name, handler)
	if err != nil {
		log.Fatal(err)
	}
	cogman.SendTask(*task, nil)

	close()
}

func close() {
	close := time.After(time.Second * 3)
	<-close

	log.Print("[x] press ctrl + c to terminate the program")

	<-close
}
