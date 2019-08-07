package main

import (
	"log"
	"time"

	exampletasks "github.com/Tapfury/cogman/example/tasks"
)

func main() {
	client, err := SetupClient()
	if err != nil {
		log.Fatal(err)
	}

	server, err := SetupServer()
	if err != nil {
		log.Fatal(err)
	}

	// Task handler register

	server.Register(exampletasks.TaskAddition, exampletasks.NewSumTask())
	server.Register(exampletasks.TaskSubtraction, exampletasks.NewSubTask())
	server.Register(exampletasks.TaskMultiplication, exampletasks.NewSubTask())

	task, err := exampletasks.GetAdditionTask(234, 435)
	if err != nil {
		log.Fatal(err)
	}
	client.SendTask(*task)

	task, err = exampletasks.GetSubtractionTask(43, 23)
	if err != nil {
		log.Fatal(err)
	}
	client.SendTask(*task)

	task, err = exampletasks.GetMultiplicationTask(2, 24)
	if err != nil {
		log.Fatal(err)
	}
	client.SendTask(*task)

	close()
}

func close() {
	close := time.After(time.Second * 3)
	<-close

	log.Print("[x] press ctrl + c to terminate the program")

	<-close
}
