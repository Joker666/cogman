package main

import (
	"log"
	"time"

	"github.com/Tapfury/cogman"
	"github.com/Tapfury/cogman/config"
	exampletasks "github.com/Tapfury/cogman/example/tasks"
	"github.com/Tapfury/cogman/util"
)

func main() {
	cfg := &config.Config{
		AmqpURI:  "amqp://localhost:5672",    // required
		RedisURI: "redis://localhost:6379/0", // required
	}

	// StartBackgroud will initiate a client & a server together.
	// Both client & server will retry if a task fails.
	// Task will be re-enqueued (ReEnqueue: true) from client
	// if client can not deliver it to amqp for any issues.

	log.Print("initiate client & server together")
	if err := cogman.StartBackground(cfg); err != nil {
		log.Fatal(err)
	}

	time.Sleep(time.Second * 3)
	log.Print("========================================>")

	// Send task required a task signature and a handler.
	// If a task register by a handler, task with same name can
	// use that handler without sending it again.

	task, err := exampletasks.GetMultiplicationTask(21, 7, util.TaskPriorityLow, 1)
	if err != nil {
		log.Fatal(err)
	}
	handler := exampletasks.NewMulTask()
	if err := cogman.SendTask(*task, handler); err != nil {
		log.Fatal(err)
	}

	time.Sleep(time.Second * 3)
	log.Print("========================================>")

	// task can be registered before hand using register api.
	handler = exampletasks.NewSumTask()
	cogman.Register(exampletasks.TaskAddition, handler)
	if err != nil {
		log.Fatal(err)
	}

	task, err = exampletasks.GetAdditionTask(12, 31, util.TaskPriorityHigh, 2)
	if err != nil {
		log.Fatal(err)
	}
	if err := cogman.SendTask(*task, nil); err != nil {
		log.Fatal(err)
	}

	close()
}

func close() {
	close := time.After(time.Second * 3)
	<-close

	log.Print("[x] press ctrl + c to terminate the program")

	<-close
}
