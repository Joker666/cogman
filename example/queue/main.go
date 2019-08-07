package main

import (
	"log"
	"time"

	"github.com/Tapfury/cogman"
	"github.com/Tapfury/cogman/config"
	exampletasks "github.com/Tapfury/cogman/example/tasks"
)

// Low priority task will be pushed to low priority queue
// High priority task will be pushed to high priority queue
// Number of each type of queue can be configured from config file

// format of queue naming: lazy_priority_queue_*   & high_priority_queue_*
// specific types of task push to specific type of queue using round robin manner
// also it check the availability of the queue before pushing a task

func main() {
	cfg := &config.Config{
		ConnectionTimeout: time.Minute * 10, // optional
		RequestTimeout:    time.Second * 5,  // optional

		AmqpURI:  "amqp://localhost:5672",                  // required
		RedisURI: "redis://localhost:6379/0",               // required
		MongoURI: "mongodb://root:secret@localhost:27017/", //optional

		HighPriorityQueueCount: 2, // Optional. Default value 1
		LowPriorityQueueCount:  1, // Optional. Default value 1

		ReEnqueue: true, // optional. default false. Mongo connection also needed
	}

	// StartBackgroud will initiate a client & a server together.
	// Both client & server will retry if a task fails.
	// Task will be re-enqueued (ReEnqueue: true) from client
	// if client can not deliver it to amqp for any issues.

	log.Print("initiate client & server together")
	if err := cogman.StartBackground(cfg); err != nil {
		log.Fatal(err)
	}

	// Any number of task handler can be register
	// Task name must be unique

	cogman.Register(exampletasks.TaskAddition, exampletasks.NewSumTask())
	cogman.Register(exampletasks.TaskSubtraction, exampletasks.NewSubTask())

	task, err := exampletasks.getAdditionTask(9, 9)
	if err != nil {
		log.Fatal(err)
	}
	cogman.SendTask(*task, nil)

	task, err = exampletasks.getSubtractionTask(10, 100)
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
