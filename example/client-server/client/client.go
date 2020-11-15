package main

import (
	"log"
	"time"

	cogman "github.com/Joker666/cogman/client"
	"github.com/Joker666/cogman/config"
	exampleTasks "github.com/Joker666/cogman/example/tasks"
	"github.com/Joker666/cogman/util"
)

func main() {
	cfg := config.Client{
		ConnectionTimeout: time.Minute * 10,
		RequestTimeout:    time.Second * 10,

		Mongo: config.Mongo{URI: "mongodb://root:secret@localhost:27017/", TTL: time.Hour},
		Redis: config.Redis{URI: "redis://localhost:6379/0", TTL: time.Hour},
		AMQP: config.AMQP{
			URI:                    "amqp://localhost:5672",
			HighPriorityQueueCount: 5,
			LowPriorityQueueCount:  5,
			Exchange:               "",
		},

		ReEnqueue: true,
	}

	clnt, err := cogman.NewSession(cfg)
	if err != nil {
		log.Fatal(err)
	}

	if err := clnt.Connect(); err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 1000; i++ {
		if err := SendExampleTask(clnt); err != nil {
			log.Fatal(err)
		}
	}

	end := time.After(time.Second * 3)
	<-end

	log.Print("[x] press ctrl + c to terminate the program")

	<-end
}

// SendExampleTask sends example task
func SendExampleTask(client *cogman.Session) error {
	log.Printf("========================================>")

	task, err := exampleTasks.GetAdditionTask(234, 435, util.TaskPriorityHigh, 3)
	if err != nil {
		return err
	}
	if err := client.SendTask(*task); err != nil {
		return err
	}

	log.Print("========================================>")

	task, err = exampleTasks.GetSubtractionTask(43, 23, util.TaskPriorityLow, 3)
	if err != nil {
		return err
	}
	if err := client.SendTask(*task); err != nil {
		return err
	}

	log.Print("========================================>")

	task, err = exampleTasks.GetMultiplicationTask(2, 24, util.TaskPriorityHigh, 3)
	if err != nil {
		return err
	}
	if err := client.SendTask(*task); err != nil {
		return err
	}

	return nil
}
