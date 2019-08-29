package main

import (
	"log"
	"time"

	cogman "github.com/Tapfury/cogman/client"
	"github.com/Tapfury/cogman/config"
	exampletasks "github.com/Tapfury/cogman/example/tasks"
	"github.com/Tapfury/cogman/util"
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
			LowPriorityQueueCount:  3,
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

	if err := SendExampleTask(clnt); err != nil {
		log.Fatal(err)
	}

	close := time.After(time.Second * 3)
	<-close

	log.Print("[x] press ctrl + c to terminate the program")

	<-close
}

func SendExampleTask(clnt *cogman.Session) error {
	log.Printf("========================================>")
	time.Sleep(time.Second * 3)

	for i := 0; i < 3; i++ {
		task, err := exampletasks.GetAdditionTask(234, 435, util.TaskPriorityHigh, 3)
		if err != nil {
			return err
		}
		if err := clnt.SendTask(*task); err != nil {
			return err
		}

	}

	time.Sleep(time.Second * 3)
	log.Print("========================================>")

	for i:=0;i<5;i++{
		task, err := exampletasks.GetSubtractionTask(43, 23, util.TaskPriorityLow, 3)
		if err != nil {
			return err
		}
		if err := clnt.SendTask(*task); err != nil {
			return err
		}
	}

	//time.Sleep(time.Second * 3)
	log.Print("========================================>")

	task, err := exampletasks.GetMultiplicationTask(2, 24, util.TaskPriorityHigh, 3)
	if err != nil {
		return err
	}
	if err := clnt.SendTask(*task); err != nil {
		return err
	}

	return nil
}
